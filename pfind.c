#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <errno.h>
#include <sys/stat.h>
#include <pthread.h>
#include <dirent.h>
#include <stdatomic.h>

#define T_UNKNOWN 0
#define T_FILE 1
#define T_DIR 2
#define T_LINK 3


typedef struct dirent dirent;

typedef struct queueItem {
    char *value;
    struct queueItem *next;
} QueueItem;

typedef struct queue {
    int size;
    QueueItem *first;
    QueueItem *last;
} Queue;

Queue *queue;
pthread_mutex_t startLock;
pthread_mutex_t queueLock;
pthread_rwlock_t runningThreadsLock;
pthread_rwlock_t queueRWLock;

int parallelism;
pthread_cond_t queueConsumableCond;
pthread_cond_t doneInitCond;
atomic_int threadsSignaled;
atomic_int createdProcesses;
atomic_int foundFiles;
atomic_int runningThreads;
atomic_int failedThreads;

/**
 * Allocate memory for queue and initiate value
 */
void initQueue() {
    queue = malloc(sizeof(Queue));
    queue->size = 0;
    queue->first = NULL;
    queue->last = NULL;
}

/**
 * return amount of items in queue
 */
int unsafeGetQueueSize() {
    int size = queue->size;
    return size;
}

/**
 * Safely return the amount of items in queue (lock only queue's size)
 */
int getQueueSize() {
    pthread_rwlock_rdlock(&queueRWLock);
    int size = unsafeGetQueueSize();
    pthread_rwlock_unlock(&queueRWLock);
    return size;
}

/**
 * return first item in queue, NULL if empty
 */
char *unsafeDeQueue() {
    char *value;
    QueueItem *qItem;
    if (queue->size == 0) {
        return NULL;
    }
    qItem = queue->first;
    value = qItem->value;
    queue->first = qItem->next;
    if (queue->size == 1) {
        queue->last = NULL;
    }
    queue->size -= 1;
    free(qItem);
    return value;
}


void incRunningThreads() {
    pthread_rwlock_wrlock(&runningThreadsLock);
    runningThreads++;
    pthread_rwlock_unlock(&runningThreadsLock);
}

void decRunningThreads() {
    pthread_rwlock_wrlock(&runningThreadsLock);
    runningThreads--;
    pthread_rwlock_unlock(&runningThreadsLock);
}

int unsafeGetRunningThreads() {
    return runningThreads;
}

int getRunningThreads() {
    pthread_rwlock_rdlock(&runningThreadsLock);
    int res = unsafeGetRunningThreads();
    pthread_rwlock_unlock(&runningThreadsLock);
    return res;
}


/**
 * Returns if the thread is allowed to handle this item.
 * Returns true if:
 *  1. parallelism is 1, so he MUST handle the item
 *  2. queue is not empty, so other waiting thread should not be waiting
 *  3. There is nobody else waiting to consume (everybody is busy)
 *
 *  Otherwise returns false
 */
int unsafeIsAllowedToHandle() {
    return parallelism == 1 || unsafeGetQueueSize() > 0 || (parallelism - getRunningThreads() - failedThreads - 1) == 0;
}


/**
 * Add string item to queue
 */
void unsafeEnQueue(char *str) {
    QueueItem *qItem = malloc(sizeof(QueueItem));
    qItem->next = NULL;
    qItem->value = str;
    if (queue->size == 0) {
        queue->first = qItem;
    } else {
        queue->last->next = qItem;
    }
    queue->last = qItem;
    queue->size++;
}

/**
 *  Add item to queue
 */
void enQueue(char *str) {
    pthread_rwlock_wrlock(&queueRWLock);
    int isAllowedToHandle = unsafeIsAllowedToHandle();
    unsafeEnQueue(str);
    pthread_cond_signal(&queueConsumableCond);
    pthread_rwlock_unlock(&queueRWLock);
    if (!isAllowedToHandle) {
        sched_yield();
    }
}


/**
 * Safely pop and return first item in queue, NULL if empty
 */
char *deQueue() {
    pthread_mutex_lock(&queueLock);
    while (unsafeGetQueueSize() == 0 && getRunningThreads() > 0) {
        pthread_cond_wait(&queueConsumableCond, &queueLock);
    }
    pthread_mutex_unlock(&queueLock);
    pthread_rwlock_wrlock(&queueRWLock);
    incRunningThreads();
    char *path = unsafeDeQueue();
    pthread_rwlock_unlock(&queueRWLock);
    return path;
}

/**
 * @param strParallelism parallelism from input
 * @param intParallelism where to store the int result
 * @return -1 if conversion failed, 0 if successfull
 */
int parseParallelism(char *strParallelism, int *intParallelism) {
    int prlsm = (int) strtol(strParallelism, NULL, 10);
    if (prlsm == UINT_MAX && errno == ERANGE) {
        return -1;
    }
    *intParallelism = prlsm;
    return 0;
}

int getTypeFromPath(char *path) {
    struct stat buff;
    lstat(path, &buff);
    if (S_ISREG(buff.st_mode)) return T_FILE;
    if (S_ISDIR(buff.st_mode)) return T_DIR;
    if (S_ISLNK(buff.st_mode)) return T_LINK;
    return T_UNKNOWN;
}

/**
 * type of current entry using this project's macros
 */
int getTypeFromDirent(dirent *ent) {
    switch (ent->d_type) {
        case 4:
            return T_DIR;
        case 8:
            return T_FILE;
        case 10:
            return T_LINK;
        default:
            return T_UNKNOWN;
    }
}

int isDir(char *path) {
    return getTypeFromPath(path) == T_DIR;
}

/**
 * @param dir directory in string
 * @return string with final directory, NULL if path isn't a directory
 */
char *parseRootDir(char *dir) {
    if (isDir(dir)) {
        char *rootDir = malloc(sizeof(char) * (1 + strlen(dir)));
        memcpy(rootDir, dir, strlen(dir) + 1);
        return rootDir;
    }
    return NULL;
}

void killThread() {
    failedThreads++;
    pthread_exit(NULL);
}

/**
 * @param dir current directory
 * @param entry current entry name
 * @return full path of file in format "dir/entry"
 */
char *pathJoin(char *dir, char *entry) {
    unsigned long strSize = strlen(dir) + 1 + strlen(entry) + 1; // Add 1 for null byte
    char *dst = malloc(sizeof(char) * strSize);
    snprintf(dst, strSize, "%s/%s", dir, entry);
    dst[strSize - 1] = '\0';
    return dst;
}

int hasReadPermission(char *path) {
    struct stat fileStat;
    if (lstat(path, &fileStat)) {
        return 0;
    }
    return (fileStat.st_mode & S_IRUSR) && (fileStat.st_mode & S_IXUSR);
}

/**
 * @param dir path of current directory
 * @param entry current dir entry
 * @param searchTerm searchTerm we're looking for
 * @param queue our queue
 *
 * This function handles the entry and adds it to the queue if it's a directory, ignores it if it's a symbolic link
 * and if it's a regular file, matches it against our searchTerm and prints the full path if it's a match.
 */
void handleEntry(char *dir, dirent *entry, char *searchTerm) {
    char *newPath;
    if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
        newPath = pathJoin(dir, entry->d_name);
        switch (getTypeFromDirent(entry)) {
            case T_DIR:
                if (hasReadPermission(newPath)) {
                    enQueue(newPath);
                } else {
                    printf("Directory %s: Permission denied.\n", newPath);
                }
                break;
            case T_LINK:
            case T_FILE:
                if (strstr(entry->d_name, searchTerm) != NULL) {
                    foundFiles++;
                    printf("%s\n", newPath);
                }
                free(newPath);
                break;
            default:
                break;
        }
    }
}

/**
 * @param path current directory we're parsing
 * @param searchTerm searchTerm we're looking for
 * @param queue our queue
 *
 * Handle directory by scanning all it's contents and treating every entry according to it's type
 */
void handleDirectory(char *path, char *searchTerm) {
    DIR *dir;
    dirent *entry;


    // Open directory
    if ((dir = opendir(path)) == NULL) {
        printf("Directory %s: Permission denied.\n", path);
        return;
    }

    // Reset errno to know why we stopped reading directory entries
    errno = 0;
    while ((entry = readdir(dir)) != NULL) {
        handleEntry(path, entry, searchTerm);
    }

    // Close directory
    closedir(dir);
    if (errno != 0) {
        printf("failed reading dir %s\n", path);
        killThread();
    }
}

int isDone() {
    pthread_rwlock_rdlock(&queueRWLock);
    int isDone = unsafeGetQueueSize() == 0 && getRunningThreads() == 0;
    pthread_rwlock_unlock(&queueRWLock);
    return isDone;
}

/**
 * Entry point for all threads
 * At first, thread will wait for condition that all threads have done being initiated,
 * and only then will it start handling directories
 */
void *threadMain(void *searchTerm) {
    char *path;
    pthread_mutex_lock(&startLock);
    createdProcesses++;
    if (createdProcesses == parallelism) {
        pthread_cond_signal(&doneInitCond);
    }

    // For last thread, this line might be hit after main broadcasts to start, but it will stop waiting on one of the
    // following queueConsumableCond signals once a new item is added to the queue
    if (threadsSignaled == 0) {
        pthread_cond_wait(&queueConsumableCond, &startLock);
    }
    pthread_mutex_unlock(&startLock);
    while (1) {
        path = deQueue();
        if (path != NULL) {
            handleDirectory(path, (char *) searchTerm);
            free(path);
        }
        decRunningThreads(); // Thread is done handling everything
        if (isDone()) {
            pthread_cond_broadcast(&queueConsumableCond);
            break;
        }
    }
    pthread_exit(NULL);
}

/**
 * Allocate memory for thread array and start them
 */
void initThreads(pthread_t **threads, char *searchTerm) {
    if ((*threads = malloc(sizeof(pthread_t) * parallelism)) == NULL) {
        printf("failed allocating memory for threads\n");
        exit(1);
    }

    pthread_t *limit = *threads + parallelism;
    for (pthread_t *tmpThread = *threads; tmpThread < limit; tmpThread++) {
        pthread_create(tmpThread, NULL, threadMain, searchTerm);
    }
}

/**
 * Wait for all threads to finish
 */
void waitForThreads(pthread_t *threads) {
    pthread_t *limit = threads + parallelism;
    for (pthread_t *tmpThread = threads; tmpThread < limit; tmpThread++) {
        pthread_join(*tmpThread, NULL);
    }

}

/**
 * Init mutexes and condition variables
 */
void initThreadingVars() {
    pthread_mutex_init(&queueLock, NULL);
    pthread_mutex_init(&startLock, NULL);
    pthread_rwlock_init(&runningThreadsLock, NULL);
    pthread_rwlock_init(&queueRWLock, NULL);
    pthread_cond_init(&queueConsumableCond, NULL);
    pthread_cond_init(&doneInitCond, NULL);
}

/**
 * Destroy mutexes and condition variables
 */
void destroyThreadingVars() {
    pthread_mutex_destroy(&queueLock);
    pthread_mutex_destroy(&startLock);
    pthread_rwlock_destroy(&runningThreadsLock);
    pthread_rwlock_destroy(&queueRWLock);
    pthread_cond_destroy(&queueConsumableCond);
    pthread_cond_destroy(&doneInitCond);
}

int main(int c, char *args[]) {
    char *rootDir, *searchTerm;
    pthread_t *threads;
    if (c != 4) {
        printf("wrong amount of arguments given\n");
    }

    if ((rootDir = parseRootDir(args[1])) == NULL) {
        printf("first argument is not a valid directory\n");
        exit(1);
    }

    searchTerm = args[2];
    if (parseParallelism(args[3], &parallelism) < 0) {
        printf("bad thread count given\n");
        exit(1);
    }

    // Init our environment
    initQueue();
    initThreadingVars();
    initThreads(&threads, searchTerm);

    // Wait for all threads to have been created and the start all threads
    pthread_mutex_lock(&startLock);
    if (createdProcesses != parallelism) {
        pthread_cond_wait(&doneInitCond, &startLock);
    }
    unsafeEnQueue(rootDir);
    pthread_cond_broadcast(&queueConsumableCond);
    threadsSignaled = 1;
    pthread_mutex_unlock(&startLock);

    // Wait for all threads to finish
    waitForThreads(threads);
    printf("Done searching, found %d files\n", foundFiles);

    // Cleanup our environment
    destroyThreadingVars();
    free(queue);
    free(threads);

    // Check if any thread failed
    if (failedThreads > 0) {
        exit(1);
    }
}
