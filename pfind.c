#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <errno.h>
#include <sys/stat.h>
#include <pthread.h>
#include <dirent.h>
#include <stdatomic.h>

#define DEBUG
#ifdef DEBUG

#include <stdarg.h>

#endif

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
pthread_mutex_t printLock;
pthread_rwlock_t rwLock;

int parallelism;
pthread_cond_t queueConsumableCond;
pthread_cond_t doneInitCond;
atomic_int threadsSignaled;
atomic_int createdProcesses;
atomic_int foundFiles;
atomic_int runningThreads;
atomic_int failedThreads;

#ifdef DEBUG

long getNanoTs(void) {
    struct timespec spec;
    clock_gettime(CLOCK_REALTIME, &spec);
    return (int64_t) (spec.tv_sec) * (int64_t) 1000000000 + (int64_t) (spec.tv_nsec);
}


void printWithTs(char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    pthread_mutex_lock(&printLock);
    printf("[%02x] : %lu : ", pthread_self(), getNanoTs());
    vprintf(fmt, args);
    pthread_mutex_unlock(&printLock);
    va_end(args);
}

#endif

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
#ifdef DEBUG
    printWithTs("locking queue for queue size (read)\n");
#endif
    pthread_rwlock_rdlock(&rwLock);
    int size = unsafeGetQueueSize();
#ifdef DEBUG
    printWithTs("unlocking queue for queue size (read)\n");
#endif
    pthread_rwlock_unlock(&rwLock);
#ifdef DEBUG
    printWithTs("done unlocking queue for queue size (read)\n");
#endif
    return size;
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
#ifdef DEBUG
    printWithTs("locking queue for enqueueing (read/write)\n");
#endif
    pthread_rwlock_wrlock(&rwLock);
    unsafeEnQueue(str);
#ifdef DEBUG
    printWithTs("unlocking queue for enqueueing (read/write)\n");
#endif
    pthread_rwlock_unlock(&rwLock);
#ifdef DEBUG
    printWithTs("done unlocking queue for enqueueing (read/write)\n");
#endif
    pthread_cond_signal(&queueConsumableCond);
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

/**
 * Safely pop and return first item in queue, NULL if empty
 */
char *deQueue() {
#ifdef DEBUG
    printWithTs("locking for cond var dequeueing\n");
#endif
    pthread_mutex_lock(&queueLock);
    while (unsafeGetQueueSize() == 0 && runningThreads > 0) {
        pthread_cond_wait(&queueConsumableCond, &queueLock);
    }
#ifdef DEBUG
    printWithTs("unlocking queue for cond var dequeueing\n");
#endif
    pthread_mutex_unlock(&queueLock);
#ifdef DEBUG
    printWithTs("locking queue for dequeueing (read/write)\n");
#endif
    pthread_rwlock_wrlock(&rwLock);
    char *path = unsafeDeQueue();
#ifdef DEBUG
    printWithTs("unlocking queue for dequeueing (read/write)\n");
#endif
    pthread_rwlock_unlock(&rwLock);
#ifdef DEBUG
    printWithTs("done unlocking queue for dequeueing (read/write)\n");
#endif
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
    return fileStat.st_mode & S_IRUSR;
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

    printWithTs("Handling directory: %s\n", path);

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
#ifdef DEBUG
        printWithTs("signaling doneInitCond\n");
#endif
        pthread_cond_signal(&doneInitCond);
    }

    // For last thread, this line might be hit after main broadcasts to start, but it will stop waiting on one of the
    // following queueConsumableCond signals once a new item is added to the queue
#ifdef DEBUG
    printWithTs("waiting for queueConsumableCond when createdProcesses = %d\n", createdProcesses);
#endif
    if (threadsSignaled == 0) {
        pthread_cond_wait(&queueConsumableCond, &startLock);
    }
#ifdef DEBUG
    printWithTs("done waiting for queueConsumableCond\n");
#endif
    pthread_mutex_unlock(&startLock);
    while (1) {
        path = deQueue();
        if (path != NULL) {
            runningThreads++;
            handleDirectory(path, (char *) searchTerm);
            free(path);
            runningThreads--;
        }
        if (runningThreads == 0 && getQueueSize() == 0) {
#ifdef DEBUG
            printWithTs("done going over all queue, broadcasting everyone to continue\n");
#endif
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
    pthread_rwlock_init(&rwLock, NULL);
#ifdef DEBUG
    printWithTs("initing queueConsumableCond condition variable\n");
#endif
    pthread_cond_init(&queueConsumableCond, NULL);
    pthread_cond_init(&doneInitCond, NULL);
}

/**
 * Destroy mutexes and condition variables
 */
void destroyThreadingVars() {
    pthread_mutex_destroy(&printLock);
    pthread_mutex_destroy(&queueLock);
    pthread_mutex_destroy(&startLock);
    pthread_rwlock_destroy(&rwLock);
    pthread_cond_destroy(&queueConsumableCond);
    pthread_cond_destroy(&doneInitCond);
}

int main(int c, char *args[]) {
    char *rootDir, *searchTerm;
    pthread_t *threads;
    pthread_mutex_init(&printLock, NULL);
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
#ifdef DEBUG
        printWithTs("waiting for doneInitCond in main\n");
#endif
        pthread_cond_wait(&doneInitCond, &startLock);
    } else {
#ifdef DEBUG
        printWithTs("all threads have already been created, no need to wait in main\n");
#endif
    }
#ifdef DEBUG
    printWithTs("enqueuing root and broadcasting to start\n");
#endif
    unsafeEnQueue(rootDir);
    pthread_cond_broadcast(&queueConsumableCond);
    threadsSignaled = 1;
    pthread_mutex_unlock(&startLock);

    // Wait for all threads to finish
#ifdef DEBUG
    printWithTs("waiting for all threads to finish\n");
#endif
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
