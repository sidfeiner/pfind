#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <errno.h>
#include <sys/stat.h>
#include <pthread.h>
#include <dirent.h>
#include <stdatomic.h>

#undef DEBUG

#include <stdarg.h>

#ifdef DEBUG

#include <stdint.h>

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
pthread_mutex_t isDoneLock;
pthread_rwlock_t runningThreadsLock;
pthread_rwlock_t createdThreadsLock;
pthread_rwlock_t waitingThreadsLock;
pthread_rwlock_t queueRWLock;

int parallelism;
pthread_cond_t queueConsumableCond;
pthread_cond_t doneInitCond;
atomic_int threadsSignaled;
atomic_int createdThreads;
atomic_int foundFiles;
atomic_int runningThreads;
atomic_int failedThreads;
atomic_int waitingThreads;
atomic_int programIsDone;

long getNanoTs(void) {
    struct timespec spec;
    clock_gettime(CLOCK_REALTIME, &spec);
    return (int64_t) (spec.tv_sec) * (int64_t) 1000000000 + (int64_t) (spec.tv_nsec);
}

char *debugFormat = "[%02x] : %lu : %d : %d : %d : ";
char *debugLevel = "***";

void debugPrintf(char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    char *placeholder = malloc(strlen(debugFormat) + strlen(debugLevel) + 1);
    char *newFmt = malloc(strlen(debugLevel) + strlen(fmt) + 1);
    snprintf(placeholder, strlen(debugFormat) + strlen(debugLevel) + 1, "%s%s", debugLevel, debugFormat);
    snprintf(newFmt, strlen(debugLevel) + strlen(fmt) + 1, "%s%s", debugLevel, fmt);
    pthread_mutex_lock(&printLock);
    printf(placeholder, pthread_self(), getNanoTs(), runningThreads, waitingThreads, failedThreads);
    vprintf(newFmt, args);
    free(newFmt);
    free(placeholder);
    fflush(stdout);
    pthread_mutex_unlock(&printLock);
    va_end(args);
}

void safePrintf(char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    pthread_mutex_lock(&printLock);
    vprintf(fmt, args);
    pthread_mutex_unlock(&printLock);
    va_end(args);
}

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
 * Returns if the thread is allowed to handle this item.
 * Returns true if:
 *  1. parallelism is 1, so he MUST handle the item
 *  2. queue is not empty, so other waiting thread should not be waiting
 *  3. There is nobody else waiting to consume (everybody is busy)
 *
 *  Otherwise returns false
 */
int unsafeIsAllowedToHandle() {
    return parallelism == 1 || unsafeGetQueueSize() > 0 || waitingThreads == 0;
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
    debugPrintf("locking queue for enqueueing queueRWLock (read/write)\n");
#endif
    pthread_rwlock_wrlock(&queueRWLock);
#ifdef DEBUG
    debugPrintf("done locking queue for enqueueing queueRWLock (read/write)\n");
#endif
    int isAllowedToHandle = unsafeIsAllowedToHandle();
    unsafeEnQueue(str);
#ifdef DEBUG
    debugPrintf("successfully inserted item to queue, now size is %d\n", unsafeGetQueueSize());
#endif
    pthread_cond_signal(&queueConsumableCond);
    pthread_rwlock_unlock(&queueRWLock);
#ifdef DEBUG
    debugPrintf("unlocked queue for enqueueing (read/write).\n");
#endif
    if (!isAllowedToHandle) {
        sched_yield();
    }
}


/**
 * Safely pop and return first item in queue, NULL if empty
 */
char *deQueue() {
    pthread_mutex_lock(&isDoneLock);
    waitingThreads++;
    while (unsafeGetQueueSize() == 0 && waitingThreads < (parallelism - failedThreads) && !programIsDone) {
#ifdef DEBUG
        debugPrintf("locking for cond var dequeueing\n");
#endif
        pthread_cond_wait(&queueConsumableCond, &isDoneLock);
    }
    waitingThreads--;
#ifdef DEBUG
    debugPrintf("unlocking queue after cond var dequeueing\n");
#endif
    pthread_mutex_unlock(&isDoneLock);
#ifdef DEBUG
    debugPrintf("locking queue for dequeueing  queueRWLock(read/write)\n");
#endif
    pthread_rwlock_wrlock(&queueRWLock);
    runningThreads++;
    char *path = unsafeDeQueue();
    pthread_rwlock_unlock(&queueRWLock);
#ifdef DEBUG
    debugPrintf("unlocked queue for dequeueing queueRWLock (read/write). Now size is %d, current item is: %p\n",
                unsafeGetQueueSize(), path);
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
                    safePrintf("Directory %s: Permission denied.\n", newPath);
                    free(newPath);
                }
                break;
            case T_LINK:
            case T_FILE:
                if (strstr(entry->d_name, searchTerm) != NULL) {
                    foundFiles++;
                    safePrintf("%s\n", newPath);
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
        safePrintf("Directory %s: Permission denied.\n", path);
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
        fprintf(stderr, "failed closing dir %s\n", path);
        killThread();
    }
}

int isDone() {
#ifdef DEBUG
    debugPrintf("(isDone) locking queue read lock queueRWLock\n");
#endif
    pthread_rwlock_rdlock(&queueRWLock);
    int isDone = programIsDone == 1 ||
                 (runningThreads == 0 && (waitingThreads == parallelism - 1 - failedThreads) &&
                  unsafeGetQueueSize() == 0);
    pthread_rwlock_unlock(&queueRWLock);
#ifdef DEBUG
    debugPrintf("(isDone) unlocked queue read lock queueRWLock. will return %d\n", isDone);
#endif
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
    createdThreads++;
    if (createdThreads == parallelism) {
#ifdef DEBUG
        debugPrintf("signaling doneInitCond\n");
#endif
        pthread_cond_signal(&doneInitCond);
    }

    // For last thread, this line might be hit after main broadcasts to start, but it will stop waiting on one of the
    // following queueConsumableCond signals once a new item is added to the queue
#ifdef DEBUG
    debugPrintf("waiting for queueConsumableCond when createdThreads = %d\n", getCreatedThreads());
#endif
    if (threadsSignaled == 0) {
        pthread_cond_wait(&queueConsumableCond, &startLock);
    }
//#ifdef DEBUG
//    debugPrintf("done waiting for queueConsumableCond\n");
//#endif
    pthread_mutex_unlock(&startLock);
    while (1) {
        path = deQueue();
        if (path != NULL) {
            handleDirectory(path, (char *) searchTerm);
            free(path);
        }
        runningThreads--; // Thread is done handling everything
        pthread_mutex_lock(&isDoneLock);
        if (isDone()) {
#ifdef DEBUG
            debugPrintf("done going over all queue, broadcasting everyone to continue\n");
#endif
            programIsDone = 1;
            pthread_cond_broadcast(&queueConsumableCond);
            pthread_mutex_unlock(&isDoneLock);
            break;
        }
        pthread_mutex_unlock(&isDoneLock);
    }
    pthread_exit(NULL);
}

/**
 * Allocate memory for thread array and start them
 */
void initThreads(pthread_t **threads, char *searchTerm) {
    if ((*threads = malloc(sizeof(pthread_t) * parallelism)) == NULL) {
        fprintf(stderr, "failed allocating memory for threads\n");
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
    pthread_mutex_init(&startLock, NULL);
    pthread_mutex_init(&queueLock, NULL);
    pthread_mutex_init(&isDoneLock, NULL);
    pthread_rwlock_init(&runningThreadsLock, NULL);
    pthread_rwlock_init(&queueRWLock, NULL);
    pthread_rwlock_init(&createdThreadsLock, NULL);
    pthread_rwlock_init(&waitingThreadsLock, NULL);
    pthread_cond_init(&queueConsumableCond, NULL);
    pthread_cond_init(&doneInitCond, NULL);
}

/**
 * Destroy mutexes and condition variables
 */
void destroyThreadingVars() {
#ifdef DEBUG
    pthread_mutex_destroy(&printLock);
#endif
    pthread_mutex_destroy(&queueLock);
    pthread_mutex_destroy(&isDoneLock);
    pthread_mutex_destroy(&startLock);
    pthread_rwlock_destroy(&runningThreadsLock);
    pthread_rwlock_destroy(&queueRWLock);
    pthread_rwlock_destroy(&createdThreadsLock);
    pthread_rwlock_destroy(&waitingThreadsLock);
    pthread_cond_destroy(&queueConsumableCond);
    pthread_cond_destroy(&doneInitCond);
}

int main(int c, char *args[]) {
    char *rootDir, *searchTerm;
    pthread_t *threads;
#ifdef DEBUG
    pthread_mutex_init(&printLock, NULL);
#endif
    if (c != 4) {
        fprintf(stderr, "wrong amount of arguments given\n");
    }

    if ((rootDir = parseRootDir(args[1])) == NULL) {
        fprintf(stderr, "first argument is not a valid directory\n");
        exit(1);
    }

    searchTerm = args[2];
    if (parseParallelism(args[3], &parallelism) < 0) {
        fprintf(stderr, "bad thread count given\n");
        exit(1);
    }

    // Init our environment
    initQueue();
    initThreadingVars();
    initThreads(&threads, searchTerm);

    // Wait for all threads to have been created and the start all threads
    pthread_mutex_lock(&startLock);
    if (createdThreads != parallelism) {
#ifdef DEBUG
        debugPrintf("waiting for doneInitCond in main\n");
#endif
        pthread_cond_wait(&doneInitCond, &startLock);
    }
#ifdef DEBUG
    debugPrintf("enqueuing root and broadcasting to start\n");
#endif
    unsafeEnQueue(rootDir);
    pthread_cond_broadcast(&queueConsumableCond);
    threadsSignaled = 1;
    pthread_mutex_unlock(&startLock);

    // Wait for all threads to finish
#ifdef DEBUG
    debugPrintf("waiting for all threads to finish\n");
#endif
    waitForThreads(threads);
    safePrintf("Done searching, found %d files\n", foundFiles);

    // Cleanup our environment
    destroyThreadingVars();
    free(queue);
    free(threads);

    // Check if any thread failed
    if (failedThreads > 0) {
        exit(1);
    }
}
