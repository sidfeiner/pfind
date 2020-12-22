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


/**
 * Allocate memory for queue and initiate value
 */
Queue *newQueue() {
    Queue *queue = malloc(sizeof(Queue));
    queue->size = 0;
    queue->first = NULL;
    queue->last = NULL;
    return queue;
}

Queue *queue;
pthread_mutex_t queueLockLock;
pthread_mutex_t queueItemsLock;
pthread_rwlock_t queueSizeLock;

pthread_cond_t queueConsumableCond;
pthread_cond_t queueProducableCond;
atomic_int foundFiles;
atomic_int runningThreads;
atomic_int failedThreads;

/**
 * Use ReadLock for queue's size
 */
void lockQueueSize() {
    printf("locking queue size\n");
    pthread_rwlock_wrlock(&queueSizeLock);
}

/**
 * Unlock ReadLock for queue's size
 */
void unlockQueueSize() {
    printf("unlocking queue size\n");
    pthread_rwlock_wrlock(&queueSizeLock);
}

/**
 * Lock the queue (all fields)
 */
void lockQueue() {
    printf("locking queue lock\n");
    pthread_mutex_lock(&queueLockLock);
    printf("locking queue items\n");
    pthread_mutex_lock(&queueItemsLock);
    lockQueueSize();
}

/**
 * Unlock queue
 */
void unlockQueue() {
    printf("unlocking queue items\n");
    pthread_mutex_unlock(&queueItemsLock);
    unlockQueueSize();
    printf("unlocking queue lock\n");
    pthread_mutex_unlock(&queueLockLock);
}


/**
 * return amount of items in queue
 */
int unsafeGetQueueSize() {
    return queue->size;
}

/**
 * Safely return the amount of items in queue (lock only queue's size)
 */
int getQueueSize() {
    lockQueueSize();
    int size = unsafeGetQueueSize();
    unlockQueueSize();
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
    pthread_cond_signal(&queueConsumableCond);
}

/**
 *  Add item to queue
 */
void enQueue(char *str) {
    printf("enqueuing...\n");
    lockQueue();
    unsafeEnQueue(str);
    unlockQueue();
    printf("done queueing...\n");
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
    printf("dequeueing...\n");
    lockQueue();
    while (unsafeGetQueueSize() == 0) {
        printf("running cond_wait\n");
        pthread_cond_wait(&queueConsumableCond, &queueItemsLock);
    }
    char *path = unsafeDeQueue();
    pthread_cond_signal(&queueProducableCond);
    unlockQueue();
    printf("done dequeueing\n");
    return path;
}

/**
 * @param strParallelism parallelism from input
 * @param intParallelism where to store the int result
 * @return -1 if conversion failed, 0 if successfull
 */
int parseParallelism(char *strParallelism, int *intParallelism) {
    int parallelism = (int) strtol(strParallelism, NULL, 10);
    if (parallelism == UINT_MAX && errno == ERANGE) {
        return -1;
    }
    *intParallelism = parallelism;
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
        case DT_DIR:
            return T_DIR;
        case DT_REG:
            return T_FILE;
        case DT_LNK:
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
    printf("handling entry: %s/%s\n", dir, entry->d_name);
    if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
        newPath = pathJoin(dir, entry->d_name);
        switch (getTypeFromDirent(entry)) {
            case T_DIR:
                enQueue(newPath);
                break;
            case T_LINK:
                break;
            case T_FILE:
                if (strstr(entry->d_name, searchTerm) != NULL) {
                    foundFiles++;
                    printf("%s\n", newPath);
                }
                free(newPath);
                break;
            default:
                printf("unknown type format: %d\n", entry->d_type);
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

    printf("handling directory: %s\n", path);

    // Open directory
    if ((dir = opendir(path)) == NULL) {
        printf("failed opening dir\n");
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

void *threadMain(void *searchTerm) {
    char *path;
    printf("starting thread main\n");
    while (1) {
        path = deQueue();
        printf("dequeud path: %s\n", path);
        if (path != NULL) {
            runningThreads++;
            handleDirectory(path, (char *) searchTerm);
            free(path);
            runningThreads--;
        }
        if (runningThreads == 0 && getQueueSize() == 0) {
            break;
        }
    }
    pthread_exit(NULL);
}

int main(int c, char *args[]) {
    char *rootDir, *searchTerm;
    int parallelism;
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

    if ((threads = malloc(sizeof(pthread_t) * parallelism)) == NULL) {
        printf("failed allocating memory for threads\n");
        exit(1);
    }
    pthread_t *limit = threads + parallelism;


    pthread_mutex_init(&queueLockLock, NULL);
    pthread_mutex_init(&queueItemsLock, NULL);
    pthread_rwlock_init(&queueSizeLock, NULL);
    pthread_cond_init(&queueProducableCond, NULL);
    pthread_cond_init(&queueConsumableCond, NULL);

    for (pthread_t *tmpThread = threads; tmpThread < limit; tmpThread++) {
        pthread_create(tmpThread, NULL, threadMain, searchTerm);
    }

    queue = newQueue();
    unsafeEnQueue(rootDir);

    for (pthread_t *tmpThread = threads; tmpThread < limit; tmpThread++) {
        pthread_join(*tmpThread, NULL);
    }

    pthread_mutex_destroy(&queueItemsLock);
    pthread_rwlock_destroy(&queueSizeLock);

    printf("Done searching, found %d files\n", foundFiles);
    free(queue);
    free(threads);

    if (failedThreads > 0) {
        exit(1);
    }
}
