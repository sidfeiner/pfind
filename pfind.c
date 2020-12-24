#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <errno.h>
#include <sys/stat.h>
#include <pthread.h>
#include <dirent.h>
#include <stdatomic.h>
#include <math.h>

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
pthread_rwlock_t rwLock;

int parallelism;
pthread_cond_t queueConsumableCond;
pthread_cond_t doneInitCond;
atomic_int createdProcesses;
atomic_int foundFiles;
atomic_int runningThreads;
atomic_int failedThreads;

long getNanoTs(void) {
    struct timespec spec;
    clock_gettime(CLOCK_REALTIME, &spec);
    return (int64_t) (spec.tv_sec) * (int64_t) 1000000000 + (int64_t) (spec.tv_nsec);
}

pthread_mutex_t printLock;

void printWithTs(char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    pthread_mutex_lock(&printLock);
    printf("[%d] : %lu : ", pthread_self(), getNanoTs());
    vprintf(fmt, args);
    pthread_mutex_unlock(&printLock);
    va_end(args);
}

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
    pthread_rwlock_rdlock(&rwLock);
    int size = unsafeGetQueueSize();
    pthread_rwlock_unlock(&rwLock);
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
    pthread_rwlock_wrlock(&rwLock);
    unsafeEnQueue(str);
    pthread_rwlock_unlock(&rwLock);
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
    pthread_mutex_lock(&queueLock);
    while (unsafeGetQueueSize() == 0 && runningThreads > 0) {
        pthread_cond_wait(&queueConsumableCond, &queueLock);
    }
    pthread_mutex_unlock(&queueLock);
    pthread_rwlock_wrlock(&rwLock);
    char *path = unsafeDeQueue();
    pthread_rwlock_unlock(&rwLock);
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

int hasReadPermission(char *path) {
    struct stat s;
    lstat(path, &s);
    return (s.st_mode & S_IRUSR) || (s.st_mode & S_IRGRP);
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
                printWithTs("unknown type format: %d\n", entry->d_type);
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
    pthread_mutex_lock(&startLock);
    createdProcesses++;
    if (createdProcesses == parallelism) {
        pthread_cond_signal(&doneInitCond);
    }
    pthread_cond_wait(&queueConsumableCond, &startLock);
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
            pthread_cond_broadcast(&queueConsumableCond);
            break;
        }
    }
    pthread_exit(NULL);
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

    if ((threads = malloc(sizeof(pthread_t) * parallelism)) == NULL) {
        printf("failed allocating memory for threads\n");
        exit(1);
    }
    pthread_t *limit = threads + parallelism;

    pthread_mutex_init(&queueLock, NULL);
    pthread_rwlock_init(&rwLock, NULL);
    pthread_cond_init(&queueConsumableCond, NULL);
    pthread_cond_init(&doneInitCond, NULL);

    queue = newQueue();

    for (pthread_t *tmpThread = threads; tmpThread < limit; tmpThread++) {
        pthread_create(tmpThread, NULL, threadMain, searchTerm);
    }

    unsafeEnQueue(rootDir);

    pthread_mutex_lock(&startLock);
    pthread_cond_wait(&doneInitCond, &startLock);
    pthread_cond_broadcast(&queueConsumableCond);
    pthread_mutex_unlock(&startLock);

    for (pthread_t *tmpThread = threads; tmpThread < limit; tmpThread++) {
        pthread_join(*tmpThread, NULL);
    }

    printf("Done searching, found %d files\n", foundFiles);

    pthread_mutex_destroy(&printLock);
    pthread_mutex_destroy(&queueLock);
    pthread_rwlock_destroy(&rwLock);
    pthread_cond_destroy(&queueConsumableCond);
    pthread_cond_destroy(&doneInitCond);

    free(queue);
    free(threads);

    if (failedThreads > 0) {
        exit(1);
    }
}
