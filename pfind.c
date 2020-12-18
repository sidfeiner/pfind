#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <errno.h>
#include <sys/stat.h>
#include <pthread.h>
#include <dirent.h>

#define T_UNKNOWN 0
#define T_FILE 1
#define T_DIR 2
#define T_LINK 3

typedef struct dirent dirent;

typedef struct _QueueItem {
    char *value;
    struct _QueueItem *next;
} QueueItem;

typedef struct _Queue {
    int size;
    QueueItem *first;
    QueueItem *last;
} Queue;

int foundFiles;

/**
 * Add string item to queue
 */
void enQueue(Queue *q, char *path) {
    QueueItem *qItem = malloc(sizeof(QueueItem));
    qItem->next = NULL;
    qItem->value = path;
    if (q->size == 0) {
        q->first = qItem;
    } else {
        q->last->next = qItem;
    }
    q->last = qItem;
    q->size++;
}

/**
 * Return first item in queue, NULL if empty
 */
char *deQueue(Queue *q) {
    char *value;
    QueueItem *qItem;
    if (q->size == 0) {
        return NULL;
    }
    qItem = q->first;
    value = qItem->value;
    q->first = qItem->next;
    if (q->size == 1) {
        q->last = NULL;
    }
    q->size -= 1;
    free(qItem);
    return value;

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

int isLink(char *path) {
    return getTypeFromPath(path) == T_LINK;
}

int isFile(char *path) {
    return getTypeFromPath(path) == T_FILE;
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
void handleEntry(char *dir, dirent *entry, char *searchTerm, Queue *queue) {
    char *newPath;
    if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
        newPath = pathJoin(dir, entry->d_name);
        switch (getTypeFromDirent(entry)) {
            case T_DIR:
                enQueue(queue, newPath);
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
void handleDirectory(char *path, char *searchTerm, Queue *queue) {
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
        handleEntry(path, entry, searchTerm, queue);
    }

    // Close directory
    closedir(dir);
    if (errno == 0) {
        printf("failed reading dir %s\n", path);
        exit(0);
    }
}

int main(int c, char *args[]) {
    char *rootDir, *searchTerm, *path;
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

    threads = malloc(sizeof(pthread_t) * parallelism);

    Queue *queue = malloc(sizeof(Queue));
    queue->size = 0;
    queue->first = NULL;
    queue->last = NULL;
    enQueue(queue, rootDir);

    while ((path = deQueue(queue)) != NULL /* && all threads are waiting */) {
        handleDirectory(path, searchTerm, queue);
        free(path);
    }

    printf("Done searching, found %d files\n", foundFiles);
    free(queue);
    free(threads);
}
