#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <errno.h>

int main(int c, char *args[]) {
    char *rootDir, *searchTerm;
    int parallelism;

    if (c !=4 ){
        printf("wrong amount of arguments given\n");
    }

    rootDir = args[1];
    searchTerm = args[2];
    parallelism = (int) strtol(args[3], NULL, 10);
    if (parallelism == UINT_MAX && errno == ERANGE) {
        perror("bad thread count given");
        exit(1);
    }
}
