# pfind
#### Personal steps

##### Step I
First implement pfind without parallelism. Once it works, check for memort leaks by first compiling 
with `-g` flag and then running valgrind:
```
gcc -Wall -std=c11 -g pfind.c -o pfind
valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes --verbose ./pfind /Users/sid/Documents/studies "pdf" 1
```

And hope it prints out the following: `ERROR SUMMARY: 0 errors from 0 contexts`.

If it doesn't, start debugging and fixing.

##### Step II
Start implementing parallelism. You'll probably have deadlocks at first so you'll want to debug it. For simplicity, try first
debugging it with parallelism 2.

Add prints EVERYWHERE (especially before / after EVERY lock). To be able to differentiate between different threads easily, 
you'll need to print the thread ID. To do so, add the following func:
```
void printWithTs(char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    printf("[%d] : %lu : ", pthread_self(), getNanoTs());
    vprintf(fmt, args);
    va_end( args );
}
```
And use this INSTEAD of `printf`. This will format the prints in the following format:
```
[21377024] : 1608721687061694000 : unlocking queue size
[21377024] : 1608721687061697000 : unlocking queue lock
[21913600] : 1608721687061705000 : locking queue items
[21913600] : 1608721687061708000 : locking queue size
[21377024] : 1608721687061715000 : done dequeueing
```
[threadId] : timestamp : message

That way you can easily differentiate between what happens in which thread.

I've seen cases that the timestamps could be in the wrong order, so if the order is important to you 
(and it should be in many cases), make sure you analyze the logs in the correct order
