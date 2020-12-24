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

##### Remarks
* Do not try composing the queue lock of multiple sub locks (for example one for the items and one for the queue size).
We use `pthread_cond_wait` and this receives only one mutex, so this must be the only mutex locking the queue.
* If except for `enqueue` and `dequeue` you also have a `getQueueSize` function, then you could use
Read/Write locks. `queue` and `enqueue` will WriteLock but `getQueueSize` will only ReadLock
and this will make `pfind` more scalable because reading the queue size won't block other threads that only just want to
read the queue size.
  * Note that `pthread_cond_wait` receives a mutex and not a RWLock so you must create an additional mutex for this, 
  for example 
  ```
    pthread_mutex_lock(&queueLock);
    while (unsafeGetQueueSize() == 0 && runningThreads > 0) {
        pthread_cond_wait(&queueConsumableCond, &queueLock);
    }
    pthread_mutex_unlock(&queueLock);
    pthread_rwlock_wrlock(&rwLock);
    char *path = unsafeDeQueue();
    pthread_rwlock_unlock(&rwLock);
  ```

##### ReadWrite Locks
They works very similarly to mutexes. Use the following functions:
```
pthread_rwlock_t rwLock;
pthread_rwlock_init(&rwLock, NULL);

pthread_rwlock_rdlock(&rwLock);
// ... read only actions ...
pthread_rwlock_unlock(&rwLock);

pthread_rwlock_wrlock(&rwLock);
// ... read/write actions ...
pthread_rwlock_unlock(&rwLock);

pthread_rwlock_destroy(&rwLock);
```
