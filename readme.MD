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
long getNanoTs(void) {
    struct timespec spec;
    clock_gettime(CLOCK_REALTIME, &spec);
    return (int64_t) (spec.tv_sec) * (int64_t) 1000000000 + (int64_t) (spec.tv_nsec);
}

void printWithTs(char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    printf("[%02x] : %lu : ", pthread_self(), getNanoTs());
    vprintf(fmt, args);
    va_end(args);
}
```
And use this INSTEAD of `printf`. You can use this function also with parameters (just like `printf`, 
for example `printWithTs("Handling directory: %s", directory)`). This will format the prints in the following format:
```
[aa7d000] : 1608901474622365000 : locking queue for dequeueing (read/write)
[aa7d000] : 1608901474622367000 : unlocking queue for dequeueing (read/write)
[aa7d000] : 1608901474622369000 : done unlocking queue for dequeueing (read/write)
[aa7d000] : 1608901474622370000 : Handling directory: /Users/sid/Documents/studies/operating-systems/msgslot
[aa7d000] : 1608901474622449000 : locking queue for enqueueing (read/write)
[aa7d000] : 1608901474622453000 : unlocking queue for enqueueing (read/write)
[aa7d000] : 1608901474622455000 : done unlocking queue for enqueueing (read/write)
[a9fa000] : 1608901474622471000 : locking queue for enqueueing (read/write)
[a9fa000] : 1608901474622528000 : unlocking queue for enqueueing (read/write)
[a9fa000] : 1608901474622574000 : done unlocking queue for enqueueing (read/write)
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

#### Personal steps
Python 3 tester that randomly generates filesystems for the following cases:
* Normal case (without links or unsearchable directories)
* With links (30 percent of created files will be links, 70 percent will be regular files)
* With unsearchable directories (directories where I remove `read` permission)
* All of the above (both with links and with unsearchable directories)

For every such case, the tester checks that the correct amount of files and link names contain
our search term and that the correct amount of "Permission Denied: <DIR>" were printed during our run.

In addition, it ensures that no line was printed out more than once (could mean that multiple threads
handled the same directory). 

To recognize a deadlock, every run is allowed a timeout and if that is reached, the tester raises an error.
After the error is recognized, the file system will not be deleted so you can run it yourself and debug your program.
To help do that, the tester will print the command that found the bug. Because of the processor's scheduling mechanism, 
it might take a few tries until you hit the same racing condition as the tester reached.