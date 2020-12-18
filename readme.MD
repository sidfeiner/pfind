# pfind
#### Personal steps
First implement pfind without parallelism. Once it works, check for memort leaks by first compiling 
with `-g` flag and then running valgrind:
```
gcc -Wall -std=c11 -g pfind.c -o pfind
valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes --verbose ./pfind /Users/sid/Documents/studies "pdf" 1
```

And hope it prints out the following: `ERROR SUMMARY: 0 errors from 0 contexts`.

If it doesn't, start debugging and fixing.