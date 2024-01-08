# one billion rows rust

Dummy implementation of the [One Billion challenge](https://github.com/gunnarmorling/1brc/) in rust.

Optimized for linux (might not work in other platforms).

## options

`-t` : number of worker threads spawned. default to number of availaible cpu threads

## on my laptop

#### time

```
time ./target/release/one_billion_rows_challenge -t 48
real    0m10,361s
user    1m42,832s
sys     0m3,526s
```

#### hyperfine

```
➜  one_billion_rust git:(main) ✗ hyperfine "./target/release/one_billion_rows_challenge -t 48" --runs 5
Benchmark 1: ./target/release/one_billion_rows_challenge -t 48
  Time (mean ± σ):     10.246 s ±  0.588 s    [User: 102.523 s, System: 4.382 s]
  Range (min … max):    9.575 s … 10.812 s    5 runs
```
