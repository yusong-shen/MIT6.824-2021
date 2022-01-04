## Reminding
Please try your best to finish this lab by yourself~

### Testing
coordinator:
```
# cd src/main

go build -race -buildmode=plugin ../mrapps/wc.go
rm mr-out*
# go run -race mrcoordinator.go pg-*.txt

go run -race mrcoordinator.go pg-being_ernest.txt pg-dorian_gray.txt

```

workers:
```
# cd src/main

go run -race mrworker.go wc.so
```

### Performance
1 minute 12 second for first 5 tests by disable logging.
```
$ bash test-mr.sh 
Current time : 15:13:59
*** Starting wc test.
--- wc test: PASS
Current time : 15:14:13
*** Starting indexer test.
--- indexer test: PASS
Current time : 15:14:23
*** Starting map parallelism test.
--- map parallelism test: PASS
Current time : 15:14:34
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
Current time : 15:14:47
*** Starting job count test.
--- job count test: PASS
Current time : 15:15:11
*** PASSED ALL TESTS
```
