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
