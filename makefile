ncache: main.go
	go build -o ncache main.go
.PHONY: clean reset
clean:
	rm -rf ncache Ncache.log
reset:
	make clean && make
