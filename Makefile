run:
	cd examples/local && go run main.go

clean:
	rm -rf ./examples/local/logs/*

.PHONY: run clear
