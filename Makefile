all:
	golangci-lint run
	go test -v -cover ./...
	go build
