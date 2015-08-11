test:
	go test ./... -v
build:
	go build ./...
update:
	goapp get -u ./... -v