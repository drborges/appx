test:
	go test ./... -run=$(grep) -v
build:
	go build ./...
update:
	goapp get -u ./... -v