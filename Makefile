test:
	goapp test ./... -v -run=$(grep)
build:
	goapp build ./...
update:
	goapp get -u ./... -v