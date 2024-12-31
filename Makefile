all: install

binutil:
	go build -o binutil
install: binutil
	install binutil ${GOPATH}/bin
