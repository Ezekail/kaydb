build:
	go mod download
	go build -o kaydb-server.exe ./cmd/