fmt:
	go fmt ./...

proto:
	protoc --go_out=. --go_opt=paths=source_relative ./command/command.proto
