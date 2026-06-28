.PHONY: build run test test-race lint vet clean

BINARY := llm-stream-proxy

build:
	go build -o $(BINARY) .

run: build
	./$(BINARY)

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

lint: vet
	@which staticcheck > /dev/null 2>&1 || go install honnef.co/go/tools/cmd/staticcheck@latest
	staticcheck ./...

bench:
	@which hey > /dev/null 2>&1 || echo "Install hey: go install github.com/rakyll/hey@latest"
	hey -n 1000 -c 50 -m POST \
		-H "Content-Type: application/json" \
		-d '{"messages":[{"role":"user","content":"test"}],"stream":true}' \
		http://localhost:8080/v1/chat/completions

clean:
	rm -f $(BINARY)

docker-build:
	docker build -t $(BINARY) .

docker-run:
	docker run -p 8080:8080 $(BINARY)
