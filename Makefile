.PHONY: fmt vet test race bench perf-publish perf-soak perf-short frontend-install frontend-test frontend-build build verify compose-up compose-down

fmt:
	gofmt -w $$(find cmd internal pkg -name '*.go' -type f 2>/dev/null)

vet:
	go vet ./...

test:
	go test ./...

race:
	go test -race ./...

bench:
	go test -run '^$$' -bench . -benchmem ./internal/ring ./pkg/minilogback

perf-publish:
	go run ./cmd/perfcheck -mode publish -audit both -warmup 3s -duration 10s

perf-soak:
	go run ./cmd/perfcheck -mode soak -audit off -warmup 30s -duration 30m -rate 100000

perf-short:
	go run ./cmd/perfcheck -mode soak -audit off -warmup 1s -duration 5s -rate 10000

frontend-install:
	cd frontend && npm ci

frontend-test:
	cd frontend && npm test -- --run

frontend-build:
	cd frontend && npm run build

build: frontend-build
	go build ./...

verify: vet test frontend-test frontend-build

compose-up:
	docker compose up --build -d

compose-down:
	docker compose down
