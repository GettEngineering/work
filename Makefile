GOLANGCI_LINT_VERSION ?= v1.60.1
GOIMPORTS_VERSION     ?= v0.22.0
GCI_VERSION           ?= v0.13.4
GOFUMPT_VERSION       ?= v0.6.0

FILES_GO = $(shell find . -type f -name '*.go')

help:
	@echo "$$(grep -hE '^\S+:.*##' $(MAKEFILE_LIST) | sed -e 's/:.*##\s*/:/' | column -c2 -t -s :)"
.PHONY: help

format: ## Format code
	@echo "+ $@"
	@go run golang.org/x/tools/cmd/goimports@$(GOIMPORTS_VERSION) -local "github.com/gtforge" -w $(FILES_GO)
	@go run github.com/daixiang0/gci@$(GCI_VERSION) write \
		-s standard \
		-s default \
		-s "Prefix(github.com/GettEngineering/work)" $(FILES_GO)
	@go run mvdan.cc/gofumpt@$(GOFUMPT_VERSION) -l -w .
.PHONY: format

lint: ## Run linter
	@echo "+ $@"
	go run github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run --allow-parallel-runners
.PHONY: lint

test-setup: ## Prepare infrastructure for tests
	@echo "+ $@"
	docker-compose up -d
.PHONY: test-setup

test-teardown: ## Bring down test infrastructure
	@echo "+ $@"
	docker-compose rm -fsv
.PHONY: test-teardown

test-run: ## Run tests
	@echo "+ $@"
	go test -v -p 1 -race -coverprofile=coverage.txt -covermode=atomic ./...
.PHONY: test-run

test: ## Prepare infrastructure and run tests with redigo and goredisv8 adapters
	make test-setup
	make test-run -e TEST_REDIS_ADAPTER="redigo"
	make test-run -e TEST_REDIS_ADAPTER="goredisv8"
	make test-teardown
.PHONY: test

bench-run-goworker:
	@echo "+ $@"
	go run ./benches/bench_goworker/main.go -queues="myqueue,myqueue2,myqueue3,myqueue4,myqueue5" -namespace="bench_test:" -concurrency=50 -use-number
.PHONY: bench-run-goworker

bench-run-goworkers:
	@echo "+ $@"
	go run ./benches/bench_goworkers/main.go
.PHONY: bench-run-goworkers

bench-run-jobs:
	@echo "+ $@"
	go run ./benches/bench_jobs/main.go
.PHONY: bench-run-jobs

bench-run-work:
	@echo "+ $@"
	go run ./benches/bench_work/main.go
.PHONY: bench-run-work

bench-goworker: ## Prepare infrastructure and run benchmarks for benmanns/goworker
	$(call runbench,bench-run-goworker,goredisv8)
.PHONY: bench-goworker

bench-goworkers: ## Prepare infrastructure and run benchmarks for jrallison/go-workers
	$(call runbench,bench-run-goworkers,goredisv8)
.PHONY: bench-goworker

bench-jobs: ## Prepare infrastructure and run benchmarks for albrow/jobs
	$(call runbench,bench-run-jobs,goredisv8)
.PHONY: bench-jobs

bench-work-redigo: ## Prepare infrastructure and run benchmarks with redigo adapter
	$(call runbench,bench-run-work,redigo)
.PHONY: bench-goworker-redigo

bench-work-goredisv8: ## Prepare infrastructure and run benchmarks with goredisv8 adapter
	$(call runbench,bench-run-work,goredisv8)
.PHONY: bench-work-goredisv8

define runbench
	make test-setup
	make $(1) -e REDIS_ADAPTER="$(2)"
	make test-teardown
endef
