.PHONY: tearup
tearup:
	@echo "+ $@"
	docker-compose up -d

.PHONY: teardown
teardown:
	@echo "+ $@"
	docker-compose rm -fsv

.PHONY: test-run
test-run:
	@echo "+ $@"
	go test -v -p 1 -race -coverprofile=coverage.txt -covermode=atomic ./...

.PHONY: test
test: tearup test-run teardown
