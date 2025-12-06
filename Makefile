GO           ?= go
LINTER       ?= golangci-lint
PKG          := ./...
BINARY_NAME  := app

fmt:
	@echo "Formatting code..."
	@$(GO) fmt $(PKG)
	@goimports -w .

tidy:
	@echo "Tidying go.mod..."
	@$(GO) mod tidy

clean:
	@echo "Cleaning build cache..."
	@$(GO) clean -cache -testcache
	@rm -f $(BINARY_NAME)

lint:
	@echo "Running golangci-lint..."
	@$(LINTER) run \
		--enable-all \
		--max-issues-per-linter=0 \
		--out-format=tab \
		$(PKG)

check: tidy clean fmt lint
	@echo "All checks passed successfully!"

.PHONY: fmt tidy clean lint