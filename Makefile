DOCS_COMMAND=cargo doc --no-deps

docs: target/doc/**/*
	$(DOCS_COMMAND)

docs-view:
	$(DOCS_COMMAND) --open