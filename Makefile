DOCS_COMMAND=cargo doc --no-deps

docs: target/doc/**/*
	$(DOCS_COMMAND)

serve-docs:
	$(DOCS_COMMAND) --open