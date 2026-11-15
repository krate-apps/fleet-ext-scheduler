.PHONY: build pack
build:
	mkdir -p bin
	go build -trimpath -o bin/scheduler-worker ./cmd/scheduler-worker

pack:
	bash scripts/pack.sh dist
