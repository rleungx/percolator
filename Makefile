default: build

build: 
	cargo build

test:
	cargo test --features "no-fail" 
	cargo test --features "default" -- --test-threads=1