default: build

build: 
	cargo build

test:
	cargo test --features "no-fail" 
	cargo test -- --test-threads=1

clean:
	cargo clean
