# libheif-sys 1.14.3 and later breaks compilation on Ubuntu 18.04.
cargo-update:
	cargo update
	cargo update -p libheif-sys --precise 1.14.2

install:
	cargo install --locked --profile release-with-debug --path .
