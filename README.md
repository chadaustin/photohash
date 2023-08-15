# Building

NOTE: Requires nightly Rust for
`[iter_array_chunks](https://github.com/rust-lang/rust/issues/100450)`.

## Ubuntu

```
sudo apt install -y cmake nasm

sudo add-apt-repository ppa:strukturag/libheif
sudo add-apt-repository ppa:strukturag/libde265
sudo apt update
sudo apt install -y libheif-dev
```

## Windows

```
cargo install cargo-vcpkg
cargo vcpkg build
export VCPKGRS_DYNAMIC=1
cargo build
```
