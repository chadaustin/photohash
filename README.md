# Building

## Ubuntu

```
sudo add-apt-repository ppa:strukturag/libheif
sudo add-apt-repository ppa:strukturag/libde265
sudo apt update
sudo apt install libheif-dev
```

## Windows

```
vcpkg install libheif:x64-windows-static
export VCPKGRS_DYNAMIC=1
cargo build
```
