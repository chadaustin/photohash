# Building

## Ubuntu 24.04

```
sudo apt install -y build-essential cmake nasm pkg-config libheif-dev libsqlite3-dev libz-dev
```

## Ubuntu 22.04

```
sudo apt install -y build-essential cmake nasm pkg-config libsqlite3-dev

sudo add-apt-repository ppa:strukturag/libheif
sudo add-apt-repository ppa:strukturag/libde265
sudo apt update
sudo apt install -y libheif-dev
```

## Debian

```
sudo apt install -y build-essential cmake nasm pkg-config libheif-dev libsqlite3-dev
```

## Cygwin

Install Visual Studio Build Tools 2019. Make sure to select the
Windows 10 SDK.

Ensure Git for Windows is installed:

```
winget install -e --id Git.Git
```

```
cargo install cargo-vcpkg
PATH="/cygdrive/c/Program Files/Git/bin:$PATH" cargo vcpkg -v build
cargo build
```

## Windows

Install CMake and nasm.exe.

```
cargo install cargo-vcpkg
cargo vcpkg build
cargo build
```
