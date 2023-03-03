fn main() {
    if cfg!(windows) {
        vcpkg::find_package("libheif").unwrap();
    }
}
