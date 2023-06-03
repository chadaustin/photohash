fn main() -> Result<(), vcpkg::Error> {
    if cfg!(windows) {
        vcpkg::find_package("libheif")?;
    }
    Ok(())
}
