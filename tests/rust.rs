use std::path::PathBuf;

#[test]
fn foo_starts_with_foo() {
    assert!(PathBuf::from("foo").starts_with("foo"));
}

#[test]
fn canonical_starts_with() {
    let cwd = PathBuf::from(".").canonicalize().unwrap();
    assert!(cwd.starts_with(&cwd));
}
