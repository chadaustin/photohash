use photohash::hash::compute_blake3;
use photohash::hash::compute_extra_hashes;
use photohash::hash::compute_image_hashes;

#[tokio::test]
async fn moonlight() -> anyhow::Result<()> {
    let hashes = compute_image_hashes("tests/images/Moonlight.heic").await?;
    assert_eq!(Some((4032, 3024)), hashes.dimensions);
    assert_eq!(None, hashes.jpegrothash);
    assert_eq!(
        Some([
            1, 224, 7, 240, 15, 252, 31, 252, 7, 240, 7, 248, 15, 248, 15, 240, 127, 255, 63, 248,
            7, 224, 0, 0, 15, 252, 7, 248, 15, 248, 9, 208
        ]),
        hashes.blockhash256,
    );
    Ok(())
}

#[tokio::test]
async fn saturn_v() -> anyhow::Result<()> {
    let hashes = compute_image_hashes("tests/images/SaturnV.jpeg").await?;
    assert_eq!(Some((4032, 3024)), hashes.dimensions);
    assert_eq!(
        Some([
            9, 211, 39, 115, 136, 11, 49, 25, 193, 244, 22, 178, 53, 74, 223, 51, 140, 27, 193, 4,
            149, 85, 184, 88, 8, 250, 51, 48, 18, 65, 144, 79
        ]),
        hashes.jpegrothash,
    );
    assert_eq!(
        Some([
            249, 0, 224, 41, 225, 57, 230, 251, 224, 145, 225, 208, 227, 240, 219, 184, 206, 24,
            199, 24, 207, 25, 199, 153, 243, 241, 243, 243, 1, 244, 192, 4
        ]),
        hashes.blockhash256,
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn blake3() -> anyhow::Result<()> {
    let b3 = compute_blake3("tests/images/Moonlight.heic".into()).await?;
    assert_eq!(
        "d8828886771faa4da22c36c352acdbf0988f780b457dd8525499a3f2153a25d5",
        hex::encode(b3)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn extra_hashes() -> anyhow::Result<()> {
    let extra_hashes = compute_extra_hashes("tests/images/Moonlight.heic".into()).await?;
    assert_eq!(
        Some("e5dae7611472d7102fc3a05a16152247"),
        extra_hashes.md5.map(hex::encode).as_deref()
    );
    assert_eq!(
        Some("3260706646db71bb48cc4165e46410fde3e98a44"),
        extra_hashes.sha1.map(hex::encode).as_deref()
    );
    assert_eq!(
        Some("d5b1055e3a5f5fc68d5a1ae706639f3cd1bd34349e6db7003e48cb11f755d3e8"),
        extra_hashes.sha256.map(hex::encode).as_deref()
    );
    Ok(())
}
