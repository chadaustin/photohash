use crate::model::ContentMetadata;
use crate::model::ExtraHashes;
use crate::model::Hash32;
use crate::model::IMPath;
use crate::model::ImageMetadata;

#[derive(Debug)]
pub struct ProcessFileResult {
    pub path: IMPath,
    pub blake3_computed: bool,
    pub content_metadata: ContentMetadata,
    pub image_metadata_computed: bool,
    pub image_metadata: Option<ImageMetadata>,
    /// Only set if the indexing operation requests extra hashes.
    pub extra_hashes: Option<ExtraHashes>,
}

impl ProcessFileResult {
    pub fn blockhash256(&self) -> Option<&Hash32> {
        self.image_metadata
            .as_ref()
            .and_then(|im| im.blockhash256.as_ref())
    }

    pub fn jpegrothash(&self) -> Option<&Hash32> {
        self.image_metadata
            .as_ref()
            .and_then(|im| im.jpegrothash.as_ref())
    }
}
