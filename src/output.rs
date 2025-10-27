use crate::index::ProcessFileResult;
use hex::ToHex;
use serde::ser::SerializeSeq;
use serde::Serialize;
use serde::Serializer;
use superconsole::SuperConsole;

pub trait OutputMode {
    fn start(&mut self) -> anyhow::Result<Box<dyn OutputSequence<'_> + '_>>;
}

pub trait OutputSequence<'a> {
    fn file(&mut self, pfr: &ProcessFileResult) -> anyhow::Result<()>;
    fn end(self: Box<Self>) -> anyhow::Result<()>;
}

pub struct StdoutMode;

impl OutputMode for StdoutMode {
    fn start(&mut self) -> anyhow::Result<Box<dyn OutputSequence<'_>>> {
        // Does not allocate because StdoutMode is ZST.
        Ok(Box::new(StdoutSequence))
    }
}

struct StdoutSequence;

impl OutputSequence<'_> for StdoutSequence {
    fn file(&mut self, pfr: &ProcessFileResult) -> anyhow::Result<()> {
        if pfr.blake3_computed || pfr.image_metadata_computed {
            let content_metadata = &pfr.content_metadata;
            println!(
                "{} {:>8}K {}",
                content_metadata.blake3.encode_hex::<String>(),
                content_metadata.file_info.size / 1024,
                &dunce::simplified(pfr.path.as_ref()).display(),
            );
        }
        Ok(())
    }

    fn end(self: Box<Self>) -> anyhow::Result<()> {
        Ok(())
    }
}

pub struct TtyMode(Option<SuperConsole>);
struct TtySequence(SuperConsole);

impl OutputMode for TtyMode {
    fn start(&mut self) -> anyhow::Result<Box<dyn OutputSequence<'_>>> {
        Ok(Box::new(TtySequence(
            self.0.take().expect("cannot start TtyMode twice"),
        )))
    }
}

impl OutputSequence<'_> for TtySequence {
    fn file(&mut self, pfr: &ProcessFileResult) -> anyhow::Result<()> {
        let sc = &mut self.0;
        let content_metadata = &pfr.content_metadata;
        use superconsole::components::Blank;
        use superconsole::Line;
        use superconsole::Lines;
        if pfr.blake3_computed || pfr.image_metadata_computed {
            sc.emit(Lines(vec![Line::unstyled(&format!(
                "{} {:>8}K {}",
                content_metadata.blake3.encode_hex::<String>(),
                content_metadata.file_info.size / 1024,
                &dunce::simplified(pfr.path.as_ref()).display(),
            ))?]));
            sc.render(&Blank)?;
        }
        Ok(())
    }

    fn end(self: Box<Self>) -> anyhow::Result<()> {
        self.0.finalize(&superconsole::components::Blank)?;
        Ok(())
    }
}

pub struct JsonMode {
    serializer: serde_json::Serializer<std::io::Stdout>,
}
struct JsonSequence<'a>(
    Option<<&'a mut serde_json::Serializer<std::io::Stdout> as serde::Serializer>::SerializeSeq>,
);

impl JsonMode {
    fn new() -> Self {
        let serializer = serde_json::Serializer::new(std::io::stdout());
        Self { serializer }
    }
}

impl OutputMode for JsonMode {
    fn start(&mut self) -> anyhow::Result<Box<dyn OutputSequence<'_> + '_>> {
        Ok(Box::new(JsonSequence(Some(
            self.serializer.serialize_seq(None)?,
        ))))
    }
}

impl OutputSequence<'_> for JsonSequence<'_> {
    fn file(&mut self, pfr: &ProcessFileResult) -> anyhow::Result<()> {
        let content_metadata = &pfr.content_metadata;
        self.0.as_mut().unwrap().serialize_element(&JsonRecord {
            path: pfr.path.clone(),
            size: content_metadata.file_info.size,
            blake3: hex::encode(content_metadata.blake3),
            extra_hashes: pfr.extra_hashes.as_ref().map(|eh| JsonExtraHashes {
                md5: eh.md5.map(hex::encode),
                sha1: eh.sha1.map(hex::encode),
                sha256: eh.sha256.map(hex::encode),
            }),
        })?;
        Ok(())
    }

    fn end(mut self: Box<Self>) -> anyhow::Result<()> {
        self.0.take().unwrap().end()?;
        Ok(())
    }
}

#[derive(Serialize)]
struct JsonRecord {
    path: String,
    size: u64,
    blake3: String,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    extra_hashes: Option<JsonExtraHashes>,
}

#[derive(Serialize)]
struct JsonExtraHashes {
    #[serde(skip_serializing_if = "Option::is_none")]
    md5: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sha1: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sha256: Option<String>,
}

pub fn select_output_mode(json: bool) -> Box<dyn OutputMode> {
    let output: Box<dyn OutputMode> = if json {
        Box::new(JsonMode::new())
    } else if let Some(sc) = SuperConsole::new() {
        Box::new(TtyMode(Some(sc)))
    } else {
        Box::new(StdoutMode)
    };
    output
}
