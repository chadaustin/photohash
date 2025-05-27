use tracing::trace;

pub fn keep_awake(reason: &str) -> Option<keepawake::KeepAwake> {
    match keepawake::Builder::default()
        .display(false)
        .idle(true)
        .sleep(true)
        .reason(reason)
        .app_name("imagehash")
        .app_reverse_domain("me.chadaustin.imagehash")
        .create()
    {
        Ok(awake) => Some(awake),
        Err(e) => {
            trace!("WARNING: keepawake failed, ignoring: {}", e);
            None
        }
    }
}
