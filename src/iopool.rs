use futures::channel::oneshot;
use std::io;
use std::path::PathBuf;
use std::sync::OnceLock;

// https://docs.rs/tokio/latest/tokio/index.html#cpu-bound-tasks-and-blocking-code
static IO_POOL: OnceLock<rayon::ThreadPool> = OnceLock::new();
const IO_POOL_CONCURRENCY: usize = 4;

fn get_io_pool() -> &'static rayon::ThreadPool {
    IO_POOL.get_or_init(|| {
        rayon::ThreadPoolBuilder::new()
            .num_threads(IO_POOL_CONCURRENCY)
            .thread_name(|i| format!("io{i}"))
            .build()
            .unwrap()
    })
}

pub async fn run_in_io_pool<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = oneshot::channel();

    get_io_pool().spawn(move || {
        _ = tx.send(f());
    });

    rx.await.unwrap()
}

pub async fn run_in_io_pool_local<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send,
    T: Send,
{
    tokio::task::block_in_place(move || get_io_pool().install(f))
}

pub async fn get_file_contents(path: PathBuf) -> io::Result<Vec<u8>> {
    run_in_io_pool(move || std::fs::read(path)).await
}
