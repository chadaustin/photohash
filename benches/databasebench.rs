use criterion::*;
use std::path::PathBuf;
use database::Database;

fn database_benchmark(c: &mut Criterion) {
    c.bench_function("get_file", |b| {
        let db = Database::open_memory().unwrap();
        let path = PathBuf::from("test");
        b.iter(|| db.get_file(&path).unwrap());
    });
}

criterion_group!(benches, database_benchmark);
criterion_main!(benches);
