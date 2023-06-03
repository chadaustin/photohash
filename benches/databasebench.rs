use criterion::*;
use imagehash::database::Database;
use std::path::PathBuf;

fn database_benchmark(c: &mut Criterion) {
    //let mut c = Criterion::default().measurement_time(std::time::Duration::from_secs(60));
    c.bench_function("get_file", |b| {
        let db = Database::open_memory().unwrap();
        let path = PathBuf::from("test");
        b.iter(|| db.get_file(&path).unwrap());
    });
}

criterion_group!(benches, database_benchmark);
criterion_main!(benches);
