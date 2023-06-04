use criterion::*;
use imagehash::database::Database;

fn database_benchmark(c: &mut Criterion) {
    //let mut c = Criterion::default().measurement_time(std::time::Duration::from_secs(60));

    c.bench_function("rusqlite_transaction", |b| {
        let mut db = Database::open_memory().unwrap();
        b.iter(|| db.rusqlite_transaction().unwrap());
    });

    c.bench_function("cached_immediate_transaction", |b| {
        let db = Database::open_memory().unwrap();
        b.iter(|| db.cached_immediate_transaction().unwrap());
    });

    c.bench_function("cached_deferred_transaction", |b| {
        let db = Database::open_memory().unwrap();
        b.iter(|| db.cached_deferred_transaction().unwrap());
    });

    c.bench_function("with_transaction", |b| {
        let mut db = Database::open_memory().unwrap();
        b.iter(|| db.with_transaction().unwrap());
    });

    c.bench_function("get_file", |b| {
        let mut db = Database::open_memory().unwrap();
        b.iter(|| db.get_file("test").unwrap());
    });
}

criterion_group!(benches, database_benchmark);
criterion_main!(benches);
