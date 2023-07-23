use criterion::*;
use photohash::database::Database;

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
        b.iter(|| db.with_transaction(|_, _| Ok(())).unwrap());
    });

    c.bench_function("get_file", |b| {
        let mut db = Database::open_memory().unwrap();
        b.iter(|| db.get_file("test").unwrap());
    });

    c.bench_function("get_files 10", |b| {
        let mut db = Database::open_memory().unwrap();

        const N: usize = 10;
        let mut paths = Vec::with_capacity(N);
        for i in 0..N {
            paths.push(format!("test{}", i));
        }
        let paths: Vec<&str> = paths.iter().map(String::as_ref).collect();

        b.iter(|| {
            // Criterion is kind of goofy and we don't have a way
            // to consume N iterations. Therefore, amortize.
            db.get_files(&paths).unwrap()
        });
    });
}

criterion_group!(benches, database_benchmark);
criterion_main!(benches);
