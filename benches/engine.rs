use criterion::{black_box, criterion_group, criterion_main, Criterion};
use fetish::engine::Db;

fn benchmark_engine(c: &mut Criterion) {
    let db = Db::new();

    c.bench_function("SET", |b| {
        b.iter(|| {
            db.s(black_box("name"), black_box("Himanshu"));
        });
    });

    db.s("name", "Himanshu");
    c.bench_function("GET", |b| {
        b.iter(|| {
            db.g(black_box("name"));
        });
    });

    c.bench_function("DELETE", |b| {
        b.iter(|| {
            db.d(black_box("name"));
            db.s(black_box("name"), black_box("Himanshu"));
        });
    });

    c.bench_function("SET_EX", |b| {
        b.iter(|| {
            let _ = db.set(
                black_box("temp_key"),
                black_box("temp_val"),
                black_box(Some(60)),
            );
        });
    });

    c.bench_function("TTL", |b| {
        b.iter(|| {
            db.ttl(black_box("temp_key"));
        });
    });
}

criterion_group!(benches, benchmark_engine);
criterion_main!(benches);
