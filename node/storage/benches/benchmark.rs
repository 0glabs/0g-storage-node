use std::{
    fs,
    path::Path,
    sync::{Arc, RwLock},
};

use criterion::{criterion_group, criterion_main, Criterion};
use rand::{random, Rng};
use shared_types::{ChunkArray, Transaction, CHUNK_SIZE};
use storage::{
    log_store::{
        log_manager::{sub_merkle_tree, tx_subtree_root_list_padded, LogConfig},
        Store,
    },
    LogManager,
};

fn write_performance(c: &mut Criterion) {
    if Path::new("db_write").exists() {
        fs::remove_dir_all("db_write").unwrap();
    }

    let store: Arc<RwLock<dyn Store>> = Arc::new(RwLock::new(
        LogManager::rocksdb(LogConfig::default(), "db_write")
            .map_err(|e| format!("Unable to start RocksDB store: {:?}", e))
            .unwrap(),
    ));

    let chunk_count = 2048;
    let data_size = CHUNK_SIZE * chunk_count;
    let (chunk_size_padded, _) = shared_types::compute_padded_chunk_size(data_size);

    let mut data_vec = vec![];
    let mut merkel_nodes_vec = vec![];
    let mut first_tree_size_vec = vec![];
    let mut merkel_root_vec = vec![];

    for _ in 0..5000 {
        let mut data = vec![0; data_size];
        for item in data.iter_mut().take(data_size) {
            *item = random();
        }

        let merkel_nodes = tx_subtree_root_list_padded(&data[..]);
        let first_tree_size = 1 << (merkel_nodes[0].0 - 1);

        let merkle = sub_merkle_tree(&data).unwrap();
        let merkel_root = merkle.root().into();

        data_vec.push(data);
        merkel_nodes_vec.push(merkel_nodes);
        first_tree_size_vec.push(first_tree_size);
        merkel_root_vec.push(merkel_root);
    }

    let mut seq = 0;
    let mut offset = 1;

    let mut group = c.benchmark_group("write performance");
    group.sample_size(10);
    group.bench_function("write performance", move |b| {
        b.iter(|| {
            let first_tree_size = first_tree_size_vec[seq as usize];
            let data = data_vec[seq as usize].clone();
            let merkel_root = merkel_root_vec[seq as usize];
            let merkel_nodes = merkel_nodes_vec[seq as usize].clone();

            let start_offset = if offset % first_tree_size == 0 {
                offset
            } else {
                (offset / first_tree_size + 1) * first_tree_size
            };

            let chunks = ChunkArray {
                data: data.to_vec(),
                start_index: 0,
            };

            let tx = Transaction {
                stream_ids: vec![],
                size: data_size as u64,
                data_merkle_root: merkel_root,
                seq,
                data: vec![],
                start_entry_index: start_offset,
                merkle_nodes: merkel_nodes,
            };

            store.write().unwrap().put_tx(tx).unwrap();
            store
                .write()
                .unwrap()
                .put_chunks(seq, chunks.clone())
                .unwrap();
            store.write().unwrap().finalize_tx(seq).unwrap();

            offset = start_offset + chunk_size_padded as u64;
            seq += 1;
        })
    });
}

fn read_performance(c: &mut Criterion) {
    if Path::new("db_read").exists() {
        fs::remove_dir_all("db_read").unwrap();
    }

    let store: Arc<RwLock<dyn Store>> = Arc::new(RwLock::new(
        LogManager::rocksdb(LogConfig::default(), "db_read")
            .map_err(|e| format!("Unable to start RocksDB store: {:?}", e))
            .unwrap(),
    ));

    let tx_size = 1000;
    let chunk_count = 4096;
    let data_size = CHUNK_SIZE * chunk_count;
    let mut offset = 1;
    let (chunk_size_padded, _) = shared_types::compute_padded_chunk_size(data_size);

    for seq in 0..tx_size {
        let mut data = vec![0; data_size];
        for item in data.iter_mut().take(data_size) {
            *item = random();
        }

        let merkel_nodes = tx_subtree_root_list_padded(&data[..]);
        let first_tree_size = 1 << (merkel_nodes[0].0 - 1);

        let merkle = sub_merkle_tree(&data).unwrap();
        let merkel_root = merkle.root().into();

        let start_offset = if offset % first_tree_size == 0 {
            offset
        } else {
            (offset / first_tree_size + 1) * first_tree_size
        };

        let chunks = ChunkArray {
            data: data.to_vec(),
            start_index: 0,
        };

        let tx = Transaction {
            stream_ids: vec![],
            size: data_size as u64,
            data_merkle_root: merkel_root,
            seq,
            data: vec![],
            start_entry_index: start_offset,
            merkle_nodes: merkel_nodes,
        };

        store.write().unwrap().put_tx(tx).unwrap();
        store
            .write()
            .unwrap()
            .put_chunks(seq, chunks.clone())
            .unwrap();
        store.write().unwrap().finalize_tx(seq).unwrap();

        offset = start_offset + chunk_size_padded as u64;
    }

    let mut rng = rand::thread_rng();

    let mut group = c.benchmark_group("read performance");
    group.sample_size(100);
    group.bench_function("read performance", move |b| {
        b.iter(|| {
            let tx_seq = rng.gen_range(0..tx_size);
            let index_start = rng.gen_range(0..=chunk_count);
            let index_end = rng.gen_range((index_start + 1)..=(chunk_count + 1));

            store
                .read()
                .unwrap()
                .get_chunks_with_proof_by_tx_and_index_range(tx_seq, index_start, index_end, None)
                .unwrap();
        })
    });
}

criterion_group!(benches, write_performance, read_performance);
criterion_main!(benches);
