//! An updating aggregate gives the result a batch query gives over the rows that remain, whatever the order in which
//! their inserts and retractions arrive, and across a restore.
//!
//! Each case draws rows in a few groups, inserts all of them and retracts some, and shuffles those changes, so that a
//! retraction often comes before its insert. It runs the pipeline as every smoke test does (straight through, with
//! checkpoints, and finishing from a restore) and compares the output with the aggregates computed directly over the
//! rows left. The seed of a case is in its name, so that a failure can be run again.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use arroyo_rpc::config;

use crate::smoke_tests::correctness_run_codegen;

const QUERY: &str = "--pk=g
CREATE TABLE source (
    id INT PRIMARY KEY,
    g TEXT,
    v INT
) WITH (
    connector = 'single_file',
    path = '$input_dir/$name.json',
    format = 'debezium_json',
    type = 'source'
);

CREATE TABLE output (
    g TEXT PRIMARY KEY,
    mn INT,
    mx INT,
    s BIGINT,
    n BIGINT,
    dv BIGINT
) WITH (
    connector = 'single_file',
    path = '$output_path',
    format = 'debezium_json',
    type = 'sink'
);

INSERT INTO output
SELECT g, min(v), max(v), sum(v), count(*), count(distinct v)
FROM source
GROUP BY g;";

const SEEDS: [u64; 6] = [1, 2, 3, 5, 8, 13];
const ROWS: u32 = 80;
const GROUPS: u64 = 4;
const VALUES: u64 = 9;

/// xorshift64*: enough to shuffle, and the same on every machine.
struct Draw(u64);

impl Draw {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

struct Row {
    id: u32,
    g: String,
    v: i64,
}

fn row_json(r: &Row) -> String {
    format!(r#"{{"id":{},"g":"{}","v":{}}}"#, r.id, r.g, r.v)
}

/// The changes of one case, shuffled, and the rows that remain.
fn case(seed: u64) -> (Vec<String>, Vec<Row>) {
    let mut draw = Draw(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let mut changes = Vec::new();
    let mut kept = Vec::new();
    for id in 0..ROWS {
        let row = Row {
            id,
            g: format!("g{}", draw.below(GROUPS)),
            v: draw.below(VALUES) as i64,
        };
        changes.push(format!(
            r#"{{"before":null,"after":{},"op":"c"}}"#,
            row_json(&row)
        ));
        if draw.below(5) < 2 {
            changes.push(format!(
                r#"{{"before":{},"after":null,"op":"d"}}"#,
                row_json(&row)
            ));
        } else {
            kept.push(row);
        }
    }
    for i in (1..changes.len()).rev() {
        changes.swap(i, draw.below(i as u64 + 1) as usize);
    }
    (changes, kept)
}

/// What the query gives over `rows` in batch, as the sink writes it.
fn expected(rows: &[Row]) -> Vec<String> {
    let mut groups: BTreeMap<&str, Vec<i64>> = BTreeMap::new();
    for r in rows {
        groups.entry(&r.g).or_default().push(r.v);
    }
    groups
        .into_iter()
        .map(|(g, vs)| {
            let distinct = vs.iter().collect::<BTreeSet<_>>().len();
            format!(
                r#"{{"before":null,"after":{{"g":"{g}","mn":{},"mx":{},"s":{},"n":{},"dv":{distinct}}},"op":"c"}}"#,
                vs.iter().min().unwrap(),
                vs.iter().max().unwrap(),
                vs.iter().sum::<i64>(),
                vs.len(),
            )
        })
        .collect()
}

fn testing_dir() -> PathBuf {
    let here = std::env::current_dir().unwrap();
    if here.ends_with("arroyo-sql-testing") {
        here
    } else {
        here.join("arroyo-sql-testing")
    }
}

#[test]
fn aggregates_ignore_change_order() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_cases());
}

async fn run_cases() {
    // As every smoke test runs: small source batches, so that changes spread over many batches and checkpoints.
    config::config();
    config::update(|c| c.pipeline.worker.source_batch_size = 32);
    let dir = testing_dir();
    for seed in SEEDS {
        let name = format!("generated_retraction_order_{seed}");
        let (changes, kept) = case(seed);
        let input = dir.join("inputs").join(format!("{name}.json"));
        let golden = dir.join("golden_outputs").join(format!("{name}.json"));
        std::fs::write(&input, changes.join("\n") + "\n").unwrap();
        std::fs::write(&golden, expected(&kept).join("\n") + "\n").unwrap();

        let query = QUERY.replace("$name", &name);
        correctness_run_codegen(&name, query, Some(&["g"]), 20)
            .await
            .unwrap_or_else(|e| panic!("{name}: {e:?}"));

        // Kept on a failure, for the case to be run again.
        std::fs::remove_file(&input).unwrap();
        std::fs::remove_file(&golden).unwrap();
    }
}
