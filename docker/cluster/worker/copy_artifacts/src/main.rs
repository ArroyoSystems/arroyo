use std::{env};
use std::path::PathBuf;
use std::str::FromStr;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use regex::Regex;

async fn get_object(path: &str) -> Vec<u8> {
    let path_regex = Regex::new(r"file://(?P<path>/.*)").unwrap();
    let s3_regex = Regex::new(
        r"s3://(?P<bucket>[^/\.]*)\.s3-(?P<region>[^\.]*).amazonaws.com/(?P<path>.*)"
    )
        .unwrap();

    if let Some(m) = path_regex.captures(path) {
        return tokio::fs::read(&m.name("path").unwrap().as_str()).await.unwrap();
    }
    if let Some(m) = s3_regex.captures(path) {
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(m.name("bucket").unwrap().as_str())
            .with_region(m.name("region").unwrap().as_str())
            .build()
            .unwrap();
        return store
            .get(&m.name("path").unwrap().as_str().try_into().unwrap())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .into();
    }

    panic!("Unhandled URL: {}", path);
}

fn main() {
    let args: Vec<_> = env::args().collect();

    if args.len() < 3 {
        panic!("Usage ./copy_artifacts src.. dst");
    }

    let srcs = &args[1..args.len() - 1];
    let dst = &args[args.len() - 1];

    // TODO: stream the data instead of materializing it all in memory
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build().unwrap();
    rt.block_on(async {
        let futures: Vec<_> = srcs.iter().map(|src| {
            let src = src.clone();
            let name = src.split("/").last().unwrap();
            let dst = PathBuf::from_str(&dst).unwrap()
                .join(name);
            tokio::spawn(async move {
                let data = get_object(&src).await;

                tokio::fs::write(&dst, data).await.unwrap();
                println!("Downloaded {}", src);
            })
        }).collect();

        for f in futures {
            f.await.unwrap();
        }
    });
}
