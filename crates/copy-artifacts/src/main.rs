use arroyo_storage::StorageProvider;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;

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
        .build()
        .unwrap();
    rt.block_on(async {
        let futures: Vec<_> = srcs
            .iter()
            .map(|src| {
                let src = src.clone();
                let name = src.split('/').last().unwrap();
                let dst = PathBuf::from_str(dst).unwrap().join(name);
                tokio::spawn(async move {
                    let data: Vec<u8> = StorageProvider::get_url(&src)
                        .await
                        .unwrap_or_else(|_| panic!("Failed to download {}", src))
                        .into();

                    tokio::fs::write(&dst, data).await.unwrap();
                    println!("Downloaded {}", src);
                })
            })
            .collect();

        for f in futures {
            f.await.unwrap();
        }
    });
}
