use arroyo_udf_host::LocalUdf;

pub fn get_udfs() -> Vec<LocalUdf> {
    vec![
        double_negative::__local(),
        async_double_negative::__local(),
        my_median2::__local(),
        none_udf::__local(),
        max_product::__local(),
    ]
}

mod double_negative {
    use arroyo_udf_macros::local_udf;

    #[local_udf]
    fn double_negative(x: u64) -> i64 {
        -2 * (x as i64)
    }
}

mod async_double_negative {
    use arroyo_udf_macros::local_udf;

    #[local_udf(ordered)]
    async fn async_double_negative(x: u64) -> i64 {
        tokio::time::sleep(std::time::Duration::from_millis(x % 100)).await;
        -2 * (x as i64)
    }
}

mod my_median2 {
    use arroyo_udf_macros::local_udf;

    #[local_udf]
    fn my_median(mut args: Vec<u64>) -> f64 {
        args.sort();
        let mid = args.len() / 2;
        if args.len() % 2 == 0 {
            (args[mid] + args[mid - 1]) as f64 / 2.0
        } else {
            args[mid] as f64
        }
    }
}

mod none_udf {
    use arroyo_udf_macros::local_udf;

    #[local_udf]
    fn none_udf(_args: Vec<u64>) -> Option<f64> {
        None
    }
}
mod max_product {
    use arroyo_udf_macros::local_udf;

    #[local_udf]
    fn max_product(first_arg: Vec<u64>, second_arg: Vec<u64>) -> u64 {
        let pairs = first_arg.iter().zip(second_arg.iter());
        pairs.map(|(x, y)| x * y).max().unwrap()
    }
}
