fn irate(values: Vec<f32>) -> Option<f32> {
    let start = values.first()?;
    let end = values.last()?;

    let rate = (end - start) / 60.0;
    if rate >= 0.0 {
        Some(rate)
    } else {
        // this implies there was a counter reset during this window;
        // just drop this sample
        None
    }
}
