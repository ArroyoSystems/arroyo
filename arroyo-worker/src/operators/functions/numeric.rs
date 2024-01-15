pub fn nanvl(v: f64, default: f64) -> f64 {
    if v.is_nan() {
        default
    } else {
        v
    }
}
