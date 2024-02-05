pub fn nanvl(v: f64, default: f64) -> f64 {
    if v.is_nan() {
        default
    } else {
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nanvl() {
        assert_eq!(nanvl(1f64, 0f64), 1f64);
        assert_eq!(nanvl(f64::NAN, 0f64), 0f64);
    }
}
