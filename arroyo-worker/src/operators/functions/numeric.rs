pub fn nanvl(v: f64, default: f64) -> f64 {
    if v.is_nan() {
        default
    } else {
        v
    }
}

pub fn is_zero(v: f64) -> bool {
    return v == 0f64;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nanvl() {
        assert_eq!(nanvl(1f64, 0f64), 1f64);
        assert_eq!(nanvl(f64::NAN, 0f64), 0f64);
    }
    #[test]
    fn test_is_zero() {
        assert_eq!(is_zero(1f64), false);
        assert_eq!(is_zero(0f64), true);
    }
}
