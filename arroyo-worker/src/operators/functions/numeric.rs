use petgraph::matrix_graph::Zero;

pub fn is_nanvl(v: f64, default: f64) -> f64 {
    if v.is_zero() {
        default
    } else {
        v
    }
}
