use crate::nexmark::operator::{GeneratorConfig, NexmarkConfig, NexmarkGenerator};
use rand::{rngs::SmallRng, SeedableRng};
use std::{borrow::BorrowMut, time::SystemTime};

#[tokio::test]
async fn test_nexmark() {
    let mut generator = NexmarkGenerator::new(10000.0, None);
    let mut random = SmallRng::seed_from_u64(1);
    for _ in 0..10000 {
        generator.next_event(random.borrow_mut());
    }
}

#[test]
fn test_auction_generation() {
    let mut random = SmallRng::seed_from_u64(1);
    let generator = NexmarkGenerator::new(10000.0, None);
    let _result = generator
        .generator_config
        .next_base0_auction_id(7, &mut random);
}

#[test]
fn test_exit() {
    let mut random = SmallRng::seed_from_u64(1);
    let config = GeneratorConfig::new(
        NexmarkConfig::new(10000.0, Some(100), 1),
        SystemTime::now(),
        1,
        Some(100),
        1,
    );
    let subconfig = config.split(8)[4].clone();
    let mut generator = NexmarkGenerator::from_config(&subconfig, 0);
    while generator.has_next() {
        generator.next_event(&mut random);
    }
}
