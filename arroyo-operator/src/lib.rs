use crate::operator::{ArrowOperator, ArrowOperatorConstructor};
use crate::operators::console::ConsoleSink;
use crate::operators::impulse::ImpulseSource;
use crate::operators::projection::ProjectionOperator;
use arroyo_types::{CheckpointBarrier, Watermark};
use serde_json::Value;
use std::time::SystemTime;

mod inq_reader;
pub mod operator;
mod operators;


pub enum ControlOutcome {
    Continue,
    Stop,
    Finish,
}

pub enum SourceFinishType {
    // stop messages should be propagated through the dataflow
    Graceful,
    // shuts down the operator immediately, triggering immediate shut-downs across the dataflow
    Immediate,
    // EndOfData messages are propagated, causing MAX_WATERMARK and flushing all timers
    Final,
}

pub struct WatermarkHolder {
    // This is the last watermark with an actual value; this helps us keep track of the watermark we're at even
    // if we're currently idle
    last_present_watermark: Option<SystemTime>,
    cur_watermark: Option<Watermark>,
    watermarks: Vec<Option<Watermark>>,
}

impl WatermarkHolder {
    pub fn new(watermarks: Vec<Option<Watermark>>) -> Self {
        let mut s = Self {
            last_present_watermark: None,
            cur_watermark: None,
            watermarks,
        };
        s.update_watermark();

        s
    }

    pub fn watermark(&self) -> Option<Watermark> {
        self.cur_watermark
    }

    pub fn last_present_watermark(&self) -> Option<SystemTime> {
        self.last_present_watermark
    }

    fn update_watermark(&mut self) {
        self.cur_watermark = self
            .watermarks
            .iter()
            .fold(Some(Watermark::Idle), |current, next| {
                match (current?, (*next)?) {
                    (Watermark::EventTime(cur), Watermark::EventTime(next)) => {
                        Some(Watermark::EventTime(cur.min(next)))
                    }
                    (Watermark::Idle, Watermark::EventTime(t))
                    | (Watermark::EventTime(t), Watermark::Idle) => Some(Watermark::EventTime(t)),
                    (Watermark::Idle, Watermark::Idle) => Some(Watermark::Idle),
                }
            });

        if let Some(Watermark::EventTime(t)) = self.cur_watermark {
            self.last_present_watermark = Some(t);
        }
    }

    pub fn set(&mut self, idx: usize, watermark: Watermark) -> Option<Option<Watermark>> {
        *(self.watermarks.get_mut(idx)?) = Some(watermark);
        self.update_watermark();
        Some(self.cur_watermark)
    }
}

pub struct CheckpointCounter {
    inputs: Vec<Option<u32>>,
    counter: Option<usize>,
}

impl CheckpointCounter {
    pub fn new(size: usize) -> CheckpointCounter {
        CheckpointCounter {
            inputs: vec![None; size],
            counter: None,
        }
    }

    pub fn is_blocked(&self, idx: usize) -> bool {
        self.inputs[idx].is_some()
    }

    pub fn all_clear(&self) -> bool {
        self.inputs.iter().all(|x| x.is_none())
    }

    pub fn mark(&mut self, idx: usize, checkpoint: &CheckpointBarrier) -> bool {
        assert!(self.inputs[idx].is_none());

        if self.inputs.len() == 1 {
            return true;
        }

        self.inputs[idx] = Some(checkpoint.epoch);
        self.counter = match self.counter {
            None => Some(self.inputs.len() - 1),
            Some(1) => {
                for v in self.inputs.iter_mut() {
                    *v = None;
                }
                None
            }
            Some(n) => Some(n - 1),
        };

        self.counter.is_none()
    }
}

pub fn construct_operator(operator: &str, config: Vec<u8>) -> Box<dyn ArrowOperator> {
    let mut buf = config.as_slice();
    match operator {
        "ImpulseSource" => ImpulseSource::from_config(prost::Message::decode(&mut buf).unwrap()),
        "Projection" => ProjectionOperator::from_config(prost::Message::decode(&mut buf).unwrap()),
        "ConsoleSink" => ConsoleSink::from_config(prost::Message::decode(&mut buf).unwrap()),
        _ => {
            panic!("unknown operator {}", operator);
        }
    }
}
