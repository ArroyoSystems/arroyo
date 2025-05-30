use crate::nexmark::{auction_fields, bid_fields, person_fields, NexmarkTable};
use arrow::array::{
    Int64Builder, RecordBatch, StringBuilder, StructBuilder, TimestampNanosecondBuilder,
};
use arroyo_formats::should_flush;
use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::grpc::rpc::{StopMode, TableConfig};
use arroyo_rpc::ControlMessage;
use arroyo_types::{to_millis, to_nanos};
use async_trait::async_trait;
use bincode::{Decode, Encode};
use rand::prelude::IndexedRandom;
use rand::{distr::Alphanumeric, distr::SampleString, rngs::SmallRng, Rng, SeedableRng};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::{sync::mpsc::error::TryRecvError, time::sleep};
use tracing::debug;
use tracing::{info, log::warn};

const HOT_AUCTION_RATIO: u64 = 100;
const HOT_BIDDER_RATIO: u64 = 100;
const HOT_CHANNELS_RATIO: u64 = 2;
const CHANNELS_NUMBER: u64 = 10_000;
const HOT_SELLER_RATIO: u64 = 100;
const PERSON_ID_LEAD: u64 = 10;
const AUCTION_ID_LEAD: u64 = 10;
const FIRST_AUCTION_ID: u64 = 1000;
const FIRST_PERSON_ID: u64 = 1000;
const FIRST_CATEGORY_ID: u64 = 10;
const NUM_CATEGORIES: u64 = 5;
const MIN_STRING_LENGTH: u32 = 3;

const FIRST_NAMES: [&str; 11] = [
    "Peter", "Paul", "Luke", "John", "Saul", "Vicky", "Kate", "Julie", "Sarah", "Deiter", "Walter",
];
const LAST_NAMES: [&str; 9] = [
    "Shultz", "Abrams", "Spencer", "White", "Bartels", "Walton", "Smith", "Jones", "Noris",
];
const US_CITIES: [&str; 10] = [
    "Phoenix",
    "Los Angeles",
    "San Francisco",
    "Boise",
    "Portland",
    "Bend",
    "Redmond",
    "Seattle",
    "Kent",
    "Cheyenne",
];
const US_STATES: [&str; 6] = ["AZ", "CA", "ID", "OR", "WA", "WY"];

const HOT_CHANNELS: [&str; 4] = ["Google", "Facebook", "Baidu", "Apple"];

const HOT_URLS: [&str; 4] = [
    "https://www.nexmark.com/abo/eoci/cidro/item.htm?query=1",
    "https://www.nexmark.com/eoax/oad/cidro/item.htm?query=1",
    "https://www.nexmark.com/abo/jack/cidro/item.htm?query=1",
    "https://www.nexmark.com/abo/micah/cidro/item.htm?query=1",
];

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Person {
    pub id: i64,
    pub name: String,
    pub email_address: String,
    pub credit_card: String,
    pub city: String,
    pub state: String,
    pub datetime: SystemTime,
    pub extra: String,
}

trait ArrowWriter {
    fn write_into(self, b: &mut StructBuilder);
}

fn write_i64(builder: &mut StructBuilder, idx: usize, v: Option<i64>) {
    builder
        .field_builder::<Int64Builder>(idx)
        .unwrap()
        .append_option(v);
}
fn write_string(builder: &mut StructBuilder, idx: usize, v: Option<&str>) {
    builder
        .field_builder::<StringBuilder>(idx)
        .unwrap()
        .append_option(v);
}
fn write_timestamp(builder: &mut StructBuilder, idx: usize, v: Option<SystemTime>) {
    builder
        .field_builder::<TimestampNanosecondBuilder>(idx)
        .unwrap()
        .append_option(v.map(|t| to_nanos(t) as i64));
}

impl ArrowWriter for Option<&Person> {
    fn write_into(self, b: &mut StructBuilder) {
        write_i64(b, 0, self.map(|p| p.id));
        write_string(b, 1, self.map(|p| p.name.as_str()));
        write_string(b, 2, self.map(|p| p.email_address.as_str()));
        write_string(b, 3, self.map(|p| p.credit_card.as_str()));
        write_string(b, 4, self.map(|p| p.city.as_str()));
        write_string(b, 5, self.map(|p| p.state.as_str()));
        write_timestamp(b, 6, self.map(|p| p.datetime));
        write_string(b, 7, self.map(|p| p.extra.as_str()));
        b.append(self.is_some());
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Auction {
    pub id: i64,
    pub item_name: String,
    pub description: String,
    pub initial_bid: i64,
    pub reserve: i64,
    pub datetime: SystemTime,
    pub expires: SystemTime,
    pub seller: i64,
    pub category: i64,
    pub extra: String,
}

impl ArrowWriter for Option<&Auction> {
    fn write_into(self, b: &mut StructBuilder) {
        write_i64(b, 0, self.map(|p| p.id));
        write_string(b, 1, self.map(|p| p.description.as_str()));
        write_string(b, 2, self.map(|p| p.item_name.as_str()));
        write_i64(b, 3, self.map(|p| p.initial_bid));
        write_i64(b, 4, self.map(|p| p.reserve));
        write_timestamp(b, 5, self.map(|p| p.datetime));
        write_timestamp(b, 6, self.map(|p| p.expires));
        write_i64(b, 7, self.map(|p| p.seller));
        write_i64(b, 8, self.map(|p| p.category));
        write_string(b, 9, self.map(|p| p.extra.as_str()));
        b.append(self.is_some());
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Bid {
    pub auction: i64,
    pub bidder: i64,
    pub price: i64,
    pub channel: String,
    pub url: String,
    pub datetime: SystemTime,
    pub extra: String,
}

impl ArrowWriter for Option<&Bid> {
    fn write_into(self, b: &mut StructBuilder) {
        write_i64(b, 0, self.map(|p| p.auction));
        write_i64(b, 1, self.map(|p| p.bidder));
        write_i64(b, 2, self.map(|p| p.price));
        write_string(b, 3, self.map(|p| p.channel.as_str()));
        write_string(b, 4, self.map(|p| p.url.as_str()));
        write_timestamp(b, 5, self.map(|p| p.datetime));
        write_string(b, 6, self.map(|p| p.extra.as_str()));
        b.append(self.is_some());
    }
}

pub struct NexmarkSourceFunc {
    first_event_rate: f64,
    num_events: Option<u64>,
    state: Option<NexmarkSourceState>,
}

#[derive(Debug, Encode, Decode, Clone, PartialEq)]
struct NexmarkSourceState {
    config: GeneratorConfig,
    event_count: usize,
}

impl NexmarkSourceFunc {
    #[allow(unused)]
    pub fn new(first_event_rate: u64, num_events: Option<u64>) -> Self {
        Self {
            first_event_rate: first_event_rate as f64,
            num_events,
            state: None,
        }
    }

    pub fn from_config(table: &NexmarkTable) -> Self {
        Self {
            first_event_rate: table.event_rate,
            num_events: table
                .runtime
                .map(|time| (table.event_rate * time).floor() as u64),
            state: None,
        }
    }
}

#[async_trait]
impl SourceOperator for NexmarkSourceFunc {
    fn name(&self) -> String {
        "nexmark".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        arroyo_state::global_table_config("s", "nexmark source state")
    }

    async fn on_start(&mut self, ctx: &mut SourceContext) {
        // load state
        self.state = Some({
            let ss = ctx
                .table_manager
                .get_global_keyed_state::<usize, NexmarkSourceState>("s")
                .await
                .expect("should be able to read state");
            let saved_states = ss.get_all().len();
            if saved_states != ctx.task_info.parallelism as usize {
                let config = GeneratorConfig::new(
                    NexmarkConfig::new(
                        self.first_event_rate,
                        self.num_events,
                        ctx.task_info.parallelism as usize,
                    ),
                    SystemTime::now(),
                    1,
                    self.num_events,
                    1,
                );
                let splits = config.split(ctx.task_info.parallelism as u64);
                NexmarkSourceState {
                    config: splits[ctx.task_info.task_index as usize].clone(),
                    event_count: 0,
                }
            } else {
                ss.get(&(ctx.task_info.task_index as usize))
                    .unwrap()
                    .clone()
            }
        });
    }

    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> SourceFinishType {
        let state = self.state.as_ref().unwrap().clone();

        let mut generator = NexmarkGenerator::from_config(&state.config, state.event_count as u64);

        let mut random = SmallRng::seed_from_u64(ctx.task_info.task_index as u64);
        let mut last_check = Instant::now();

        let mut records = 0;
        let mut flush_time = Instant::now();
        let mut person_builder = StructBuilder::from_fields(person_fields(), 128);
        let mut auction_builder = StructBuilder::from_fields(auction_fields(), 128);
        let mut bid_builder = StructBuilder::from_fields(bid_fields(), 128);
        let mut timestamp_builder = TimestampNanosecondBuilder::with_capacity(128);

        while generator.has_next() {
            let now = SystemTime::now();
            let next_event = generator.next_event(&mut random);
            if next_event.wallclock_timestamp > now {
                sleep(next_event.wallclock_timestamp.duration_since(now).unwrap()).await;
            }

            records += 1;
            next_event.person.as_ref().write_into(&mut person_builder);
            next_event.auction.as_ref().write_into(&mut auction_builder);
            next_event.bid.as_ref().write_into(&mut bid_builder);
            timestamp_builder.append_value(to_nanos(next_event.event_timetamp) as i64);

            if should_flush(records, flush_time) {
                collector
                    .collect(
                        RecordBatch::try_new(
                            ctx.out_schema.schema.clone(),
                            vec![
                                Arc::new(person_builder.finish()),
                                Arc::new(auction_builder.finish()),
                                Arc::new(bid_builder.finish()),
                                Arc::new(timestamp_builder.finish()),
                            ],
                        )
                        .unwrap(),
                    )
                    .await;
                records = 0;
                flush_time = Instant::now();
            }

            // TODO: rewrite this as a select with the sleep
            if last_check.elapsed() > Duration::from_millis(10) {
                match ctx.control_rx.try_recv() {
                    Ok(ControlMessage::Checkpoint(c)) => {
                        // checkpoint our state
                        ctx.table_manager
                            .get_global_keyed_state::<usize, NexmarkSourceState>("s")
                            .await
                            .expect("should be able to get nexmark state")
                            .insert(
                                ctx.task_info.task_index as usize,
                                NexmarkSourceState {
                                    config: state.config.clone(),
                                    event_count: generator.events_count_so_far as usize,
                                },
                            )
                            .await;
                        debug!("starting checkpointing {}", ctx.task_info.task_index);
                        if self.start_checkpoint(c, ctx, collector).await {
                            return SourceFinishType::Immediate;
                        }
                    }
                    Ok(ControlMessage::Stop { mode }) => {
                        info!("Stopping nexmark source");
                        match mode {
                            StopMode::Graceful => {
                                return SourceFinishType::Graceful;
                            }
                            StopMode::Immediate => {
                                return SourceFinishType::Immediate;
                            }
                        }
                    }
                    Err(TryRecvError::Empty) => {}
                    x => {
                        warn!("{:?}", x);
                    }
                }
                last_check = Instant::now();
            }
        }

        info!("finished generating nexmark data");
        SourceFinishType::Final
    }
}

#[derive(Clone, Encode, Decode, Debug, PartialEq)]
pub struct NexmarkConfig {
    num_events: Option<u64>,
    num_event_generators: u64,
    first_event_rate: f64,
    next_event_rate: u64,
    rate_period_seconds: u64,
    preload_seconds: u64,
    stream_timeout: u64,
    is_rate_limited: bool,
    use_wallclock_event_time: bool,
    person_proportion: u64,
    auction_proportion: u64,
    bid_proportion: u64,
    avg_person_byte_size: u64,
    avg_auction_byte_size: u64,
    avg_bid_byte_size: u64,
    hot_auction_ratio: u64,
    hot_seller_ratio: u64,
    hot_bidders_ratio: u64,
    window_size_seconds: u64,
    window_period_seconds: u64,
    watermark_holdback_seconds: u64,
    num_inflight_auctions: u64,
    num_active_people: u64,
    occasional_delay_seconds: u64,
    prob_delayed_event: f64,
    out_of_order_group_size: u64,
}

impl NexmarkConfig {
    pub fn new(
        first_event_rate: f64,
        num_events: Option<u64>,
        parallelism: usize,
    ) -> NexmarkConfig {
        NexmarkConfig {
            num_events,
            num_event_generators: parallelism as u64,
            first_event_rate,
            next_event_rate: 10000,
            rate_period_seconds: 600,
            preload_seconds: 0,
            stream_timeout: 240,
            is_rate_limited: false,
            use_wallclock_event_time: false,
            person_proportion: 1,
            auction_proportion: 3,
            bid_proportion: 46,
            avg_person_byte_size: 200,
            avg_auction_byte_size: 500,
            avg_bid_byte_size: 100,
            hot_auction_ratio: 2,
            hot_seller_ratio: 4,
            hot_bidders_ratio: 4,
            window_size_seconds: 10,
            window_period_seconds: 5,
            watermark_holdback_seconds: 0,
            num_inflight_auctions: 100,
            num_active_people: 1000,
            occasional_delay_seconds: 3,
            prob_delayed_event: 0.1,
            out_of_order_group_size: 50,
        }
    }

    fn get_total_proportion(&self) -> u64 {
        self.person_proportion + self.auction_proportion + self.bid_proportion
    }

    fn get_max_events(&self, max_events: Option<u64>) -> u64 {
        match max_events {
            Some(max_events) => max_events,
            None => {
                u64::MAX
                    / (self.get_total_proportion()
                        * u64::max(
                            u64::max(self.avg_auction_byte_size, self.avg_bid_byte_size),
                            self.avg_person_byte_size,
                        ))
            }
        }
    }
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
pub struct GeneratorConfig {
    configuration: NexmarkConfig,
    person_proportion: u64,
    auction_proportion: u64,
    _bid_proportion: u64,
    total_proportion: u64,
    // TODO: this was supposed to be an array.
    inter_event_delay: Duration,
    _step_length_second: u64,
    base_time: SystemTime,
    first_event_id: u64,
    max_events: u64,
    first_event_number: u64,
    _epoch_period_ms: u64,
    _events_per_epoch: u64,
}

impl GeneratorConfig {
    pub fn new(
        nexmark_config: NexmarkConfig,
        base_time: SystemTime,
        first_event_id: u64,
        max_events: Option<u64>,
        first_event_number: u64,
    ) -> GeneratorConfig {
        GeneratorConfig {
            person_proportion: nexmark_config.person_proportion,
            auction_proportion: nexmark_config.auction_proportion,
            _bid_proportion: nexmark_config.bid_proportion,
            total_proportion: nexmark_config.person_proportion
                + nexmark_config.auction_proportion
                + nexmark_config.bid_proportion,
            inter_event_delay: Duration::from_nanos(
                (1_000_000_000.0 / (nexmark_config.first_event_rate)
                    * (nexmark_config.num_event_generators as f64)) as u64,
            ),
            _step_length_second: nexmark_config.rate_period_seconds.div_ceil(2),
            base_time,
            first_event_id,
            max_events: nexmark_config.get_max_events(max_events),
            first_event_number,
            configuration: nexmark_config,
            _epoch_period_ms: 0,
            _events_per_epoch: 0,
        }
    }

    fn copy_with(
        &self,
        first_event_id: u64,
        max_events: Option<u64>,
        first_event_number: u64,
    ) -> GeneratorConfig {
        GeneratorConfig::new(
            self.configuration.clone(),
            self.base_time,
            first_event_id,
            max_events,
            first_event_number,
        )
    }

    pub fn split(&self, n: u64) -> Vec<GeneratorConfig> {
        let mut result = Vec::new();
        if n == 1 {
            result.push(self.clone());
            return result;
        }
        let mut sub_max_events = self.max_events / n;
        let mut sub_first_event_id = self.first_event_id;
        let first_event = self.first_event_number;
        for i in 0..n {
            if i == n - 1 {
                // Don't lose any events to round-down.
                sub_max_events = self.max_events - sub_max_events * (n - 1);
            }
            let generator = self.copy_with(sub_first_event_id, Some(sub_max_events), first_event);
            result.push(generator);
            sub_first_event_id += sub_max_events;
        }
        result
    }

    fn next_adjusted_event_number(&self, num_events: u64) -> u64 {
        let n = self.configuration.out_of_order_group_size;
        let event_number = self.first_event_number + num_events;
        let base = (event_number / n) * n;
        let offset = (event_number * 953) % n;
        base + offset
    }

    fn next_event_number(&self, num_events: u64) -> u64 {
        self.first_event_number + num_events
    }

    fn next_event_number_for_watermark(&self, num_events: u64) -> u64 {
        let n = self.configuration.out_of_order_group_size;
        let event_number = self.next_event_number(num_events);
        (event_number / n) * n
    }

    fn random_string(random: &mut SmallRng, max_length: u32) -> String {
        let size = random.random_range(MIN_STRING_LENGTH..max_length);
        Alphanumeric.sample_string(random, size as usize)
    }

    fn next_extra_string(
        random: &mut SmallRng,
        current_size: usize,
        desired_average_size: usize,
    ) -> String {
        if current_size > desired_average_size {
            return String::new();
        }
        let size = desired_average_size - current_size;
        Alphanumeric.sample_string(random, size)
    }

    pub fn next_auction(
        &self,
        event_counts_so_far: u64,
        event_id: u64,
        random: &mut SmallRng,
        timestamp: SystemTime,
    ) -> Auction {
        let id = self.last_base0_auction_id(event_id) + 1000;

        let mut seller;
        // Here P(auction will be for a hot seller) = 1 - 1/hotSellersRatio.
        if random.random_range(0..(self.configuration.hot_seller_ratio)) > 0 {
            // Choose the first person in the batch of last HOT_SELLER_RATIO people.
            seller = (self.last_base0_person_id(event_id) / HOT_SELLER_RATIO) * HOT_SELLER_RATIO;
        } else {
            seller = self.next_base0_person_id(event_id, random);
        }
        seller += FIRST_PERSON_ID;

        let category = FIRST_CATEGORY_ID + random.random_range(0..NUM_CATEGORIES);
        let initial_bid = Self::next_price(random);
        let expires = timestamp + self.next_auction_length(event_counts_so_far, random, timestamp);
        let name = Self::random_string(random, 20);
        let desc = Self::random_string(random, 100);
        let reserve = initial_bid + Self::next_price(random);
        let current_size = 8 + name.len() + desc.len() + 8 + 8 + 8 + 8 + 8;
        let extra = Self::next_extra_string(
            random,
            current_size,
            self.configuration.avg_auction_byte_size as usize,
        );
        Auction {
            id: id as i64,
            item_name: name,
            description: desc,
            initial_bid: initial_bid as i64,
            reserve: reserve as i64,
            datetime: timestamp,
            expires,
            seller: seller as i64,
            category: category as i64,
            extra,
        }
    }
    fn last_base0_auction_id(&self, event_id: u64) -> u64 {
        let mut epoch: u64 = event_id / self.total_proportion;
        let mut offset = event_id % self.total_proportion;
        if offset < self.person_proportion {
            // About to generate a person.
            // Go back to the last auction in the last epoch.
            epoch -= 1;
            offset = self.auction_proportion - 1;
        } else if offset >= self.person_proportion + self.auction_proportion {
            // About to generate a bid.
            // Go back to the last auction generated in this epoch.
            offset = self.auction_proportion - 1;
        } else {
            // About to generate an auction.
            offset -= self.person_proportion;
        }
        epoch * self.auction_proportion + offset
    }

    pub fn next_base0_auction_id(&self, event_id: u64, random: &mut SmallRng) -> u64 {
        let max_auction = self.last_base0_auction_id(event_id);
        let min_auction = max_auction.saturating_sub(self.configuration.num_inflight_auctions);
        random.random_range(min_auction..max_auction + 1 + AUCTION_ID_LEAD)
    }

    fn last_base0_person_id(&self, event_id: u64) -> u64 {
        let epoch = event_id / self.total_proportion;
        let mut offset = event_id % self.total_proportion;
        if offset >= self.person_proportion {
            // About to generate an auction or bid.
            // Go back to the last person generated in this epoch.
            offset = self.person_proportion - 1;
        }
        // About to generate a person.
        epoch * self.person_proportion + offset
    }

    fn next_base0_person_id(&self, event_id: u64, random: &mut SmallRng) -> u64 {
        let num_people = self.last_base0_person_id(event_id);
        let active_people = u64::min(num_people, self.configuration.num_active_people);
        let n = random.random_range(0..(active_people + PERSON_ID_LEAD));
        num_people - active_people + n
    }

    fn timestamp_for_event(&self, event_number: u64) -> SystemTime {
        self.base_time
            + Duration::from_nanos(self.inter_event_delay.as_nanos() as u64 * event_number)
    }

    fn next_price(random: &mut SmallRng) -> u64 {
        (f64::powf(10.0, random.random_range(0.0..6.0)) * 100.0) as u64
    }

    fn next_auction_length(
        &self,
        events_counts_so_far: u64,
        random: &mut SmallRng,
        timestamp: SystemTime,
    ) -> Duration {
        // What's our current event number?
        let current_event_number = self.next_adjusted_event_number(events_counts_so_far);
        // How many events till we've generated numInFlightAuctions?
        let num_events_for_auctions = (self.configuration.num_inflight_auctions
            * self.total_proportion)
            / self.auction_proportion;
        // When will the auction numInFlightAuctions beyond now be generated?
        let future_auction =
            self.timestamp_for_event(current_event_number + num_events_for_auctions);
        // System.out.printf("*** auction will be for %dms (%d events ahead) ***\n",
        //     futureAuction - timestamp, numEventsForAuctions);
        // Choose a length with average horizonMs.
        let horizon = future_auction.duration_since(timestamp).unwrap();
        Duration::from_nanos(
            1 + u64::max(
                random.random_range(0..(1 + horizon.as_nanos() as u64 * 2)),
                1,
            ),
        )
    }

    fn next_person(
        &mut self,
        next_event_id: u64,
        random: &mut SmallRng,
        timestamp: SystemTime,
    ) -> Person {
        let id = self.last_base0_person_id(next_event_id) + FIRST_PERSON_ID;

        let name = format!(
            "{} {}",
            FIRST_NAMES.choose(random).unwrap(),
            LAST_NAMES.choose(random).unwrap()
        );
        let email_address = format!(
            "{}@{}.com",
            Self::random_string(random, 7),
            Self::random_string(random, 5)
        );
        let credit_card = format!(
            "{:04} {:04} {:04} {:04}",
            random.random_range(0..10000),
            random.random_range(0..10000),
            random.random_range(0..10000),
            random.random_range(0..10000)
        );
        let city = US_CITIES.choose(random).unwrap().to_string();
        let state = US_STATES.choose(random).unwrap().to_string();
        let current_size =
            8 + name.len() + email_address.len() + credit_card.len() + city.len() + state.len();
        let extra = Self::next_extra_string(
            random,
            current_size,
            self.configuration.avg_person_byte_size as usize,
        );

        Person {
            id: id as i64,
            name,
            email_address,
            credit_card,
            city,
            state,
            datetime: timestamp,
            extra,
        }
    }

    fn next_bid(
        &self,
        event_id: u64,
        random: &mut SmallRng,
        timestamp: SystemTime,
        channel_cache: &mut ChannelCache,
    ) -> Bid {
        let mut auction = if random.random_range(0..self.configuration.hot_auction_ratio) > 0 {
            (self.last_base0_auction_id(event_id) / HOT_AUCTION_RATIO) * HOT_AUCTION_RATIO
        } else {
            self.next_base0_auction_id(event_id, random)
        };
        auction += FIRST_AUCTION_ID;
        let mut bidder = if random.random_range(0..self.configuration.hot_bidders_ratio) > 0 {
            (self.last_base0_person_id(event_id) / HOT_BIDDER_RATIO) * HOT_BIDDER_RATIO
        } else {
            self.next_base0_person_id(event_id, random)
        };
        bidder += FIRST_PERSON_ID;
        let price = Self::next_price(random);
        let channel;
        let url;
        if random.random_range(0..HOT_CHANNELS_RATIO) > 0 {
            let i = random.random_range(0..HOT_CHANNELS.len());
            channel = HOT_CHANNELS[i].to_string();
            url = HOT_URLS[i].to_string();
        } else {
            let pair = channel_cache.get_channel(random.random_range(0..CHANNELS_NUMBER));
            channel = pair.0.to_string();
            url = pair.1;
        }
        let extra =
            Self::next_extra_string(random, 32, self.configuration.avg_person_byte_size as usize);
        Bid {
            auction: auction as i64,
            bidder: bidder as i64,
            price: price as i64,
            channel,
            url,
            datetime: timestamp,
            extra,
        }
    }
}

struct ChannelCache {
    cache: HashMap<u64, (String, String)>,
    random: SmallRng,
}

impl ChannelCache {
    fn get_channel(&mut self, channel: u64) -> (String, String) {
        if let std::collections::hash_map::Entry::Vacant(e) = self.cache.entry(channel) {
            let new_value = Self::new_instance(&mut self.random, channel);
            e.insert(new_value);
        }
        match self.cache.get(&channel) {
            Some(val) => (val.0.to_string(), val.1.to_string()),
            None => {
                unreachable!();
            }
        }
    }

    fn new_instance(random: &mut SmallRng, channel: u64) -> (String, String) {
        let url = format!(
            "https://www.nexmark.com/{}/{}/{}/item.htm?query=1",
            GeneratorConfig::random_string(random, 5),
            GeneratorConfig::random_string(random, 5),
            GeneratorConfig::random_string(random, 5)
        );
        if random.random_range(0..10) > 0 {
            return (
                format!("channel-{}", channel),
                format!("{}&channel_id={}", url, channel),
            );
        }
        (format!("channel-{}", channel), url)
    }
}

pub struct NexmarkGenerator {
    pub(crate) generator_config: GeneratorConfig,
    channel_cache: ChannelCache,
    events_count_so_far: u64,
    wallclock_base_time: SystemTime,
}

impl NexmarkGenerator {
    #[allow(unused)]
    pub fn new(first_event_rate: f64, num_events: Option<u64>) -> NexmarkGenerator {
        let time = SystemTime::now();
        NexmarkGenerator::from_config(
            &GeneratorConfig::new(
                NexmarkConfig::new(first_event_rate, num_events, 1),
                time,
                1,
                num_events,
                1,
            ),
            0,
        )
    }

    pub fn from_config(
        generator_config: &GeneratorConfig,
        events_count_so_far: u64,
    ) -> NexmarkGenerator {
        let next_event_timestamp = generator_config
            .timestamp_for_event(generator_config.next_event_number(events_count_so_far));

        let wallclock_base_time = SystemTime::now()
            - (next_event_timestamp.duration_since(generator_config.base_time)).unwrap();

        NexmarkGenerator {
            generator_config: generator_config.clone(),
            channel_cache: ChannelCache {
                cache: HashMap::new(),
                random: SmallRng::seed_from_u64(to_millis(generator_config.base_time)),
            },
            events_count_so_far,
            wallclock_base_time,
        }
    }

    pub fn has_next(&self) -> bool {
        self.events_count_so_far < self.generator_config.max_events
    }

    pub fn next_event(&mut self, random: &mut SmallRng) -> TimedEvent {
        let event_timestamp = self.generator_config.timestamp_for_event(
            self.generator_config
                .next_event_number(self.events_count_so_far),
        );
        let adjusted_event_timestamp = self.generator_config.timestamp_for_event(
            self.generator_config
                .next_adjusted_event_number(self.events_count_so_far),
        );
        let watermark = self.generator_config.timestamp_for_event(
            self.generator_config
                .next_event_number_for_watermark(self.events_count_so_far),
        );

        let wallclock_timestamp = self.wallclock_base_time
            + event_timestamp
                .duration_since(self.generator_config.base_time)
                .unwrap();
        let new_event_id = self.generator_config.first_event_id
            + self
                .generator_config
                .next_adjusted_event_number(self.events_count_so_far);
        let rem = new_event_id % self.generator_config.total_proportion;

        let mut event = TimedEvent {
            person: None,
            auction: None,
            bid: None,
            wallclock_timestamp,
            event_timetamp: adjusted_event_timestamp,
            watermark,
        };

        if rem < self.generator_config.person_proportion {
            event.person = Some(self.generator_config.next_person(
                new_event_id,
                random,
                adjusted_event_timestamp,
            ));
        } else if rem
            < self.generator_config.person_proportion + self.generator_config.auction_proportion
        {
            event.auction = Some(self.generator_config.next_auction(
                self.events_count_so_far,
                new_event_id,
                random,
                adjusted_event_timestamp,
            ));
        } else {
            event.bid = Some(self.generator_config.next_bid(
                new_event_id,
                random,
                adjusted_event_timestamp,
                &mut self.channel_cache,
            ));
        }
        self.events_count_so_far += 1;

        event
    }
}
#[derive(Debug)]
pub struct TimedEvent {
    person: Option<Person>,
    auction: Option<Auction>,
    bid: Option<Bid>,
    wallclock_timestamp: SystemTime,
    event_timetamp: SystemTime,
    #[allow(unused)]
    watermark: SystemTime,
}
