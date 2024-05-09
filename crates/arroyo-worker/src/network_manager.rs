#![allow(clippy::redundant_slicing)]
use anyhow::{anyhow, bail};
use arrow::buffer::MutableBuffer;
use arrow::ipc::reader::read_record_batch;
use arrow::ipc::writer::{DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteOptions};
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use arroyo_types::ArrowMessage;
use bincode::config;
use std::{collections::HashMap, mem::size_of, pin::Pin, sync::Arc, time::Duration};
use tokio::{
    io::{self, BufReader, BufWriter},
    select,
    sync::Mutex,
};
use tracing::warn;

use bytes::{Buf, BufMut};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use arroyo_operator::context::{BatchReceiver, BatchSender};
use tokio::time::{interval, Interval};
use tokio_stream::StreamExt;

use arroyo_operator::inq_reader::InQReader;
use arroyo_server_common::shutdown::ShutdownGuard;

#[derive(Clone)]
struct NetworkSender {
    tx: BatchSender,
    schema: SchemaRef,
}

#[derive(Clone)]
pub struct Senders {
    senders: HashMap<Quad, NetworkSender>,
}

impl Default for Senders {
    fn default() -> Self {
        Self::new()
    }
}

impl Senders {
    pub fn new() -> Self {
        Self {
            senders: HashMap::new(),
        }
    }

    pub fn merge(&mut self, other: Self) {
        self.senders.extend(other.senders)
    }

    pub fn add(&mut self, quad: Quad, schema: SchemaRef, tx: BatchSender) {
        self.senders.insert(quad, NetworkSender { tx, schema });
    }

    async fn send(&mut self, header: Header, data: Vec<u8>) {
        let sender = self.senders.get(&header.as_quad()).unwrap();

        let message = match header.message_type {
            MessageType::Data => ArrowMessage::Data(
                read_message(sender.schema.clone(), data).expect("failed to read message"),
            ),
            MessageType::Signal => ArrowMessage::Signal(
                bincode::decode_from_slice(&data, config::standard())
                    .expect("couldn't decode signal message, probably a record.")
                    .0,
            ),
        };

        if let Err(send_error) = sender.tx.send(message).await {
            if !send_error.0.is_end() {
                panic!("{:?} not sent", send_error.0);
            } else {
                warn!("couldn't send end message");
            }
        }
    }
}

pub struct InNetworkLink {
    _source: String,
    stream: BufReader<TcpStream>,
    senders: Senders,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum MessageType {
    Data,
    Signal,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Header {
    src_operator: u32,
    src_subtask: u32,
    dst_operator: u32,
    dst_subtask: u32,
    len: usize,
    message_type: MessageType,
}

impl Header {
    fn from_quad(quad: Quad, len: usize, message_type: MessageType) -> Self {
        Self {
            src_operator: quad.src_id as u32,
            src_subtask: quad.src_idx as u32,
            dst_operator: quad.dst_id as u32,
            dst_subtask: quad.dst_idx as u32,
            len,
            message_type,
        }
    }

    fn as_quad(&self) -> Quad {
        Quad {
            src_id: self.src_operator as usize,
            src_idx: self.src_subtask as usize,
            dst_id: self.dst_operator as usize,
            dst_idx: self.dst_subtask as usize,
        }
    }

    fn from_bytes<B: Buf>(mut bytes: B) -> Header {
        Header {
            src_operator: bytes.get_u32_le(),
            src_subtask: bytes.get_u32_le(),
            dst_operator: bytes.get_u32_le(),
            dst_subtask: bytes.get_u32_le(),
            len: bytes.get_u32_le() as usize,
            message_type: match bytes.get_u32_le() {
                0 => MessageType::Data,
                1 => MessageType::Signal,
                b => panic!("invalid message type: {}", b),
            },
        }
    }

    async fn write<W: AsyncWrite + AsyncWriteExt>(&self, writer: &mut Pin<&mut W>) {
        let mut bytes = [0u8; size_of::<Header>()];
        let mut buf = &mut bytes[..];
        buf.put_u32_le(self.src_operator);
        buf.put_u32_le(self.src_subtask);
        buf.put_u32_le(self.dst_operator);
        buf.put_u32_le(self.dst_subtask);
        buf.put_u32_le(self.len as u32);
        buf.put_u32_le(match self.message_type {
            MessageType::Data => 0,
            MessageType::Signal => 1,
        });

        writer.write_all(&bytes).await.unwrap();
    }
}

impl InNetworkLink {
    pub fn new(source: String, stream: TcpStream, senders: Senders) -> Self {
        InNetworkLink {
            _source: source,
            stream: BufReader::new(stream),
            senders,
        }
    }

    async fn next(&mut self, header_buf: &mut [u8]) -> Result<(), io::Error> {
        self.stream.read_exact(header_buf).await?;
        let header = Header::from_bytes(&header_buf[..]);

        let mut buf = vec![0; header.len];
        self.stream.read_exact(&mut buf).await?;

        self.senders.send(header, buf).await;
        Ok(())
    }

    pub fn start(mut self) {
        tokio::spawn(async move {
            let mut header_buf = vec![0u8; size_of::<Header>()];
            loop {
                if let Err(e) = self.next(&mut header_buf).await {
                    warn!("Socket hung up: {:?}", e);
                    break;
                };
            }
        });
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub struct Quad {
    pub src_id: usize,
    pub src_idx: usize,
    pub dst_id: usize,
    pub dst_idx: usize,
}

struct NetworkReceiver {
    quad: Quad,
    rx: BatchReceiver,
    dictionary_tracker: Arc<Mutex<DictionaryTracker>>,
}

struct OutNetworkLink {
    _dest: String,
    stream: BufWriter<TcpStream>,
    receivers: Vec<NetworkReceiver>,
}

impl OutNetworkLink {
    pub async fn connect(dest: String) -> Self {
        let mut rand = StdRng::from_entropy();
        for i in 0..10 {
            match TcpStream::connect(&dest).await {
                Ok(stream) => {
                    return Self {
                        _dest: dest,
                        stream: BufWriter::new(stream),
                        receivers: vec![],
                    }
                }
                Err(e) => {
                    warn!("Failed to connect to {dest}: {:?}", e);
                    tokio::time::sleep(Duration::from_millis(
                        (i + 1) * (50 + rand.gen_range(1..50)),
                    ))
                    .await;
                }
            }
        }
        panic!("failed to connect to {dest}");
    }

    pub async fn add_receiver(&mut self, quad: Quad, rx: BatchReceiver) {
        self.receivers.push(NetworkReceiver {
            quad,
            rx,
            dictionary_tracker: Arc::new(Mutex::new(DictionaryTracker::new(true))),
        });
    }

    pub fn start(mut self) {
        tokio::spawn(async move {
            let mut sel = InQReader::new();
            for NetworkReceiver {
                quad,
                mut rx,
                dictionary_tracker,
            } in self.receivers
            {
                let stream = async_stream::stream! {
                    while let Some(item) = rx.recv().await {
                        yield (quad, dictionary_tracker.clone(), item);
                    }
                };
                sel.push(Box::pin(stream));
            }
            let mut flush_interval: Interval = interval(Duration::from_millis(100));

            let write_options = IpcWriteOptions::default();

            loop {
                select! {
                    Some(((quad, dictionary_tracker, msg), s)) = sel.next() => {
                        match msg {
                            ArrowMessage::Signal(signal) => {
                                let data = bincode::encode_to_vec(&signal, config::standard()).unwrap();
                                let header = Header::from_quad(quad, data.len(), MessageType::Signal);
                                header.write(&mut Pin::new(&mut self.stream)).await;
                                self.stream.write_all(&data).await.unwrap();
                            }
                            ArrowMessage::Data(data) => {
                                let (_, encoded_message) = {
                                    let mut dictionary_tracker = dictionary_tracker.lock().await;
                                    IpcDataGenerator {}.encoded_batch(&data, &mut dictionary_tracker, &write_options)
                                      .expect("failed to encode batch")
                                };
                                write_message_and_header(&mut Pin::new(&mut self.stream), quad, encoded_message).await.unwrap();
                            }
                        };

                        sel.push(s);
                    }
                    _ = flush_interval.tick() => {
                        self.stream.flush().await.unwrap();
                    }
                }
            }
        });
    }
}

enum InStreamsOrSenders {
    InStreams(Vec<TcpStream>),
    Senders(Senders),
}

pub struct NetworkManager {
    port: u16,
    in_streams: Arc<Mutex<InStreamsOrSenders>>,
    out_streams: Arc<Mutex<HashMap<Quad, OutNetworkLink>>>,
}

impl NetworkManager {
    pub fn new(port: u16) -> Self {
        NetworkManager {
            port,
            in_streams: Arc::new(Mutex::new(InStreamsOrSenders::InStreams(vec![]))),
            out_streams: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn open_listener(&mut self, shutdown_guard: ShutdownGuard) -> u16 {
        let port = self.port;
        let listener = TcpListener::bind(format!("0.0.0.0:{}", port))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();

        let streams = Arc::clone(&self.in_streams);
        shutdown_guard.into_spawn_task(async move {
            loop {
                let (stream, _) = listener.accept().await?;

                let mut s = streams.lock().await;

                match &mut *s {
                    InStreamsOrSenders::InStreams(streams) => streams.push(stream),
                    InStreamsOrSenders::Senders(ref senders) => {
                        let senders = senders.clone();
                        tokio::spawn(async move {
                            InNetworkLink::new(
                                stream.local_addr().unwrap().to_string(),
                                stream,
                                senders,
                            )
                            .start();
                        });
                    }
                }
            }
            #[allow(unreachable_code)]
            Ok(())
        });

        port
    }

    pub async fn start(&mut self, senders: Senders) {
        let mut sockets = self.in_streams.lock().await;

        match &mut *sockets {
            InStreamsOrSenders::InStreams(ref mut in_streams) => {
                for s in in_streams.drain(..) {
                    let senders = senders.clone();
                    tokio::spawn(async move {
                        InNetworkLink::new(s.local_addr().unwrap().to_string(), s, senders.clone())
                            .start();
                    });
                }
            }
            InStreamsOrSenders::Senders(_) => {
                panic!("already started!");
            }
        }

        *sockets = InStreamsOrSenders::Senders(senders.clone());

        let mut out_streams = self.out_streams.lock().await;
        for (_, s) in out_streams.drain() {
            s.start();
        }
    }

    pub async fn connect(&self, addr: String, quad: Quad, rx: BatchReceiver) {
        let link = OutNetworkLink::connect(addr.clone()).await;
        let mut ins = self.out_streams.lock().await;
        if let std::collections::hash_map::Entry::Vacant(e) = ins.entry(quad) {
            e.insert(link);
        }

        ins.get_mut(&quad)
            .as_mut()
            .unwrap()
            .add_receiver(quad, rx)
            .await;
    }
}

#[inline]
fn pad_to_8(len: u32) -> usize {
    (((len + 7) & !7) - len) as usize
}

// Async-ified and modified version of arrow::ipc::writer::write_message
pub async fn write_message_and_header<W: AsyncWrite + AsyncWriteExt>(
    writer: &mut Pin<&mut W>,
    quad: Quad,
    encoded: EncodedData,
) -> Result<(), ArrowError> {
    let arrow_data_len = encoded.arrow_data.len();
    if arrow_data_len % 8 != 0 {
        return Err(ArrowError::MemoryError(
            "Arrow data not aligned".to_string(),
        ));
    }

    let buffer = encoded.ipc_message;

    let prefix_size = 4;
    let flatbuf_size = buffer.len();

    let total_size = prefix_size + flatbuf_size + arrow_data_len;

    let header = Header::from_quad(quad, total_size, MessageType::Data);
    header.write(writer).await;

    let mut bytes_written = 0;

    // write the flatbuf
    if flatbuf_size > 0 {
        writer
            .write_all(&(flatbuf_size as u32).to_le_bytes())
            .await?;
        writer.write_all(&buffer).await?;
        bytes_written += buffer.len() + 4;
    }
    // write arrow data
    if arrow_data_len > 0 {
        let len = encoded.arrow_data.len() as u32;
        let pad_len = pad_to_8(len);

        // write body buffer
        writer.write_all(&encoded.arrow_data).await?;
        bytes_written += encoded.arrow_data.len();
        if pad_len > 0 {
            writer.write_all(&vec![0u8; pad_len][..]).await?;
            bytes_written += pad_len;
        }
    }

    assert_eq!(
        bytes_written, total_size,
        "Wrote unexpected number of bytes {} != {}",
        bytes_written, total_size
    );

    Ok(())
}

fn read_message(schema: SchemaRef, data: Vec<u8>) -> anyhow::Result<RecordBatch> {
    let mut buf = &data[..];

    // read the header size
    let meta_size = buf.get_u32_le() as usize;
    let mut meta_buffer = vec![0; meta_size];
    std::io::Read::read_exact(&mut buf, &mut meta_buffer)?;

    let message = arrow::ipc::root_as_message(&meta_buffer)
        .map_err(|e| anyhow!("Unable to read IPC message: {:?}", e))?;

    let arrow::ipc::MessageHeader::RecordBatch = message.header_type() else {
        bail!("unexpected message type: {:?}", message.header_type());
    };

    let Some(batch) = message.header_as_record_batch() else {
        bail!("Unable to read IPC message as record batch")
    };

    // read the block that makes up the record batch into a buffer
    let mut batch_buf = MutableBuffer::from_len_zeroed(message.bodyLength() as usize);
    std::io::Read::read_exact(&mut buf, &mut batch_buf)?;

    Ok(read_record_batch(
        &batch_buf.into(),
        batch,
        schema,
        &HashMap::new(),
        None,
        &message.version(),
    )?)
}

#[cfg(test)]
mod test {
    use arrow_array::{ArrayRef, RecordBatch, TimestampNanosecondArray, UInt64Array};
    use arrow_schema::{Field, Schema, TimeUnit};
    use std::sync::Arc;
    use std::time::SystemTime;
    use std::{pin::Pin, time::Duration};

    use arroyo_operator::context::batch_bounded;
    use arroyo_server_common::shutdown::Shutdown;
    use arroyo_types::{to_nanos, ArrowMessage, CheckpointBarrier, SignalMessage};
    use tokio::time::timeout;

    use crate::network_manager::{MessageType, Quad};

    use super::{Header, NetworkManager, Senders};

    #[tokio::test]
    async fn test_header_serdes() {
        let mut buffer = vec![];

        let header = Header {
            src_operator: 12412,
            src_subtask: 3,
            dst_operator: 9098,
            dst_subtask: 100,
            len: 30,
            message_type: MessageType::Signal,
        };

        header.write(&mut Pin::new(&mut buffer)).await;

        let h2 = Header::from_bytes(&buffer[..]);

        assert_eq!(header, h2);
    }

    #[tokio::test]
    async fn test_client_server() {
        let (server_tx, mut server_rx) = batch_bounded(10);

        let mut senders = Senders::new();

        let quad = Quad {
            src_id: 50,
            src_idx: 1,
            dst_id: 21234,
            dst_idx: 3,
        };

        let time = SystemTime::now();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", arrow_schema::DataType::UInt64, false),
            Field::new(
                "time",
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from((0..10).collect::<Vec<_>>())),
            Arc::new(TimestampNanosecondArray::from(vec![
                to_nanos(time) as i64;
                10
            ])),
        ];

        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

        senders.add(quad, schema.clone(), server_tx);

        let shutdown = Shutdown::new("test");
        let mut nm = NetworkManager::new(0);
        let port = nm.open_listener(shutdown.guard("test")).await;

        let (client_tx, client_rx) = batch_bounded(10);
        nm.connect(format!("localhost:{}", port), quad, client_rx)
            .await;

        nm.start(senders).await;

        client_tx
            .send(ArrowMessage::Data(batch.clone()))
            .await
            .unwrap();

        let result = timeout(Duration::from_secs(1), server_rx.recv())
            .await
            .unwrap()
            .expect("timed out");

        let ArrowMessage::Data(result) = result else {
            panic!("expected bytes");
        };

        assert_eq!(result, batch);

        // test control message
        let message = ArrowMessage::Signal(SignalMessage::Barrier(CheckpointBarrier {
            epoch: 5,
            min_epoch: 3,
            timestamp: SystemTime::now(),
            then_stop: false,
        }));

        client_tx.send(message.clone()).await.unwrap();

        let result = timeout(Duration::from_secs(1), server_rx.recv())
            .await
            .unwrap()
            .expect("timed out");

        assert_eq!(result, message);
    }
}
