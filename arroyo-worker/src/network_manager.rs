#![allow(clippy::redundant_slicing)]
use arroyo_types::Message;
use bincode::config;
use std::{collections::HashMap, mem::size_of, pin::Pin, sync::Arc, time::Duration};
use tokio::{
    io::{self, BufReader, BufWriter},
    select,
    sync::Mutex,
};
use tracing::warn;

use bytes::{Buf, BufMut};
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc::{Receiver, Sender},
};

use tokio::time::{interval, Interval};
use tokio_stream::StreamExt;
use crate::engine::QueueItem;

use crate::inq_reader::InQReader;

#[derive(Clone)]
pub struct Senders {
    senders: HashMap<Quad, Sender<QueueItem>>,
}

impl Senders {
    pub fn new() -> Self {
        Self {
            senders: HashMap::new(),
        }
    }

    pub fn add(&mut self, quad: Quad, tx: Sender<QueueItem>) {
        self.senders.insert(quad, tx);
    }

    async fn send(&mut self, header: Header, data: Vec<u8>) {
        let tx = self.senders.get(&header.as_quad()).unwrap();
        todo!("networking");
        // if let Err(send_error) = tx.send(QueueItem::Bytes(data)).await {
        //     match send_error.0 {
        //         QueueItem::Data(_) => unreachable!(),
        //         QueueItem::Bytes(data) => {
        //             let message: Message<i64, i64> =
        //                 bincode::decode_from_slice(&data, config::standard())
        //                     .expect("couldn't decode, probably a record.")
        //                     .0;
        //             if !message.is_end() {
        //                 panic!("{:?} not sent", message);
        //             } else {
        //                 warn!("couldn't send end message");
        //             }
        //         }
        //     }
        // }
    }
}

pub struct InNetworkLink {
    _source: String,
    stream: BufReader<TcpStream>,
    senders: Senders,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Header {
    src_operator: u32,
    src_subtask: u32,
    dst_operator: u32,
    dst_subtask: u32,
    len: usize,
}

impl Header {
    fn from_quad(quad: Quad, len: usize) -> Self {
        Self {
            src_operator: quad.src_id as u32,
            src_subtask: quad.src_idx as u32,
            dst_operator: quad.dst_id as u32,
            dst_subtask: quad.dst_idx as u32,
            len,
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
            len: bytes.get_u64_le() as usize,
        }
    }

    async fn write<W: AsyncWrite + AsyncWriteExt>(&self, mut writer: Pin<&mut W>) {
        let mut bytes = [0u8; size_of::<Header>()];
        let mut buf = &mut bytes[..];
        buf.put_u32_le(self.src_operator);
        buf.put_u32_le(self.src_subtask);
        buf.put_u32_le(self.dst_operator);
        buf.put_u32_le(self.dst_subtask);
        buf.put_u64_le(self.len as u64);

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

struct OutNetworkLink {
    _dest: String,
    stream: BufWriter<TcpStream>,
    receivers: Vec<(Quad, Receiver<QueueItem>)>,
}

impl OutNetworkLink {
    pub async fn connect(dest: String) -> Self {
        let stream = TcpStream::connect(&dest).await.unwrap();

        Self {
            _dest: dest,
            stream: BufWriter::new(stream),
            receivers: vec![],
        }
    }

    pub async fn add_receiver(&mut self, quad: Quad, rx: Receiver<QueueItem>) {
        self.receivers.push((quad, rx));
    }

    pub fn start(mut self) {
        tokio::spawn(async move {
            let mut sel = InQReader::new();
            for (quad, mut rx) in self.receivers {
                let stream = async_stream::stream! {
                    while let Some(item) = rx.recv().await {
                        yield (quad, item);
                    }
                };
                sel.push(Box::pin(stream));
            }
            let mut flush_interval: Interval = interval(Duration::from_millis(100));

            todo!("networking");
            // loop {
            //     select! {
            //         Some(((quad, msg), s)) = sel.next() => {
            //             let QueueItem::Bytes(data) = msg else {
            //                 panic!("non-byte data in network queue")
            //             };
            //             let frame = Header::from_quad(quad, data.len());
            //             frame.write(Pin::new(&mut self.stream)).await;
            //             self.stream.write_all(&data).await.unwrap();
            //             sel.push(s);
            //         }
            //         _ = flush_interval.tick() => {
            //             self.stream.flush().await.unwrap();
            //         }
            //     }
            // }
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

    pub async fn open_listener(&mut self) -> u16 {
        let port = self.port;
        let listener = TcpListener::bind(format!("0.0.0.0:{}", port))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();

        let streams = Arc::clone(&self.in_streams);
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();

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

    pub async fn connect(&mut self, addr: String, quad: Quad, rx: Receiver<QueueItem>) {
        let mut ins = self.out_streams.lock().await;
        if let std::collections::hash_map::Entry::Vacant(e) = ins.entry(quad) {
            e.insert(OutNetworkLink::connect(addr.clone()).await);
        }

        ins.get_mut(&quad)
            .as_mut()
            .unwrap()
            .add_receiver(quad, rx)
            .await;
    }
}

#[cfg(test)]
mod test {
    use std::{pin::Pin, time::Duration};

    use crate::old::QueueItem;
    use tokio::{io::AsyncWriteExt, net::TcpStream, sync::mpsc::channel, time::timeout};

    use crate::network_manager::Quad;

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
        };

        header.write(Pin::new(&mut buffer)).await;

        let h2 = Header::from_bytes(&buffer[..]);

        assert_eq!(header, h2);
    }

    #[tokio::test]
    async fn test_server() {
        let (tx, mut rx) = channel(10);

        let mut senders = Senders::new();
        let quad = Quad {
            src_id: 0,
            src_idx: 1,
            dst_id: 2,
            dst_idx: 3,
        };

        senders.add(quad, tx);

        let mut nm = NetworkManager::new(0);
        let port = nm.open_listener().await;

        println!("port: {}", port);

        nm.start(senders).await;

        let mut client = TcpStream::connect(format!("localhost:{}", port))
            .await
            .unwrap();

        let message = b"Hello World!";

        let header = Header {
            src_operator: 0,
            src_subtask: 1,
            dst_operator: 2,
            dst_subtask: 3,
            len: message.len(),
        };

        header.write(Pin::new(&mut client)).await;
        client.write_all(message).await.unwrap();

        let item = rx.recv().await.unwrap();
        let QueueItem::Bytes(data) = item else {
            panic!("expected bytes!");
        };

        assert_eq!(&message[..], &data);
    }

    #[tokio::test]
    async fn test_client_server() {
        let (server_tx, mut server_rx) = channel(10);

        let mut senders = Senders::new();

        let quad = Quad {
            src_id: 50,
            src_idx: 1,
            dst_id: 21234,
            dst_idx: 3,
        };

        senders.add(quad, server_tx);

        let mut nm = NetworkManager::new(0);
        let port = nm.open_listener().await;

        let (client_tx, client_rx) = channel(10);
        nm.connect(format!("localhost:{}", port), quad, client_rx)
            .await;

        nm.start(senders).await;

        let data = b"this is some data being sent... over the network";

        client_tx
            .send(QueueItem::Bytes(data.to_vec()))
            .await
            .unwrap();

        let result = timeout(Duration::from_secs(1), server_rx.recv())
            .await
            .unwrap()
            .expect("timed out");

        let QueueItem::Bytes(bytes) = result else {
            panic!("expected bytes");
        };
        assert_eq!(&data[..], &bytes);
    }
}
