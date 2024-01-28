use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::select;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub struct ShutdownGuard {
    tx: broadcast::Sender<()>,
    token: CancellationToken,
    ref_count: Arc<AtomicUsize>,
    temporary: bool,
}

impl Clone for ShutdownGuard {
    fn clone(&self) -> Self {
        Self::new(self.tx.clone(), self.token.clone(), self.ref_count.clone(), self.temporary)
    }
}

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        if !self.temporary {
            self.token.cancel();
        }
        let count = self.ref_count.fetch_sub(1, Ordering::SeqCst);
        info!("Dropping guard (count={})", count);
        if count == 1 {
            let _ = self.tx.send(());
        }
    }
}

impl ShutdownGuard {
    fn new(tx: broadcast::Sender<()>, token: CancellationToken, ref_count: Arc<AtomicUsize>, temporary: bool) -> Self {
        ref_count.fetch_add(1, Ordering::SeqCst);

        Self {
            tx,
            token,
            ref_count,
            temporary,
        }
    }

    pub fn cancel(&self) {
        self.token.cancel();
    }

    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    pub fn clone_temporary(&self) -> Self {
        ShutdownGuard::new(self.tx.clone(), self.token.clone(), self.ref_count.clone(), true)
    }

    pub fn spawn_task<T>(&self, task: T) -> JoinHandle<Option<T::Output>>
        where T: Future + Send + 'static, T::Output: Send + 'static {
        let guard = self.clone();
        guard.into_spawn_task(task)
    }

    pub fn into_spawn_task<T>(self, task: T) -> JoinHandle<Option<T::Output>>
        where T: Future + Send + 'static, T::Output: Send + 'static {
        let token = self.token.clone();
        tokio::spawn(async move {
            let output = select! {
                output = task => {
                    Some(output)
                }
                _ = token.cancelled() => {
                    None
                }
            };
            drop(self);
            output
        })
    }

    pub fn spawn_temporary<T>(&self, task: T) -> JoinHandle<Option<T::Output>>
        where T: Future + Send + 'static, T::Output: Send + 'static {
        let guard = self.clone_temporary();
        guard.into_spawn_task(task)
    }
}

pub struct Shutdown {
    name: &'static str,
    guard: ShutdownGuard,
    rx: broadcast::Receiver<()>,
    signal_rx: mpsc::Receiver<()>,
}

pub enum ShutdownError {
    Signal,
    Timeout,
}

impl Shutdown {
    pub fn new(name: &'static str) -> Self {
        let (tx, rx) = broadcast::channel(1);

        let token = CancellationToken::new();

        let (signal_tx, signal_rx) = mpsc::channel(4);
        tokio::spawn(async move {
            loop {
                let ctrl_c = tokio::signal::ctrl_c();
                let signal = async {
                    let mut os_signal =
                        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
                    os_signal.recv().await;
                    io::Result::Ok(())
                };

                select! {
                    _ = ctrl_c => {}
                    _ = signal => {}
                }

                let _ = signal_tx.send(()).await;
            }
        });

        Self {
            name,
            guard: ShutdownGuard::new(tx, token, Arc::new(AtomicUsize::new(0)), false),
            rx,
            signal_rx,
        }
    }

    pub fn spawn_task<T>(&self, task: T) -> JoinHandle<Option<T::Output>>
    where T: Future + Send + 'static,
          T::Output: Send + 'static {
        self.guard.spawn_task(task)
    }

    pub fn guard(&self) -> ShutdownGuard {
        self.guard.clone()
    }

    pub fn token(&self) -> CancellationToken {
        self.guard.token.clone()
    }

    pub fn is_canceled(&self) -> bool {
        self.guard.is_cancelled()
    }

    pub async fn wait_for_shutdown(mut self, timeout: Duration) -> Result<(), ShutdownError> {
        select! {
            _ = self.signal_rx.recv() => {
                // wait for a signal to start the cancellation process
                info!("Received signal, shutting down {}", self.name);
                self.guard.token.cancel();
            }
            _ = self.guard.token.cancelled() => {
                // Or some part of the system shut down
                warn!("{} shutting down", self.name);
            }
        }

        drop(self.guard);
        select! {
            _ = self.rx.recv() => {
                // everything has shutdown
                info!("{} shutdown complete", self.name);
                Ok(())
            }
            _ = self.signal_rx.recv() => {
                // we got another signal, shutdown immediately
                info!("Received signal, shutting down {} immediately", self.name);
                Err(ShutdownError::Signal)
            }
            _ = tokio::time::sleep(timeout) => {
                warn!("{} failed to shutdown after {} seconds", self.name, timeout.as_secs());
                Err(ShutdownError::Timeout)
            }
        }
    }
}
