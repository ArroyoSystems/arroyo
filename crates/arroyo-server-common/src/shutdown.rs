use axum::async_trait;
use futures::future::OptionFuture;
use std::future::Future;
use std::io;
use std::process::exit;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

static ERROR_CODE: AtomicI32 = AtomicI32::new(0);

pub struct ShutdownGuard {
    name: &'static str,
    tx: broadcast::Sender<()>,
    token: CancellationToken,
    ref_count: Arc<AtomicUsize>,
    temporary: bool,
}

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        if !self.temporary {
            self.token.cancel();
        }
        let count = self.ref_count.fetch_sub(1, Ordering::SeqCst);
        debug!("[{}] Dropping guard (count={})", self.name, count);
        if count == 1 {
            let _ = self.tx.send(());
        }
    }
}

impl ShutdownGuard {
    fn new(
        name: &'static str,
        tx: broadcast::Sender<()>,
        token: CancellationToken,
        ref_count: Arc<AtomicUsize>,
        temporary: bool,
    ) -> Self {
        ref_count.fetch_add(1, Ordering::SeqCst);

        Self {
            name,
            tx,
            token,
            ref_count,
            temporary,
        }
    }

    pub fn cancel(&self) {
        self.token.cancel();
    }

    pub fn child(&self, name: &'static str) -> Self {
        Self::new(
            name,
            self.tx.clone(),
            self.token.clone(),
            self.ref_count.clone(),
            self.temporary,
        )
    }

    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    pub fn clone_temporary(&self) -> Self {
        ShutdownGuard::new(
            "temp",
            self.tx.clone(),
            self.token.clone(),
            self.ref_count.clone(),
            true,
        )
    }

    pub fn spawn_task<F, T>(&self, name: &'static str, task: T) -> JoinHandle<Option<T::Output>>
    where
        F: Send + 'static,
        T: Future<Output = anyhow::Result<F>> + Send + 'static,
    {
        let guard = self.child(name);
        guard.into_spawn_task(task)
    }

    pub fn into_spawn_task<F, T>(self, task: T) -> JoinHandle<Option<T::Output>>
    where
        F: Send + 'static,
        T: Future<Output = anyhow::Result<F>> + Send + 'static,
    {
        let token = self.token.clone();
        tokio::spawn(async move {
            let output = select! {
                output = task => {
                    if let Err(e) = &output {
                        error!("{}", e);
                        ERROR_CODE.store(1, Ordering::Relaxed);
                    }
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

    pub fn spawn_temporary<F, T>(&self, task: T) -> JoinHandle<Option<T::Output>>
    where
        F: Send + 'static,
        T: Future<Output = anyhow::Result<F>> + Send + 'static,
    {
        let guard = self.clone_temporary();
        guard.into_spawn_task(task)
    }
}

pub struct Shutdown {
    name: &'static str,
    guard: ShutdownGuard,
    rx: broadcast::Receiver<()>,
    signal_rx: Option<mpsc::Receiver<()>>,
    handler: Option<Box<dyn ShutdownHandler + Send>>,
}

pub enum ShutdownError {
    Signal,
    Timeout,
    Err(i32),
}

#[async_trait]
pub trait ShutdownHandler {
    async fn shutdown(&self);
}

pub enum SignalBehavior {
    Handle,
    Ignore,
    None,
}

impl Shutdown {
    pub fn new(name: &'static str, signal_behavior: SignalBehavior) -> Self {
        let (tx, rx) = broadcast::channel(1);

        let token = CancellationToken::new();

        let signal_rx = if !matches!(signal_behavior, SignalBehavior::None) {
            let (signal_tx, signal_rx) = mpsc::channel(4);
            tokio::spawn(async move {
                loop {
                    let ctrl_c = tokio::signal::ctrl_c();
                    let signal = async {
                        let mut os_signal = tokio::signal::unix::signal(
                            tokio::signal::unix::SignalKind::terminate(),
                        )?;
                        os_signal.recv().await;
                        io::Result::Ok(())
                    };

                    select! {
                        _ = ctrl_c => {}
                        _ = signal => {}
                    }

                    if matches!(signal_behavior, SignalBehavior::Ignore) {
                        debug!("Ignoring signal");
                    } else {
                        let _ = signal_tx.send(()).await;
                    }
                }
            });
            Some(signal_rx)
        } else {
            None
        };

        Self {
            name,
            guard: ShutdownGuard::new("root", tx, token, Arc::new(AtomicUsize::new(0)), false),
            rx,
            signal_rx,
            handler: None,
        }
    }

    pub fn set_handler(&mut self, handler: Box<dyn ShutdownHandler + Send>) {
        self.handler = Some(handler);
    }

    pub fn spawn_task<F, T>(&self, name: &'static str, task: T) -> JoinHandle<Option<T::Output>>
    where
        F: Send + 'static,
        T: Future<Output = anyhow::Result<F>> + Send + 'static,
    {
        self.guard.spawn_task(name, task)
    }

    pub fn guard(&self, name: &'static str) -> ShutdownGuard {
        self.guard.child(name)
    }

    pub fn token(&self) -> CancellationToken {
        self.guard.token.clone()
    }

    pub fn is_canceled(&self) -> bool {
        self.guard.is_cancelled()
    }

    pub fn handle_shutdown(result: Result<(), ShutdownError>) {
        match result {
            Ok(_) => exit(0),
            Err(ShutdownError::Err(code)) => exit(code),
            Err(ShutdownError::Signal) => exit(128),
            Err(ShutdownError::Timeout) => exit(129),
        }
    }

    pub async fn wait_for_shutdown(mut self, timeout: Duration) -> Result<(), ShutdownError> {
        select! {
            Some(_) = OptionFuture::from(self.signal_rx.as_mut().map(|s| s.recv())) => {
                // wait for a signal to start the cancellation process
                info!("Received signal, shutting down {}", self.name);

                // call the handler if there is one
                if let Some(handler) = self.handler {
                    info!("Running shutdown handler");
                    handler.shutdown().await;
                }

                self.guard.token.cancel();
            }
            _ = self.guard.token.cancelled() => {
                // Or some part of the system shut down
                info!("{} shutting down", self.name);
            }
        }

        drop(self.guard);
        select! {
            _ = self.rx.recv() => {
                // everything has shutdown
                info!("{} shutdown complete", self.name);
                let code = ERROR_CODE.load(Ordering::Relaxed);
                if code == 0 {
                    Ok(())
                } else {
                    Err(ShutdownError::Err(code))
                }
            }
            Some(_) = OptionFuture::from(self.signal_rx.as_mut().map(|s| s.recv())) => {
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
