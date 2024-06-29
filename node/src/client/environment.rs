//! This crate aims to provide a common set of tools that can be used to create a "environment" to
//! run Zgs services. This allows for the unification of creating tokio runtimes, etc.
//!
//! The idea is that the main thread creates an `Environment`, which is then used to spawn a
//! `Context` which can be handed to any service that wishes to start async tasks.

use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::{future, StreamExt};
use std::sync::Arc;
use task_executor::{ShutdownReason, TaskExecutor};
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

#[cfg(target_family = "unix")]
use {
    futures::Future,
    std::{pin::Pin, task::Context, task::Poll},
    tokio::signal::unix::{signal, Signal, SignalKind},
};

#[cfg(not(target_family = "unix"))]
use {futures::channel::oneshot, std::cell::RefCell};

/// The maximum time in seconds the client will wait for all internal tasks to shutdown.
const MAXIMUM_SHUTDOWN_TIME: u64 = 15;

/// Builds an `Environment`.
pub struct EnvironmentBuilder {
    runtime: Option<Arc<Runtime>>,
}

impl EnvironmentBuilder {
    pub fn new() -> Self {
        Self { runtime: None }
    }
}

impl EnvironmentBuilder {
    /// Specifies that a multi-threaded tokio runtime should be used. Ideal for production uses.
    ///
    /// The `Runtime` used is just the standard tokio runtime.
    pub fn multi_threaded_tokio_runtime(mut self) -> Result<Self, String> {
        self.runtime = Some(Arc::new(
            RuntimeBuilder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| format!("Failed to start runtime: {:?}", e))?,
        ));
        Ok(self)
    }

    /// Consumes the builder, returning an `Environment`.
    pub fn build(self) -> Result<Environment, String> {
        let (signal, exit) = exit_future::signal();
        let (signal_tx, signal_rx) = channel(1);
        Ok(Environment {
            runtime: self
                .runtime
                .ok_or("Cannot build environment without runtime")?,
            signal_tx,
            signal_rx: Some(signal_rx),
            signal: Some(signal),
            exit,
        })
    }
}

/// An execution context that can be used by a service.
///
/// Distinct from an `Environment` because a `Context` is not able to give a mutable reference to a
/// `Runtime`, instead it only has access to a `Runtime`.
#[derive(Clone)]
pub struct RuntimeContext {
    pub executor: TaskExecutor,
}

/// An environment where Zgs services can run.
pub struct Environment {
    runtime: Arc<Runtime>,

    /// Receiver side of an internal shutdown signal.
    signal_rx: Option<Receiver<ShutdownReason>>,

    /// Sender to request shutting down.
    signal_tx: Sender<ShutdownReason>,
    signal: Option<exit_future::Signal>,
    exit: exit_future::Exit,
}

impl Environment {
    /// Returns a mutable reference to the `tokio` runtime.
    ///
    /// Useful in the rare scenarios where it's necessary to block the current thread until a task
    /// is finished (e.g., during testing).
    pub fn runtime(&self) -> &Arc<Runtime> {
        &self.runtime
    }

    /// Returns a `Context`.
    pub fn core_context(&mut self) -> RuntimeContext {
        RuntimeContext {
            executor: TaskExecutor::new(
                self.runtime().handle().clone(),
                self.exit.clone(),
                self.signal_tx.clone(),
            ),
        }
    }

    /// Block the current thread until a shutdown signal is received.
    ///
    /// This can be either the user Ctrl-C'ing or a task requesting to shutdown.
    #[cfg(target_family = "unix")]
    pub fn block_until_shutdown_requested(&mut self) -> Result<ShutdownReason, String> {
        // future of a task requesting to shutdown
        let mut rx = self
            .signal_rx
            .take()
            .ok_or("Inner shutdown already received")?;
        let inner_shutdown =
            async move { rx.next().await.ok_or("Internal shutdown channel exhausted") };
        futures::pin_mut!(inner_shutdown);
        let shutdown = async {
            let mut handles = vec![];

            // setup for handling SIGTERM
            match signal(SignalKind::terminate()) {
                Ok(terminate_stream) => {
                    let terminate = SignalFuture::new(terminate_stream, "Received SIGTERM");
                    handles.push(terminate);
                }
                Err(e) => error!(error = %e, "Could not register SIGTERM handler"),
            };

            // setup for handling SIGINT
            match signal(SignalKind::interrupt()) {
                Ok(interrupt_stream) => {
                    let interrupt = SignalFuture::new(interrupt_stream, "Received SIGINT");
                    handles.push(interrupt);
                }
                Err(e) => error!(error = %e, "Could not register SIGINT handler"),
            }

            // setup for handling a SIGHUP
            match signal(SignalKind::hangup()) {
                Ok(hup_stream) => {
                    let hup = SignalFuture::new(hup_stream, "Received SIGHUP");
                    handles.push(hup);
                }
                Err(e) => error!(error = %e, "Could not register SIGHUP handler"),
            }

            future::select(inner_shutdown, future::select_all(handles.into_iter())).await
        };

        match self.runtime().block_on(shutdown) {
            future::Either::Left((Ok(reason), _)) => {
                info!(reason = reason.message(), "Internal shutdown received");
                Ok(reason)
            }
            future::Either::Left((Err(e), _)) => Err(e.into()),
            future::Either::Right(((res, _, _), _)) => {
                res.ok_or_else(|| "Handler channel closed".to_string())
            }
        }
    }

    /// Block the current thread until a shutdown signal is received.
    ///
    /// This can be either the user Ctrl-C'ing or a task requesting to shutdown.
    #[cfg(not(target_family = "unix"))]
    pub fn block_until_shutdown_requested(&mut self) -> Result<ShutdownReason, String> {
        // future of a task requesting to shutdown
        let mut rx = self
            .signal_rx
            .take()
            .ok_or("Inner shutdown already received")?;
        let inner_shutdown =
            async move { rx.next().await.ok_or("Internal shutdown channel exhausted") };
        futures::pin_mut!(inner_shutdown);

        // setup for handling a Ctrl-C
        let (ctrlc_send, ctrlc_oneshot) = oneshot::channel();
        let ctrlc_send_c = RefCell::new(Some(ctrlc_send));
        ctrlc::set_handler(move || {
            if let Some(ctrlc_send) = ctrlc_send_c.try_borrow_mut().unwrap().take() {
                if let Err(e) = ctrlc_send.send(()) {
                    error!("Error sending ctrl-c message: {:?}", e);
                }
            }
        })
        .map_err(|e| format!("Could not set ctrlc handler: {:?}", e))?;

        // Block this thread until a shutdown signal is received.
        match self
            .runtime()
            .block_on(future::select(inner_shutdown, ctrlc_oneshot))
        {
            future::Either::Left((Ok(reason), _)) => {
                info!(reasion = reason.message(), "Internal shutdown received");
                Ok(reason)
            }
            future::Either::Left((Err(e), _)) => Err(e.into()),
            future::Either::Right((x, _)) => x
                .map(|()| ShutdownReason::Success("Received Ctrl+C"))
                .map_err(|e| format!("Ctrlc oneshot failed: {}", e)),
        }
    }

    /// Shutdown the `tokio` runtime when all tasks are idle.
    pub fn shutdown_on_idle(self) {
        match Arc::try_unwrap(self.runtime) {
            Ok(runtime) => {
                runtime.shutdown_timeout(std::time::Duration::from_secs(MAXIMUM_SHUTDOWN_TIME))
            }
            Err(e) => warn!(
                error = ?e,
                "Failed to obtain runtime access to shutdown gracefully",
            ),
        }
    }

    /// Fire exit signal which shuts down all spawned services
    pub fn fire_signal(&mut self) {
        if let Some(signal) = self.signal.take() {
            let _ = signal.fire();
        }
    }
}

#[cfg(target_family = "unix")]
struct SignalFuture {
    signal: Signal,
    message: &'static str,
}

#[cfg(target_family = "unix")]
impl SignalFuture {
    pub fn new(signal: Signal, message: &'static str) -> SignalFuture {
        SignalFuture { signal, message }
    }
}

#[cfg(target_family = "unix")]
impl Future for SignalFuture {
    type Output = Option<ShutdownReason>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.signal.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(_)) => Poll::Ready(Some(ShutdownReason::Success(self.message))),
            Poll::Ready(None) => Poll::Ready(None),
        }
    }
}
