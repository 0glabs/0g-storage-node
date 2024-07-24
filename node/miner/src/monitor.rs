use std::time::Duration;

use task_executor::TaskExecutor;
use tokio::time::sleep;

use super::metrics;

pub struct Monitor {
    period: Duration,
}

impl Monitor {
    pub fn spawn(executor: TaskExecutor, period: Duration) {
        let monitor = Monitor { period };
        executor.spawn(
            async move { Box::pin(monitor.start()).await },
            "pora_master",
        );
    }

    async fn start(&self) {
        loop {
            info!("Mine iterations statistics: {}", metrics::report());
            let _ = sleep(self.period).await;
        }
    }
}
