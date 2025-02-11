use task_executor::TaskExecutor;
use tracing_log::AsLog;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

const LOG_RELOAD_PERIOD_SEC: u64 = 30;

pub fn configure(log_level_file: &str, log_directory: &str, executor: TaskExecutor) {
    let file_appender = tracing_appender::rolling::daily(log_directory, "zgs.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let level_file = log_level_file.trim_end().to_string();
    // load config synchronously
    let mut config = std::fs::read_to_string(&level_file)
        .unwrap_or_default()
        .trim_end()
        .to_string();
    let filter = EnvFilter::try_new(config.clone()).expect("invalid log level");
    let (filter, reload_handle) = tracing_subscriber::reload::Layer::new(filter);

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_ansi(false)
        .compact()
        .with_filter(filter);
    // .with_file(true)
    // .with_line_number(true)
    // .with_thread_names(true)
    let subscriber = tracing_subscriber::registry().with(fmt_layer);
    #[cfg(feature = "tokio-console")]
    {
        subscriber.with(console_subscriber::spawn()).init();
    }
    #[cfg(not(feature = "tokio-console"))]
    {
        subscriber.init();
    }

    // periodically check for config changes
    executor.spawn(
        async move {
            // move the log writer guard so that it's not dropped.
            let _moved_guard = guard;
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(LOG_RELOAD_PERIOD_SEC));

            loop {
                interval.tick().await;

                let new_config = match tokio::fs::read_to_string(&level_file).await {
                    Ok(c) => {
                        let nc = c.trim_end().to_string();
                        if nc == config {
                            continue;
                        } else {
                            nc
                        }
                    }
                    Err(e) => {
                        println!("Unable to read log file {}: {:?}", level_file, e);
                        continue;
                    }
                };

                println!("Updating log config to {:?}", new_config);

                match reload_handle.reload(&new_config) {
                    Ok(()) => {
                        rust_log::set_max_level(tracing_core::LevelFilter::current().as_log());
                        config = new_config
                    }
                    Err(e) => {
                        println!("Failed to load new config: {:?}", e);
                    }
                }
            }
        },
        "log_reload",
    );
}
