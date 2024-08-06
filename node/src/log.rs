use task_executor::TaskExecutor;
use tracing::Level;
use tracing_subscriber::EnvFilter;

const LOG_RELOAD_PERIOD_SEC: u64 = 30;

pub fn configure(log_level_file: &str, log_directory: &str, executor: TaskExecutor) {
    let file_appender = tracing_appender::rolling::daily(log_directory, "zgs.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
    let builder = tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .with_env_filter(EnvFilter::default())
        .with_writer(non_blocking)
        .with_ansi(false)
        // .with_file(true)
        // .with_line_number(true)
        // .with_thread_names(true)
        .with_filter_reloading();

    let handle = builder.reload_handle();
    builder.init();

    let level_file = log_level_file.trim_end().to_string();

    // load config synchronously
    let mut config = std::fs::read_to_string(&level_file)
        .unwrap_or_default()
        .trim_end()
        .to_string();
    let _ = handle.reload(&config);

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

                match handle.reload(&new_config) {
                    Ok(()) => config = new_config,
                    Err(e) => {
                        println!("Failed to load new config: {:?}", e);
                    }
                }
            }
        },
        "log_reload",
    );
}
