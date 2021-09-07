use super::ThreadPool;
use crate::Result;
use std::thread;

/// naive thread pool
pub struct NaiveThreadPool {}

impl ThreadPool for NaiveThreadPool {
    /// new
    fn new(_: u32) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(NaiveThreadPool {})
    }

    /// spawn
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(|| {
            job();
        });
    }
}
