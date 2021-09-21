use super::ThreadPool;
use crate::Result;
use std::thread;

/// naive thread pool
///
/// ```
/// use kvs::thread_pool::{NaiveThreadPool, ThreadPool};
///
/// let pool = NaiveThreadPool::new(5).unwrap();
///
/// pool.spawn(|| {
///     let a = 1 + 2;
///     println!("{}", a);
/// });
///
/// ```
///
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
