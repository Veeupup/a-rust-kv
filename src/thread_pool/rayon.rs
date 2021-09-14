use super::ThreadPool as ThreadPoolTrait;
use crate::Result;
use rayon::{ThreadPool, ThreadPoolBuilder};

/// Rayon Thread Pool
pub struct RayonThreadPool {
    pool: ThreadPool,
}

impl ThreadPoolTrait for RayonThreadPool {
    /// new
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        let pool_builder = ThreadPoolBuilder::new().num_threads(threads as usize);
        let pool = pool_builder.build().unwrap();
        Ok(RayonThreadPool { pool: pool })
    }

    /// spawn
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.pool.spawn(job)
    }
}
