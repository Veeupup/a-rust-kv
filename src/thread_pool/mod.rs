use crate::Result;

/// threadpool trait
pub trait ThreadPool {
    /// new
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;

    /// spawn
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

pub use sharedqueuethreadpool::SharedQueueThreadPool;
pub use naive::NaiveThreadPool;

mod sharedqueuethreadpool;
mod naive;
