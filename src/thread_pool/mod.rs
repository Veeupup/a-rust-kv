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

pub use self::rayon::RayonThreadPool;
pub use naive::NaiveThreadPool;
pub use shared_queue::SharedQueueThreadPool;

mod naive;
mod rayon;
mod shared_queue;
