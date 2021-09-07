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

pub use sharedqueue::SharedQueueThreadPool;
pub use naive::NaiveThreadPool;
pub use self::rayon::RayonThreadPool;

mod sharedqueue;
mod naive;
mod rayon;
