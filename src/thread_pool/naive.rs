use log::info;

use crate::thread_pool::ThreadPool;
use crate::Result;
use std::sync::{mpsc, Arc, Mutex};
use std::{thread, usize};

/// naive thread pool
pub struct NaiveThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Message>,
}

impl ThreadPool for NaiveThreadPool {
    fn new(threads: u32) -> Result<NaiveThreadPool> {
        assert!(threads > 0);

        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(threads as usize);

        for id in 0..threads {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }

        Ok(NaiveThreadPool { workers, sender })
    }

    /// spawn
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(job);

        self.sender.send(Message::NewJob(job)).unwrap();
    }
}

impl Drop for NaiveThreadPool {
    fn drop(&mut self) {
        for _ in &self.workers {
            self.sender.send(Message::Terminate).unwrap();
        }

        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
        info!("Shutting down all workers.")
    }
}

type Job = Box<dyn FnOnce() + Send + 'static>;

enum Message {
    NewJob(Job),
    Terminate,
}

struct Worker {
    #[allow(unused)]
    id: u32,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: u32, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Worker {
        let thread = thread::spawn(move || loop {
            let message = receiver.lock().unwrap().recv().unwrap();

            match message {
                Message::NewJob(job) => {
                    info!("Worker {} got a job; executing.", id);
                    job();
                }
                Message::Terminate => {
                    info!("Worker {} was told to terminate.", id);
                    break;
                }
            }
        });

        Worker {
            id,
            thread: Some(thread),
        }
    }
}
