use log::info;

use super::ThreadPool;
use crate::Result;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

/// naive thread pool
pub struct SharedQueueThreadPool {
    threads: u32,
    sender: mpsc::Sender<Message>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<SharedQueueThreadPool> {
        assert!(threads > 0);

        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));

        for id in 0..threads {
            Worker::new(id, Arc::clone(&receiver));
        }

        Ok(SharedQueueThreadPool { threads, sender })
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

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        for _ in 0..self.threads {
            self.sender.send(Message::Terminate).unwrap();
        }

        info!("Shutting down all workers.")
    }
}

type Job = Box<dyn FnOnce() + Send + 'static>;

enum Message {
    NewJob(Job),
    Terminate,
}

#[derive(Clone)]
struct Worker {
    id: u32,
    receiver: Arc<Mutex<mpsc::Receiver<Message>>>,
}

impl Worker {
    fn new(id: u32, receiver: Arc<Mutex<mpsc::Receiver<Message>>>){
        let worker = Worker {
            id: id,
            receiver: receiver.clone(),
        };
        take_job(worker);
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if thread::panicking() {
            let new_worker = self.clone();
            take_job(new_worker);
        }
    }
}

fn take_job(worker: Worker) {
    thread::spawn(move || loop {
        let message = worker.receiver.lock().unwrap().recv().unwrap();

        match message {
            Message::NewJob(job) => {
                info!("Worker {} got a job; executing.", worker.id);
                job();
            }
            Message::Terminate => {
                info!("Worker {} was told to terminate.", worker.id);
                break;
            }
        }
    });
}
