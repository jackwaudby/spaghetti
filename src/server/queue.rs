use std::sync::atomic::{AtomicBool, Ordering};
use std::{sync, thread, time};
use tracing::info;

// TODO: Add crossbeam queue.
// TODO: Link with handlers and transaction manager, messages and shutdown.
// TODO: Ensure queue is emptied before notification of shutdown is sent to tm.

pub struct WorkQueue {
    // Thread that runs the queue on.
    handle: Option<thread::JoinHandle<()>>,

    // Indicates if shutdown notification has been received.
    shutdown: sync::Arc<AtomicBool>,

    // Notify channel.
    channel: sync::Arc<sync::Mutex<std::sync::mpsc::Receiver<()>>>,
}

impl WorkQueue {
    pub fn new(channel: std::sync::mpsc::Receiver<()>) -> WorkQueue {
        WorkQueue {
            handle: None,
            shutdown: sync::Arc::new(AtomicBool::new(false)),
            channel: sync::Arc::new(sync::Mutex::new(channel)),
        }
    }

    pub fn start<F>(&mut self, fun: F)
    where
        F: 'static + Send + FnMut() -> (),
    {
        let shutdown = self.shutdown.clone();
        let channel = self.channel.clone();

        self.handle = Some(thread::spawn(move || {
            let mut fun = fun;
            while !shutdown.load(Ordering::SeqCst) {
                // Check if shutdown received.
                let res = channel.lock().unwrap().try_recv();
                match res {
                    Ok(_) => {
                        info!("Shutdown notification received");
                        shutdown.store(true, Ordering::SeqCst);
                        break;
                    }
                    _ => {}
                }

                fun();
                thread::sleep(time::Duration::from_millis(10));
            }
        }));
    }

    pub fn stop(&mut self) {
        self.handle
            .take()
            .expect("Called stop on non-running thread")
            .join()
            .expect("Could not join spawned thread");
        info!(" WorkQueue Stopped");
    }
}

impl Drop for WorkQueue {
    fn drop(&mut self) {
        info!("Call join");
        self.handle
            .take()
            .expect("Called stop on non-running thread")
            .join()
            .expect("Could not join spawned thread");

        info!("Work queue stopped");
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::sync::Once;
//     use std::{thread, time};
//     use tracing::{info, Level};
//     use tracing_subscriber::FmtSubscriber;

//     static LOG: Once = Once::new();

//     fn logging() {
//         LOG.call_once(|| {
//             let subscriber = FmtSubscriber::builder()
//                 .with_max_level(Level::DEBUG)
//                 .finish();
//             tracing::subscriber::set_global_default(subscriber)
//                 .expect("setting default subscriber failed");
//         });
//     }

//     #[test]
//     fn work_queue() {
//         logging();

//         let (tx, rx) = std::sync::mpsc::channel();
//         let mut wq = WorkQueue::new(rx);
//         {
//             wq.start(|| info!("Hello, World!"));
//         }

//         thread::sleep(time::Duration::from_millis(100));

//         info!("Sending shutdown notification!");
//         tx.send()).unwrap();
//         assert_eq!(true, true);
//     }
// }
