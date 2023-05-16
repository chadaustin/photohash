use std::sync::{Arc, Mutex};

//use async_std::sync::{Mutex, Condvar};
use tokio::sync::Notify;

use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc::SendError;

struct State<T> {
    queue: VecDeque<T>,
    tx_count: usize,
    rx_count: usize,
}

struct Queue<T> {
    state: Mutex<State<T>>,
    not_empty_or_no_sender: Notify,
}

pub struct Sender<T> {
    q: Arc<Queue<T>>,
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let state = self.q.state.lock();
        state.tx_count -= 1;
        if state.tx_count == 0 {
            // TODO:
            self.not_empty_or_no_sender.notify_waiters();
        }
    }
}

impl<T> Sender<T> {
    pub async fn push(&self, value: T) -> Result<(), SendError<T>> {
        let mut state = self.q.state.lock().unwrap();
        let was_empty = state.queue.is_empty();
        state.queue.push_back(value);
        drop(state);

        if was_empty {
            self.q.not_empty.notify_one();
        }

        Ok(())
    }
}

pub struct Receiver<T> {
    q: Arc<Queue<T>>,
}

/*
impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let state = self.q.state.lock();
        state.rx_count -= 1;
    }
}
*/

impl<T> Receiver<T> {
    pub async fn pop(&self) -> Option<T> {
        loop {
            let mut state = self.q.state.lock().unwrap();
            match state.queue.pop_front() {
                Some(value) => return Some(value),
                None => (),
            }
            drop(state);
            self.q.not_empty.notified().await;
        }
    }
}

pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let q = Arc::new(Queue {
        state: Mutex::new(State {
            queue: VecDeque::new(),
            tx_count: 1,
            rx_count: 1,
        }),
        not_empty: Notify::new(),
    });
    (Sender { q: q.clone() }, Receiver { q })
}

#[cfg(test)]
mod tests {
    use crate::mpmc;

    #[tokio::test]
    async fn push_and_pop() {
        let (tx, rx) = mpmc::unbounded();
        tx.push(10).await.unwrap();
        assert_eq!(Some(10), rx.pop().await);
    }

    #[tokio::test]
    async fn pop_returns_none_if_sender_dropped() {
        let (tx, rx) = mpmc::unbounded();
        drop(tx);
        assert_eq!(None as Option<()>, rx.pop().await);
    }
}
