use tokio::sync::Notify;
use std::sync::Arc;
use async_std::sync::{Mutex, Condvar};
use std::collections::VecDeque;
use std::sync::mpsc::SendError;
use std::sync::atomic::AtomicUsize;

struct State<T> {
    queue: VecDeque<T>,
    tx_count: usize,
    rx_count: usize,
}

struct Queue<T> {
    state: Mutex<State<T>>,
    has_item: Condvar,
}

pub struct Sender<T> {
    q: Arc<Queue<T>>,
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let state = self.q.state.lock();
        state.tx_count -= 1;
    }
}

impl<T> Sender<T> {
    pub async fn push(&self, value: T) -> Result<(), SendError<T>> {
        let mut v = self.q.queue.lock().await;
        v.push_back(value);
        Ok(())
    }
}

pub struct Receiver<T> {
    q: Arc<Queue<T>>,
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let state = self.q.state.lock();
        state.rx_count -= 1;
    }
}

impl<T> Receiver<T> {
    pub async fn pop(&self) -> Option<T> {
        let mut v = self.q.queue.lock().await;
        Some(v.pop_front().unwrap())
    }
}

pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let q = Arc::new(Queue {
        queue: Mutex::new(VecDeque::new()),
        has_item: Condvar::new(),
        tx_count: AtomicUsize::new(1),
        rx_count: AtomicUsize::new(1),
    });
    (Sender{q: q.clone()}, Receiver{q})
}

#[cfg(test)]
mod tests {
    use crate::mpmc;

    #[tokio::test]
    async fn push_and_pop() {
        let (tx, rx) = mpmc::unbounded();
        tx.push(10).await.unwrap();
        assert_eq!(10, rx.pop().await);
    }

    #[tokio::test]
    async fn pop_returns_None_if_sender_dropped() {
        let (tx, rx) = mpmc::unbounded();
        drop(tx);
        assert_eq!(None, rx.pop().await);
    }
}
