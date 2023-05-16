use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct State<T> {
    queue: VecDeque<T>,
    tx_count: usize,
    rx_count: usize,
    rx_wakers: Vec<Waker>,
}

pub struct Sender<T> {
    state: Arc<Mutex<State<T>>>,
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();
        state.tx_count -= 1;
        if state.tx_count == 0 {
            // TODO: wake all the recv waiters
        }
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Send<'a, T> {
    sender: &'a Sender<T>,
    value: Option<T>,
}

impl<'a, T> Unpin for Send<'a, T> {}

#[derive(Debug)]
pub struct SendError();

impl<'a, T> Future for Send<'a, T> {
    type Output = Result<(), SendError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.sender.state.lock().unwrap();
        state.queue.push_back(self.value.take().unwrap());
        Poll::Ready(Ok(()))
    }
}

impl<T> Sender<T> {
    pub fn send(&self, value: T) -> Send<'_, T> {
        Send {
            sender: self,
            value: Some(value),
        }
    }
}

pub struct Receiver<T> {
    state: Arc<Mutex<State<T>>>,
}

/*
impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let state = self.q.state.lock();
        state.rx_count -= 1;
    }
}
*/

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Recv<'a, T> {
    receiver: &'a Receiver<T>,
}

impl<'a, T> Unpin for Recv<'a, T> {}

#[derive(Debug)]
pub struct RecvError();

impl<'a, T> Future for Recv<'a, T> {
    type Output = Option<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.receiver.state.lock().unwrap();
        match state.queue.pop_front() {
            Some(value) => Poll::Ready(Some(value)),
            None => {
                if state.tx_count == 0 {
                    Poll::Ready(None)
                } else {
                    state.rx_wakers.push(cx.waker().clone());
                    Poll::Pending
                }
            }
        }
    }
}

impl<T> Receiver<T> {
    pub fn recv(&self) -> Recv<'_, T> {
        Recv { receiver: self }
    }
}

pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let state = Arc::new(Mutex::new(State {
        queue: VecDeque::new(),
        tx_count: 1,
        rx_count: 1,
        rx_wakers: Vec::new(),
    }));
    (
        Sender {
            state: state.clone(),
        },
        Receiver { state },
    )
}

#[cfg(test)]
mod tests {
    use crate::mpmc;
    use futures::executor::LocalPool;

    #[test]
    fn send_and_recv() {
        let mut pool = LocalPool::new();
        pool.run_until(async move {
            let (tx, rx) = mpmc::unbounded();
            tx.send(10).await.unwrap();
            assert_eq!(Some(10), rx.recv().await);
        })
    }

    #[test]
    fn recv_returns_none_if_sender_dropped() {
        let mut pool = LocalPool::new();
        pool.run_until(async move {
            let (tx, rx) = mpmc::unbounded();
            drop(tx);
            assert_eq!(None as Option<()>, rx.recv().await);
        })
    }

    #[test]
    fn recv_returns_value_if_sender_sent_before_dropping() {
        let mut pool = LocalPool::new();
        pool.run_until(async move {
            let (tx, rx) = mpmc::unbounded();
            tx.send(10).await.unwrap();
            drop(tx);
            assert_eq!(Some(10), rx.recv().await);
        })
    }
}
