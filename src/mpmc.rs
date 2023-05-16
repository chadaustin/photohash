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

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.state.lock().unwrap().tx_count += 1;
        Sender {
            state: self.state.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();
        assert!(state.tx_count >= 1);
        state.tx_count -= 1;
        if state.tx_count == 0 {
            for waker in std::mem::take(&mut state.rx_wakers) {
                waker.wake();
            }
        }
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Send<'a, T> {
    sender: &'a Sender<T>,
    value: Option<T>,
}

impl<'a, T> Unpin for Send<'a, T> {}

#[derive(Debug, PartialEq, Eq)]
pub struct SendError<T>(T);

impl<'a, T> Future for Send<'a, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.sender.state.lock().unwrap();
        if state.rx_count == 0 {
            assert!(state.queue.is_empty());
            return Poll::Ready(Err(SendError(self.value.take().unwrap())));
        }

        state.queue.push_back(self.value.take().unwrap());
        // TODO: How many do we actually need to wake up?
        let waker = state.rx_wakers.pop();
        drop(state);

        if let Some(waker) = waker {
            waker.wake();
        }

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

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.state.lock().unwrap().rx_count += 1;
        Receiver {
            state: self.state.clone(),
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();
        assert!(state.rx_count >= 1);
        state.rx_count -= 1;
        state.queue.clear();
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Recv<'a, T> {
    receiver: &'a Receiver<T>,
}

impl<'a, T> Unpin for Recv<'a, T> {}

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
    use futures::task::SpawnExt;

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

    #[test]
    fn recv_wakes_when_sender_sends() {
        let (tx, rx) = mpmc::unbounded();

        let mut pool = LocalPool::new();
        let spawner = pool.spawner();
        spawner.spawn(async move {
            assert_eq!(Some(()), rx.recv().await);
        });

        spawner.spawn(async move {
            tx.send(()).await.unwrap();
        });

        pool.run();
    }

    #[test]
    fn recv_wakes_when_sender_drops() {
        let (tx, rx) = mpmc::unbounded();

        let mut pool = LocalPool::new();
        let spawner = pool.spawner();
        spawner.spawn(async move {
            assert_eq!(None as Option<()>, rx.recv().await);
        });

        spawner.spawn(async move {
            drop(tx);
        });

        pool.run();
    }

    #[test]
    fn send_fails_when_receiver_drops() {
        let mut pool = LocalPool::new();
        pool.run_until(async move {
            let (tx, rx) = mpmc::unbounded();
            drop(rx);
            assert_eq!(Err(mpmc::SendError(())), tx.send(()).await);
        })
    }

    #[test]
    fn two_receivers_and_two_senders() {
        let mut pool = LocalPool::new();
        pool.run_until(async move {
            let (tx1, rx1) = mpmc::unbounded();
            let tx2 = tx1.clone();
            let rx2 = rx1.clone();
            tx1.send(1).await;
            tx2.send(2).await;
            assert_eq!(Some(1), rx1.recv().await);
            assert_eq!(Some(2), rx2.recv().await);
        })
    }
}
