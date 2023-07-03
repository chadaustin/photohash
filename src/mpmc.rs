use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

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

#[derive(Debug, PartialEq, Eq)]
pub struct SendError<T>(T);

impl<T> Sender<T> {
    pub fn batch(self, capacity: usize) -> BatchSender<T> {
        BatchSender {
            sender: self,
            capacity,
            buffer: Vec::with_capacity(capacity),
        }
    }

    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let mut state = self.state.lock().unwrap();
        if state.rx_count == 0 {
            assert!(state.queue.is_empty());
            return Err(SendError(value));
        }

        state.queue.push_back(value);
        // TODO: How many do we actually need to wake up?
        let waker = state.rx_wakers.pop();
        drop(state);

        if let Some(waker) = waker {
            waker.wake();
        }

        Ok(())
    }

    pub fn send_many<I: Into<Vec<T>>>(&self, values: I) -> Result<Vec<T>, SendError<Vec<T>>> {
        // This iterator might be expensive. Evaluate it before the lock is held.
        let mut values: Vec<_> = values.into();

        let mut state = self.state.lock().unwrap();
        if state.rx_count == 0 {
            assert!(state.queue.is_empty());
            return Err(SendError(values));
        }

        state.queue.extend(values.drain(..));
        // TODO: How many do we actually need to wake up? One per added value?
        let wakers = std::mem::take(&mut state.rx_wakers);
        drop(state);

        for waker in wakers {
            waker.wake();
        }

        Ok(values)
    }
}

pub struct BatchSender<T> {
    sender: Sender<T>,
    capacity: usize,
    buffer: Vec<T>,
}

impl<T> Drop for BatchSender<T> {
    fn drop(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        // Nothing to do if receiver dropped.
        _ = self.sender.send_many(std::mem::take(&mut self.buffer));
    }
}

impl<T> BatchSender<T> {
    pub fn send(&mut self, value: T) -> Result<(), SendError<()>> {
        self.buffer.push(value);
        // TODO: consider using the full capacity if Vec overallocated.
        if self.buffer.len() == self.capacity {
            match self.sender.send_many(std::mem::take(&mut self.buffer)) {
                Ok(drained_vec) => {
                    self.buffer = drained_vec;
                }
                Err(_) => {
                    return Err(SendError(()));
                }
            }
        }
        Ok(())
    }

    pub fn send_many<I: Into<Vec<T>>>(&mut self, values: I) -> Result<(), SendError<()>> {
        for value in values.into() {
            self.send(value)?;
        }
        Ok(())
    }

    // TODO: add a drain method?
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
        if state.rx_count == 0 {
            state.queue.clear();
        }
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Recv<'a, T> {
    receiver: &'a Receiver<T>,
}

impl<'a, T> Unpin for Recv<'a, T> {}

impl<'a, T> Future for Recv<'a, T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
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

#[must_use = "futures do nothing unless you .await or poll them"]
pub struct RecvMany<'a, T> {
    receiver: &'a Receiver<T>,
    element_limit: usize,
}

impl<'a, T> Unpin for RecvMany<'a, T> {}

impl<'a, T> Future for RecvMany<'a, T> {
    type Output = Vec<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.receiver.state.lock().unwrap();
        let q = &mut state.queue;
        if q.is_empty() {
            if state.tx_count == 0 {
                Poll::Ready(Vec::new())
            } else {
                state.rx_wakers.push(cx.waker().clone());
                Poll::Pending
            }
        } else if q.len() <= self.element_limit {
            Poll::Ready(Vec::from(std::mem::take(q)))
        } else {
            let drain = q.drain(..self.element_limit);
            Poll::Ready(drain.collect())
        }
    }
}

impl<T> Receiver<T> {
    pub fn recv(&self) -> Recv<'_, T> {
        Recv { receiver: self }
    }

    pub fn recv_many(&self, element_limit: usize) -> RecvMany<'_, T> {
        RecvMany {
            receiver: self,
            element_limit,
        }
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
    use std::sync::Arc;
    use std::sync::Mutex;

    #[test]
    fn send_and_recv() {
        let mut pool = LocalPool::new();
        pool.run_until(async move {
            let (tx, rx) = mpmc::unbounded();
            tx.send(10).unwrap();
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
            tx.send(10).unwrap();
            drop(tx);
            assert_eq!(Some(10), rx.recv().await);
        })
    }

    #[test]
    fn recv_wakes_when_sender_sends() {
        let (tx, rx) = mpmc::unbounded();

        let mut pool = LocalPool::new();
        let spawner = pool.spawner();
        spawner
            .spawn(async move {
                assert_eq!(Some(()), rx.recv().await);
            })
            .unwrap();

        spawner
            .spawn(async move {
                tx.send(()).unwrap();
            })
            .unwrap();

        pool.run();
    }

    #[test]
    fn recv_wakes_when_sender_drops() {
        let (tx, rx) = mpmc::unbounded();

        let mut pool = LocalPool::new();
        let spawner = pool.spawner();
        spawner
            .spawn(async move {
                assert_eq!(None as Option<()>, rx.recv().await);
            })
            .unwrap();

        spawner
            .spawn(async move {
                drop(tx);
            })
            .unwrap();

        pool.run();
    }

    #[test]
    fn send_fails_when_receiver_drops() {
        let mut pool = LocalPool::new();
        pool.run_until(async move {
            let (tx, rx) = mpmc::unbounded();
            drop(rx);
            assert_eq!(Err(mpmc::SendError(())), tx.send(()));
        })
    }

    #[test]
    fn two_receivers_and_two_senders() {
        let mut pool = LocalPool::new();
        pool.run_until(async move {
            let (tx1, rx1) = mpmc::unbounded();
            let tx2 = tx1.clone();
            let rx2 = rx1.clone();
            tx1.send(1).unwrap();
            tx2.send(2).unwrap();
            assert_eq!(Some(1), rx1.recv().await);
            assert_eq!(Some(2), rx2.recv().await);
        })
    }

    #[test]
    fn two_reads_from_one_push() {
        let mut pool = LocalPool::new();
        let spawner = pool.spawner();

        let (tx, rx1) = mpmc::unbounded();
        let rx2 = rx1.clone();

        spawner
            .spawn(async move {
                assert_eq!(Some(10), rx1.recv().await);
            })
            .unwrap();
        spawner
            .spawn(async move {
                assert_eq!(Some(20), rx2.recv().await);
            })
            .unwrap();
        tx.send_many([10, 20]).unwrap();

        pool.run()
    }

    #[test]
    fn push_many_wakes_both() {
        let mut pool = LocalPool::new();
        let spawner = pool.spawner();

        let (tx, rx1) = mpmc::unbounded();
        let rx2 = rx1.clone();

        spawner
            .spawn(async move {
                assert_eq!(Some(10), rx1.recv().await);
            })
            .unwrap();
        spawner
            .spawn(async move {
                assert_eq!(Some(20), rx2.recv().await);
            })
            .unwrap();
        spawner
            .spawn(async move {
                tx.send_many([10, 20]).unwrap();
            })
            .unwrap();

        pool.run()
    }

    #[test]
    fn recv_many_returning_all() {
        let mut pool = LocalPool::new();
        let spawner = pool.spawner();

        let (tx, rx) = mpmc::unbounded();

        tx.send_many([10, 20, 30]).unwrap();
        spawner
            .spawn(async move {
                assert_eq!(vec![10, 20, 30], rx.recv_many(100).await);
            })
            .unwrap();

        pool.run();
    }

    #[test]
    fn recv_many_returning_some() {
        let mut pool = LocalPool::new();
        let spawner = pool.spawner();

        let (tx, rx) = mpmc::unbounded();

        tx.send_many([10, 20, 30]).unwrap();
        spawner
            .spawn(async move {
                assert_eq!(vec![10, 20], rx.recv_many(2).await);
                assert_eq!(vec![30], rx.recv_many(2).await);
            })
            .unwrap();

        pool.run();
    }

    #[test]
    fn recv_many_returns_empty_when_no_tx() {
        let mut pool = LocalPool::new();
        let spawner = pool.spawner();

        let (tx, rx) = mpmc::unbounded();
        drop(tx);

        spawner
            .spawn(async move {
                assert_eq!(Vec::<()>::new(), rx.recv_many(2).await);
            })
            .unwrap();

        pool.run();
    }

    #[test]
    fn batch_locally_accumulates() {
        let mut pool = LocalPool::new();
        let spawner = pool.spawner();

        let (tx, rx) = mpmc::unbounded();
        let read_values = Arc::new(Mutex::new(Vec::new()));
        let read_values_outer = read_values.clone();

        spawner
            .spawn(async move {
                while let Some(v) = rx.recv().await {
                    read_values.lock().unwrap().push(v);
                }
            })
            .unwrap();

        let read_values = read_values_outer;

        let mut tx = tx.batch(2);

        assert_eq!(Ok(()), tx.send(1));
        pool.run_until_stalled();
        assert_eq!(0, read_values.lock().unwrap().len());

        assert_eq!(Ok(()), tx.send(2));
        pool.run_until_stalled();
        assert_eq!(vec![1, 2], *read_values.lock().unwrap());

        assert_eq!(Ok(()), tx.send(3));
        pool.run_until_stalled();
        assert_eq!(vec![1, 2], *read_values.lock().unwrap());
        drop(tx);
        pool.run_until_stalled();
        assert_eq!(vec![1, 2, 3], *read_values.lock().unwrap());
    }
}
