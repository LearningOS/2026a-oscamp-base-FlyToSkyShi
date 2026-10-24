//! # Async Channel
//!
//! In this exercise, you will use `tokio::sync::mpsc` async channels to implement producer-consumer pattern.
//!
//! ## Concepts
//! - `tokio::sync::mpsc::channel` creates bounded async channels
//! - Async `send` and `recv`
//! - Channel closing mechanism (receiver returns None after all senders are dropped)

use std::{mem, result};

use tokio::sync::mpsc;

/// Async producer-consumer:
/// - Create a producer task that sends each element from items sequentially
/// - Create a consumer task that receives all elements and collects them into Vec for return
///
/// Hint: Set channel capacity to items.len().max(1)
pub async fn producer_consumer(items: Vec<String>) -> Vec<String> {
    // step1: Create channel with mpsc::channel
    let capacity = items.len().max(1);
    let (tx, mut rx) = mpsc::channel::<String>(capacity);
    // step2: Spawn producer task: iterate through items, send each one
    let producer = tokio::spawn(async move {
        for item in items {
            if tx.send(item).await.is_err() {
                break;
            }
        }
    });
    // step3: Spawn consumer task: loop recv until channel closes, collect results
    // step`4: Wait for consumer to complete and return results
    let consumer = tokio::spawn(async move {
        let mut result = Vec::new();
        while let Some(item) =rx.recv().await {
            result.push(item);
        }
        result
    });
    producer.await.expect("producer task failed");
    consumer.await.expect("consumer task failed")
}

/// Fan‑in pattern: multiple producers, one consumer.
/// Create `n_producers` producers, each sending `"producer {id}: message"`.
/// Consumer collects all messages, sorts them, and returns.
pub async fn fan_in(n_producers: usize) -> Vec<String> {
    // step1: Create mpsc channel
    let capacity = n_producers.max(1);
    let (tx, mut rx) = mpsc::channel::<String>(capacity); 
    // step2: Spawn n_producers producer tasks
    //       Each sends format!("producer {id}: message")
    for id in 0..n_producers {
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            let message = format!("producer {id}: message");
            let _ = tx_clone.send(message).await;
        });
    }
    // step3: Drop the original sender (important! otherwise channel won't close)
    drop(tx);
    // step4: Consumer loops receiving, collects and sorts
    let mut result = Vec::new();
    while let Some(message) = rx.recv().await {
        result.push(message);
    }
    result.sort();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_producer_consumer() {
        let items = vec!["hello".into(), "async".into(), "world".into()];
        let result = producer_consumer(items.clone()).await;
        assert_eq!(result, items);
    }

    #[tokio::test]
    async fn test_producer_consumer_empty() {
        let result = producer_consumer(vec![]).await;
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_fan_in() {
        let result = fan_in(3).await;
        assert_eq!(
            result,
            vec![
                "producer 0: message",
                "producer 1: message",
                "producer 2: message",
            ]
        );
    }

    #[tokio::test]
    async fn test_fan_in_single() {
        let result = fan_in(1).await;
        assert_eq!(result, vec!["producer 0: message"]);
    }
}
