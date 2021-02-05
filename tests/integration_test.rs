use std::sync::Arc;

mod common;

#[tokio::test]
async fn test_add() {
    let c = common::setup();
    let c1 = Arc::clone(&c);
    let server = tokio::spawn(async move { spaghetti::server::run(c).await });
    let client = tokio::spawn(async move { spaghetti::client::run(c1).await });
    server.await.unwrap();
    client.await.unwrap();


    assert_eq!(5, 5);
}
