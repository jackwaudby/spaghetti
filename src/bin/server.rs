//! This file is the entry point for the spaghetti server.
use server::server;

#[tokio::main]
async fn main() {
    server::run().await
}

// loop {
//     // " _" contains the IP and port of the new connection
//     let (mut socket, _) = listener.accept().await.unwrap();

//     // clone handle to database
//     let db = db.clone();

//     println!("Accepted connection!");

//     // spawn a new task for each inbound socket
//     tokio::spawn(async move {
//         // process(socket, db).await;

//         // echo server
//         // read contents from socket into buffer
//         let mut buf = vec![0; 1024];

//         loop {
//             match socket.read(&mut buf).await {
//                 // return value of Ok(0) indicates the remote has closed
//                 Ok(0) => return,
//                 Ok(n) => {
//                     // Copy data back to socket
//                     if socket.write_all(&buf[..n]).await.is_err() {
//                         // unexpected socket error
//                         return;
//                     }
//                 }
//                 Err(_) => {
//                     return;
//                 }
//             }
//         }
//     });
// }

// async fn process(socket: TcpStream, db: Db) {
//     // TODO: implement 'Connection' type that reads/writes frames instead of byte streams
//     // methods: read_frame(), write_frame(frame: &Frame)
//     let mut connection = Connection::new(socket);

//     // loop to accept multiple transactions per connection
//     while let Some(frame) = connection.read_frame().await.unwrap() {
//         // parse command from frame
//         // TODO: implement 'Frame' type
//         // TODO: implement 'Command' type
//         let response match Command::from_frame(frame).unwrap() {
//             TransactionA(cmd) = > {
//                 db.insert(cmd.param1().to_string()); // TODO: placeholder
//                 Frame::Simple("OK".to_string());     // response
//             },
//             TransactionB(cmd) = > {
//                  // TODO: placeholder
//                 if let Some(value) = db.get(cmd.param1().to_string()){
//                     Frame::Bulk(value.to_string());     // response
//                 } else {
//                     Frame::Null  // response
//                 }
//             },
//             cmd => panic!("unimplemented {:?}",cmd),
//         };

//         // write response to client
//         connection.write_frame(&response).await.unwrap();
//     }
// }
