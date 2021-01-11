use crate::connection::Connection;
use crate::frame::Frame;
use crate::Result;

use tokio::net::TcpStream;

pub struct Client {
    /// Tcp connection wrapped with read and write buffers.
    pub connection: Connection,
}

impl Client {
    pub async fn connect(addr: &str) -> Result<Client> {
        let socket = TcpStream::connect(addr).await?;
        let connection = Connection::new(socket);
        Ok(Client { connection })
    }
    pub async fn submit(&mut self, frame: &Frame) -> Result<Option<Frame>> {
        // Write request to the stream.
        self.connection.write_frame(frame).await?;
        // TODO: Receive response.
        self.connection.read_frame().await
        // let rframe = match res {
        //     Some(rframe) => rframe,
        //     None => , // handle this better
        // };

        // Ok(rframe)
    }
}
