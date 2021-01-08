use crate::connection::Connection;
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
}
