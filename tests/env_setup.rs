use anyhow::Result;
use std::sync::Arc;
use std::error::Error;

use tokio::task::JoinHandle;
use tonic::Request;
use tonic::transport::{Server};

use chiselstore::{StoreServer};
use chiselstore::rpc::{RpcService, RpcTransport};
use chiselstore::rpc::proto::{Query};
use chiselstore::rpc::proto::rpc_client::RpcClient;
use chiselstore::rpc::proto::rpc_server::RpcServer;

/// Node authority (host and port) in the cluster.
fn node_authority(id: u64) -> (&'static str, u16) {
    let host = "127.0.0.1";
    let port = 50000 + (id as u16);
    (host, port)
}

/// Node RPC address in cluster.
fn node_rpc_addr(id: u64) -> String {
    let (host, port) = node_authority(id);
    format!("http://{}:{}", host, port)
}

/// Store node.
pub struct Node {
    pub id: u64,
    server: Arc<StoreServer<RpcTransport>>,
    msg_handle: JoinHandle<()>,
    ble_handle: JoinHandle<()>,
    rpc_handle: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Node {

    pub fn is_leader(&self) -> bool {
        self.server.is_leader()
    }

    pub async fn shutdown(&self) {
        self.rpc_handle.abort();
        self.ble_handle.abort();
        self.msg_handle.abort();
    }
}

/// Run Node Instance.
async fn run_node(id: u64, peers: Vec<u64>) -> Node {
    
    let (host, port) = node_authority(id);
    let rpc_listen_addr = format!("{}:{}", host, port).parse().unwrap();
    
    let transport = RpcTransport::new(Box::new(node_rpc_addr));
    
    let server = StoreServer::start(id, peers.clone(), transport).unwrap();
    let server = Arc::new(server);

    let msg = server.clone();
    let msg_handle = tokio::task::spawn(async move { msg.run().await; });

    let ble = server.clone();
    let ble_handle = tokio::task::spawn(async move { ble.run_ble().await; });

    let rpc = RpcService::new(server.clone());
    let rpc_handle = tokio::task::spawn(async move {
        let res = Server::builder()
            .add_service(RpcServer::new(rpc))
            .serve(rpc_listen_addr)
            .await;
        res
    });

    Node { id, server: server.clone(), msg_handle, ble_handle, rpc_handle, }
}

/// Setup Node Instances.
pub async fn setup(ids: Vec<u64>) -> Vec<Node> {
    let mut vec: Vec<Node> = Vec::new();
    for (i, id) in ids.iter().enumerate() {
        let mut peers: Vec<u64> = ids.clone();
        peers.remove(i as usize);
        let node = run_node(*id, peers).await;
        vec.push(node);
    }
    vec
}

/// Execute the query
pub async fn execute_query(id: u64, sql_stmt: String) -> Result<String, Box<dyn Error>> {
    let addr = node_rpc_addr(id);
    let mut client = RpcClient::connect(addr).await.unwrap();
    let sql_query = Request::new(Query {
        sql: sql_stmt.to_string(),
    });

    let response = client.execute(sql_query).await.unwrap();
    let response = response.into_inner();

    if response.rows.len() == 0 || response.rows[0].values.len() == 0 {
        return Ok(String::from(""));
    }
    let res = response.rows[0].values[0].clone();
    Ok(res)
}