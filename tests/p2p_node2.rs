extern crate fnv;
extern crate net;
extern crate pi_p2p;
extern crate rand;

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::thread::{self, sleep};
use std::time::Duration;

use fnv::FnvHashMap;
use net::Socket;

use pi_p2p::manage::P2PManage;

use pi_p2p::client::P2PClient;
use pi_p2p::server::P2PServer;

use rand::prelude::*;

#[test]
fn run() {
    let addr: SocketAddr = "127.0.0.1:3334".parse().unwrap();
    let mut map = FnvHashMap::default();
    let p2p = P2PManage::new(addr, map);
    p2p.connect_addr("127.0.0.1:3333");
    loop {
        sleep(Duration::from_secs(10));
        p2p.connect();
        //广播地址
        p2p.broadcast_addr()
    }
}
