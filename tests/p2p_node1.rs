extern crate rand;
extern crate pi_p2p;
extern crate fnv;
extern crate net;


use std::net::SocketAddr;
use std::thread::{sleep};
use std::time::Duration;

use fnv::FnvHashMap;

use pi_p2p::manage::P2PManage;


#[test]
fn run() {
    let addr: SocketAddr = "127.0.0.1:3333".parse().unwrap();
    let mut map = FnvHashMap::default();
    let addr2: SocketAddr = "127.0.0.1:3334".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:3335".parse().unwrap();
    map.insert(addr2, 1111);
    map.insert(addr3, 1111);
    let p2p = P2PManage::new(addr, map);
    sleep(Duration::from_secs(10));
    p2p.connect_addr("127.0.0.1:3334");
    
    loop {
        sleep(Duration::from_secs(10));
        p2p.connect();
        //广播地址
        p2p.broadcast_addr();
    }
}