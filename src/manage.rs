use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use fnv::FnvHashMap;
use net::Socket;

use client::P2PClient;
use server::P2PServer;
use traits::P2PServerTraits;

use pi_lib::atom::Atom;
use rand::prelude::*;

pub struct P2PManage {
    local: SocketAddr,
    server: Arc<P2PServer>,
    //被动连接的地址池
    _in_peers: Arc<RwLock<FnvHashMap<usize, (SocketAddr, Socket)>>>,
    //主动连接的地址池
    out_peers: Arc<RwLock<FnvHashMap<SocketAddr, Socket>>>,
    //节点池
    peer_list: Arc<RwLock<FnvHashMap<SocketAddr, u64>>>,
    //黑名单
    _black_list: Arc<RwLock<FnvHashMap<SocketAddr, u64>>>,
}

impl P2PManage {
    //saddr: 0.0.0.0:3333
    pub fn new(saddr: SocketAddr, mut map: FnvHashMap<SocketAddr, u64>) -> Self {
        //把自己加入peer_list中
        map.insert(
            saddr.clone(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
        let peer_list = Arc::new(RwLock::new(map));
        let in_peers = Arc::new(RwLock::new(FnvHashMap::default()));

        //启动服务
        let server = P2PServer::new(saddr.clone(), peer_list.clone(), in_peers.clone());
        // //启动客户端
        // let client = P2PClient::new(caddr, peer_list.clone());
        P2PManage {
            local: saddr,
            server,
            _in_peers: in_peers,
            out_peers: Arc::new(RwLock::new(FnvHashMap::default())),
            peer_list,
            _black_list: Arc::new(RwLock::new(FnvHashMap::default())),
        }
    }
    //连接节点
    pub fn connect(&self) {
        // let out_peers = self.out_peers.write().unwrap();
        let peers = self.peer_list.read().unwrap();
        let mut addrs = vec![];
        for addr in peers.keys() {
            addrs.push(addr);
        }
        let mut rng = thread_rng();
        let n = rng.gen_range(0, addrs.len());
        let addr = addrs[n];

        let out_map = self.out_peers.read().unwrap();
        println!(
            "p2pMannage connect out_peers map:{:?}, gen_range addr:{:?}, addrs:{:?}",
            out_map.keys(),
            addr,
            addrs
        );
        if out_map.get(&addr).is_none() && addr != &self.local {
            //启动客户端
            P2PClient::new(addr.clone(), self.peer_list.clone(), self.out_peers.clone());
        }
    }
    //指定节点连接
    pub fn connect_addr(&self, addr: &str) {
        P2PClient::new(addr.parse().unwrap(), self.peer_list.clone(), self.out_peers.clone());
    }
    //广播节点
    pub fn broadcast_addr(&self) {
        let mut peers = Vec::new();
        let peer_list = self.peer_list.read().unwrap();
        for (k, v) in peer_list.iter() {
            peers.push((k.clone(), v.clone()));
        }
        let string = serde_json::to_string(&peers).unwrap();
        self.server
            .broadcast(Atom::from("addr"), Arc::new(string.into_bytes()))
    }
}
