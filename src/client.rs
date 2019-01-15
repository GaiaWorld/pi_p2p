use std::io::Result;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use std::thread::{self, sleep};

use traits::P2PClientTraits;

use fnv::FnvHashMap;

use net::{Config, NetManager, Protocol, RawStream, RawSocket};
use net::api::{Socket, Stream};
use rpc::client::RPCClient;
use rpc::traits::RPCClientTraits;

use mqtt::client::ClientNode;

use atom::Atom;

pub struct P2PClient {
    rpc: Arc<RPCClient>,
    //主动连接列表（当前已建立的连接）
    peers: Arc<RwLock<FnvHashMap<SocketAddr, Socket>>>,
    //节点池
    peer_list: Arc<RwLock<FnvHashMap<SocketAddr, u64>>>,
}

fn handle_close(p2p: Arc<P2PClient>, stream_id: usize, reason: Result<()>) {
    println!("pi_p2p client handle_close !!!!!!!");
    let mut remove = Vec::new();

    let peers = &mut p2p.peers.write().unwrap();
    for (socket_addr, socket) in peers.iter() {
        if stream_id == match socket {
            &Socket::Raw(ref s) => s.socket,
            &Socket::Tls(ref s) => s.socket,
        } {
            remove.push(socket_addr.clone());
        }
    }

    for socket in remove {
        peers.remove(&socket);
    }
    println!(
        "client handle_close, stream_id = {}, reson = {:?}",
        stream_id, reason
    );
}

fn handle_connect(
    peer: Result<(RawSocket, Arc<RwLock<RawStream>>)>,
    addr: SocketAddr,
    p2p: Arc<P2PClient>,
) {
    //TODO 连接的参数需要改为配置
    let p2p_ = p2p.clone();
    let (socket, stream) = peer.unwrap();
    // println!(
    //     "client handle_connect: addr = {:?}, socket:{}",
    //     addr.unwrap(),
    //     socket.socket
    // );
    {
        let stream = &mut stream.write().unwrap();

        stream.set_close_callback(Box::new(move |id, reason| {
            handle_close(p2p_.clone(), id, reason)
        }));
        stream.set_send_buf_size(1024 * 1024);
        stream.set_recv_timeout(500 * 1000);
    }
    let rpc = p2p.rpc.clone();
    rpc.set_stream(Socket::Raw(socket.clone()), Stream::Raw(stream));

    // //遗言
    // let last_will = LastWill {
    //     topic: String::from("$last_will"),
    //     message: String::from("{clientid:1, msg:'xxx'}"),
    //     qos: QoS::AtMostOnce,
    //     retain: false,
    // };
    rpc.connect(
        10,
        None,
        Some(Box::new(|_r| println!("client handle_close ok "))),
        Some(Box::new(move |_r| {
            println!("client connect ok!!!!!!!!!");
        })),
    );
    thread::spawn(move || {
        connect_ok(p2p, addr, socket.clone());
    });
}

//连接成功后获取peer_list
fn connect_ok(p2p: Arc<P2PClient>, addr: SocketAddr, socket: RawSocket) {
    sleep(Duration::from_secs(2));
    println!("client func connect_ok!!!!!!!!!");
    //添加连接地址到out地址池中
    p2p.peers.write().unwrap().insert(addr, Socket::Raw(socket.clone()));
    let p2p_ = p2p.clone();
    let p2p2 = p2p.clone();
    let peer_len;
    {
        peer_len = p2p.peer_list.read().unwrap().len();
    }

    if peer_len < 100 {
        //TODO 需要改为从配置获取
        p2p.request(
            Atom::from("getAddr"),
            vec![],
            Box::new(move |r: Result<Arc<Vec<u8>>>| {
                add_peer_list(p2p_.clone(), r.unwrap().to_vec())
            }),
            10,
        )
    }

    //设置广播 addr的处理方法
    p2p.set_topic_handler(
        Atom::from("addr"),
        Box::new(move |r| {
            let (_socket, msg) = r.unwrap();
            add_peer_list(p2p2.clone(), msg.to_vec())
        }),
    )
}

//添加peer_list到本地
fn add_peer_list(p2p: Arc<P2PClient>, v: Vec<u8>) {
    println!("client getAddr request!!!!!!!!!v:{:?}", v);
    if v.len() > 0 {
        let s = String::from_utf8(v).unwrap();
        let peers: Vec<(SocketAddr, u64)> = serde_json::from_str(&s).unwrap();
        for (k, v) in peers {
            let mut peer_list = p2p.peer_list.write().unwrap();
            peer_list.insert(k, v);
            println!(
                "client getAddr request!!!!!!!!!peer_list:{:?}",
                peer_list.keys()
            );
        }
    }
}

impl P2PClient {
    pub fn new(
        addr: SocketAddr,
        peer_list: Arc<RwLock<FnvHashMap<SocketAddr, u64>>>,
        peers: Arc<RwLock<FnvHashMap<SocketAddr, Socket>>>,
    ) -> Arc<Self> {
        let mgr = NetManager::new();
        let config = Config {
            protocol: Protocol::TCP,
            addr: addr.clone(),
        };
        let mqtt = ClientNode::new();

        let rpc = RPCClient::new(mqtt);
        let p2p = Arc::new(P2PClient {
            rpc: Arc::new(rpc),
            peers: peers,
            peer_list,
        });
        let p2p_ = p2p.clone();
        mgr.connect(
            config,
            Box::new(move |peer, _caddr| handle_connect(peer, addr, p2p_.clone())),
        );
        return p2p;
    }
}

impl P2PClientTraits for P2PClient {
    fn request(&self, topic: Atom, msg: Vec<u8>, resp: Box<Fn(Result<Arc<Vec<u8>>>)>, timeout: u8) {
        self.rpc.request(topic, msg, resp, timeout);
    }

    fn set_topic_handler(&self, name: Atom, handler: Box<Fn(Result<(Socket, &[u8])>)>) {
        self.rpc.set_topic_handler(name, handler).is_ok();
    }
}
