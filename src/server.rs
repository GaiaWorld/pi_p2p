use std::io::Result;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use mqtt3::{self};
use rand::prelude::*;
use fnv::FnvHashMap;

use net::tls;
use handle_server::GetAddr;
use traits::P2PServerTraits;
use net::api::{Socket, Stream, TlsManager};
use net::{Config, NetManager, Protocol, RawSocket, RawStream};
use rpc::server::RPCServer;
use rpc::traits::RPCServerTraits;
use mqtt::server::ServerNode;
use mqtt::util;
use atom::Atom;
use handler::{Args, Handler};
use gray::GrayVersion;

pub struct P2PServer {
    rpc: Arc<RwLock<RPCServer>>,
    //被动连接的节点(当前已建立的连接)
    in_peers: Arc<RwLock<FnvHashMap<usize, (SocketAddr, Socket)>>>,
    //节点池
    peer_list: Arc<RwLock<FnvHashMap<SocketAddr, u64>>>,
}

//关闭连接处理
fn handle_close(p2p: Arc<P2PServer>, stream_id: usize, reason: Result<()>) {
    let peers = &mut p2p.in_peers.write().unwrap();
    peers.remove(&stream_id);

    println!(
        "server handle_close, stream_id = {}, reason = {:?}",
        stream_id, reason
    );
}

fn handle_bind_raw(
    peer: Result<(RawSocket, Arc<RwLock<RawStream>>)>,
    addr: Result<SocketAddr>,
    p2p: Arc<P2PServer>,
) {
    let peer_list = p2p.peer_list.clone();
    let (socket, stream) = peer.unwrap();
    let addr = addr.unwrap();
    let p2p_ = p2p.clone();
    println!(
        "server handle_bind: addr = {:?}, socket:{}",
        addr.clone(),
        socket.socket
    );

    //写入连接列表
    let socket_ = Socket::Raw(socket.clone());
    {
        let peers_map = &mut p2p.in_peers.write().unwrap();
        peers_map.insert(socket.socket, (addr.clone(), socket_.clone()));
    }
    let rpc = &mut p2p.rpc.write().unwrap();

    stream.write().unwrap().set_send_buf_size(1024 * 1024);
    stream.write().unwrap().set_recv_timeout(500 * 1000);

    //调用mqtt注册遗言
    let stream_ = Stream::Raw(stream);
    rpc.set_close_callback(
        stream_.clone(),
        Box::new(move |id, reason| handle_close(p2p_.clone(), id, reason)),
    );

    rpc.add_stream(socket_, stream_);
    // rpc.set_attr(Box::new(
    //     move |attr: &mut FnvHashMap<Atom, Arc<Any>>, _socket: Socket, connect: mqtt3::Connect| {
    //         if let Some(username) = connect.username {
    //             attr.insert(Atom::from("$username"), Arc::new(Vec::from(username)));
    //         }
    //         if let Some(password) = connect.password {
    //             attr.insert(Atom::from("$password"), Arc::new(Vec::from(password)));
    //         }
    //         attr.insert(
    //             Atom::from("$client_id"),
    //             Arc::new(Vec::from(connect.client_id)),
    //         );
    //         attr.insert(
    //             Atom::from("$connect_time"),
    //             Arc::new(Vec::from(
    //                 SystemTime::now()
    //                     .duration_since(SystemTime::UNIX_EPOCH)
    //                     .unwrap()
    //                     .as_secs()
    //                     .to_string(),
    //             )),
    //         );

    //         println!("pi_p2p server attr insert peer_list");
    //         attr.insert(Atom::from("peer_list"), peer_list.clone() as Arc<Any>);
    //     },
    // )).is_ok();
    // let topic_handle = Handle::new();
    // //通过rpc注册topic
    // rpc.register(Atom::from(String::from("a/b/c").as_str()), true, Arc::new(topic_handle)).is_ok();
    // let topic_handle = Handle::new();
    // //注册遗言
    // rpc.register(Atom::from(String::from("$last_will").as_str()), true, Arc::new(topic_handle)).is_ok();
}

fn handle_bind_tls(
    peer: Result<(net::tls::TlsSocket, Arc<RwLock<net::tls::TlsStream>>)>,
    addr: Result<SocketAddr>,
    p2p: Arc<P2PServer>,
) {
    let peer_list = p2p.peer_list.clone();
    let (socket, stream) = peer.unwrap();
    let addr = addr.unwrap();
    let p2p_ = p2p.clone();
    println!(
        "server handle_bind: addr = {:?}, socket:{}",
        addr.clone(),
        socket.socket
    );

    //写入连接列表
    let socket_ = Socket::Tls(socket.clone());
    {
        let peers_map = &mut p2p.in_peers.write().unwrap();
        peers_map.insert(socket.socket, (addr.clone(), socket_.clone()));
    }
    let rpc = &mut p2p.rpc.write().unwrap();

    stream.write().unwrap().set_send_buf_size(1024 * 1024);
    stream.write().unwrap().set_recv_timeout(500 * 1000);

    //调用mqtt注册遗言
    let stream_ = Stream::Tls(stream);
    rpc.set_close_callback(
        stream_.clone(),
        Box::new(move |id, reason| handle_close(p2p_.clone(), id, reason)),
    );

    rpc.add_stream(socket_, stream_);
    // rpc.set_attr(Box::new(
    //     move |attr: &mut FnvHashMap<Atom, Arc<Any>>, _socket: Socket, connect: mqtt3::Connect| {
    //         if let Some(username) = connect.username {
    //             attr.insert(Atom::from("$username"), Arc::new(Vec::from(username)));
    //         }
    //         if let Some(password) = connect.password {
    //             attr.insert(Atom::from("$password"), Arc::new(Vec::from(password)));
    //         }
    //         attr.insert(
    //             Atom::from("$client_id"),
    //             Arc::new(Vec::from(connect.client_id)),
    //         );
    //         attr.insert(
    //             Atom::from("$connect_time"),
    //             Arc::new(Vec::from(
    //                 SystemTime::now()
    //                     .duration_since(SystemTime::UNIX_EPOCH)
    //                     .unwrap()
    //                     .as_secs()
    //                     .to_string(),
    //             )),
    //         );

    //         println!("pi_p2p server attr insert peer_list");
    //         attr.insert(Atom::from("peer_list"), peer_list.clone() as Arc<Any>);
    //     },
    // )).is_ok();
    // let topic_handle = Handle::new();
    // //通过rpc注册topic
    // rpc.register(Atom::from(String::from("a/b/c").as_str()), true, Arc::new(topic_handle)).is_ok();
    // let topic_handle = Handle::new();
    // //注册遗言
    // rpc.register(Atom::from(String::from("$last_will").as_str()), true, Arc::new(topic_handle)).is_ok();
}

impl P2PServer {
    pub fn new(addr: SocketAddr, peer_list: Arc<RwLock<FnvHashMap<SocketAddr, u64>>>, in_peers: Arc<RwLock<FnvHashMap<usize, (SocketAddr, Socket)>>>, cert: Option<&str>, key: Option<&str>) -> Arc<Self> {
        let mqtt = ServerNode::new();
        let rpc = RPCServer::new(mqtt);
        let p2p = Arc::new(P2PServer {
            rpc: Arc::new(RwLock::new(rpc)),
            in_peers,
            peer_list: peer_list.clone(),
        });
        let p2p_ = p2p.clone();
        match (cert, key) {
            (Some(c), Some(k)) => {
                let config = net::tls::TlsConfig::new(Protocol::TCP, addr, c, k);
                TlsManager::new(net::tls::MAX_TLS_RECV_SIZE).bind(
                    config,
                    Box::new(move |peer, addr| handle_bind_tls(peer, addr, p2p_.clone())),
                );
            },
            _ => {
                let config = Config {
                    protocol: Protocol::TCP,
                    addr: addr,
                };
                NetManager::new().bind(
                    config,
                    Box::new(move |peer, addr| handle_bind_raw(peer, addr, p2p_.clone())),
                );
            }
        }

        //注册topic
        p2p.register(
            Atom::from(String::from("getAddr").as_str()),
            true,
            Arc::new(GetAddr::new()),
        ).is_ok();
        return p2p;
    }
}

pub struct RpcHandler {
    pub handler: Arc<Handler<
        A = Arc<RwLock<FnvHashMap<SocketAddr, u64>>>,
        B = (),
        C = (),
        D = (),
        E = (),
        F = (),
        G = (),
        H = (),
        HandleResult = ()
    >>,
    pub peer_list: Arc<RwLock<FnvHashMap<SocketAddr, u64>>>
}

impl RpcHandler {
    pub fn new(h: Arc<Handler<
        A = Arc<RwLock<FnvHashMap<SocketAddr, u64>>>,
        B = (),
        C = (),
        D = (),
        E = (),
        F = (),
        G = (),
        H = (),
        HandleResult = ()
    >>, peer_list: Arc<RwLock<FnvHashMap<SocketAddr, u64>>>) -> Self {
        RpcHandler{
            handler:h,
            peer_list
        }
    }
}

impl Handler for RpcHandler {
    type A = u8;
    type B = Arc<Vec<u8>>;
    type C = ();
    type D = ();
    type E = ();
    type F = ();
    type G = ();
    type H = ();
    type HandleResult = ();
    fn handle(
        &self,
        session: Arc<dyn GrayVersion>,
        atom: Atom,
        _args: Args<Self::A, Self::B, Self::C, Self::D, Self::E, Self::F, Self::G, Self::H>,
    ) -> Self::HandleResult {
        self.handler.handle(session, atom, Args::OneArgs(self.peer_list.clone()));
    }
}


impl P2PServerTraits for P2PServer {
    fn register(
        &self,
        topic: Atom,
        sync: bool,
        handle: Arc<
            Handler<
                A = Arc<RwLock<FnvHashMap<SocketAddr, u64>>>,
                B = (),
                C = (),
                D = (),
                E = (),
                F = (),
                G = (),
                H = (),
                HandleResult = (),
            >,
        >,
    ) -> Result<()> {
        let h = RpcHandler::new(handle, self.peer_list.clone());
        self.rpc.write().unwrap().register(topic, sync, Arc::new(h))
    }

    fn unregister(&self, topic: Atom) -> Result<()> {
        self.rpc.write().unwrap().unset_topic_meta(topic);
        Ok(())
    }

    //广播
    fn broadcast(&self, topic: Atom, msg: Arc<Vec<u8>>) {
        let peers = self.in_peers.read().unwrap();
        //广播的节点数组
        let mut sockets = vec![];
        //需要广播的节点数组
        let mut bro_sockets = vec![];
        //map转vec
        for (_, (_, socket)) in peers.iter() {
            sockets.push(socket.clone())
        }
        let socket_len = sockets.len();
        if socket_len > 8 {
            //随机选取8个节点
            let mut rng = thread_rng();
            let mut num = rng.gen_range(0, socket_len);
            for _n in 0..8 {
                bro_sockets.push(sockets[num].clone());
                if num == socket_len {
                    num = 0;
                };
                num += 1;
            }
        } else {
            bro_sockets = sockets;
        };

        for socket in bro_sockets {
            let t = mqtt3::TopicPath::from_str((*topic).as_str());
            //发送数据
            util::send_publish(
                &socket,
                false,
                mqtt3::QoS::AtMostOnce,
                t.unwrap().path.as_str(),
                Vec::from(msg.as_slice()),
            );
        }
    }
}
