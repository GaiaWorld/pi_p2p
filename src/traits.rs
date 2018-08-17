use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::io::Result;

use fnv::FnvHashMap;

use pi_lib::handler::Handler;
use pi_lib::atom::Atom;
use net::Socket;

pub trait P2PClientTraits {
    // 最终变为：$r，payload: params
    fn request(&self, topic: Atom, msg: Vec<u8>, resp: Box<Fn(Result<Arc<Vec<u8>>>)>, timeout: u8);

    fn set_topic_handler(&self, name: Atom, handler: Box<Fn(Result<(Socket, &[u8])>)>);
}

pub trait P2PServerTraits {
    // $q 请求
    // $r 回应
    // 最终变为：$q，payload; 返回值, $r/$id
    //
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
    ) -> Result<()>;

    fn unregister(&self, topic: Atom) -> Result<()>;

    //广播
    fn broadcast(&self, topic: Atom, msg: Arc<Vec<u8>>);
}
