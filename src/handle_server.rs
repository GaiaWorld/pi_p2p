use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use fnv::FnvHashMap;
use serde_json;

use pi_lib::atom::Atom;
use pi_lib::handler::{Args, Env, GenType, Handler};

use mqtt::session::Session;

pub struct GetAddr {
    //上次通信的时间戳
    _time: u64,
}

impl GetAddr {
    pub fn new() -> Self {
        GetAddr {
            _time: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
}

impl Handler for GetAddr {
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
        session: Arc<dyn Env>,
        atom: Atom,
        args: Args<Self::A, Self::B, Self::C, Self::D, Self::E, Self::F, Self::G, Self::H>,
    ) -> Self::HandleResult {
        let mut peers = Vec::new();
        unsafe {
            let peer_list = session.get_attr(Atom::from("peer_list")).unwrap();
            if let GenType::Pointer(peer_list) = peer_list {
                let peer_list = Arc::from_raw(peer_list);
                let peer_list: Option<&RwLock<FnvHashMap<SocketAddr, u64>>> = peer_list.downcast_ref();
                if let Some(peer_list) = peer_list
                {
                    let peer_list = peer_list.read().unwrap();
                    for (k, v) in peer_list.iter() {
                        peers.push((k.clone(), v.clone()));
                    }
                } else {
                }
            }
        }
        let v = serde_json::to_string(&peers).unwrap();
        if let Args::TwoArgs(_vsn, msg) = args {
            println!(
                "topic_handle!!!!!!!atom:{}, msg:{:?}",
                *atom,
                String::from_utf8(msg.to_vec())
            );
        }
        unsafe {
            let session = Arc::from_raw(Arc::into_raw(session) as *const Session);
            session.respond(atom, v.into_bytes());
        }
    }
}
