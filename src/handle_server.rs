use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use fnv::FnvHashMap;
use serde_json;

use atom::Atom;
use handler::{Args, Handler};
use gray::GrayVersion;

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
    type A = Arc<RwLock<FnvHashMap<SocketAddr, u64>>>;
    type B = ();
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
        args: Args<Self::A, Self::B, Self::C, Self::D, Self::E, Self::F, Self::G, Self::H>,
    ) -> Self::HandleResult {
        let mut peers = Vec::new();
        match args {
            Args::OneArgs(p) => {
                let peer_list = p.read().unwrap();
                for (k, v) in peer_list.iter() {
                    peers.push((k.clone(), v.clone()));
                }
            },
            _ => panic!("GetAddr need only one arg"),
        };
        let v = serde_json::to_string(&peers).unwrap();
        unsafe {
            let session = Arc::from_raw(Arc::into_raw(session) as *const Session);
            session.respond(atom, v.into_bytes());
        }
    }
}
