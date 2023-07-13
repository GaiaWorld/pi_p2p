use std::thread;
use std::sync::Arc;
use std::str::FromStr;
use std::time::Duration;
use std::convert::TryFrom;
use std::net::{SocketAddr, IpAddr};

use futures::future::{FutureExt, LocalBoxFuture};
use async_channel::bounded;
use parking_lot::RwLock;
use ciborium::{Value,
               value::Integer};
use bytes::{Buf, BufMut, BytesMut};
use env_logger;

use pi_async::rt::{AsyncRuntime,
                   multi_thread::MultiTaskRuntimeBuilder,
                   serial::{AsyncRuntime as SerialAsyncRuntime, AsyncRuntimeBuilder, AsyncValue}};
use pi_atom::Atom;
use pi_gossip::{GossipNodeID,
                scuttlebutt::table::{Key, Table}};
use pi_gossip::scuttlebutt::table::NodeChangingEvent;
use pi_p2p::{connection::{ChannelId, Connection},
             frame::{P2PFrame, ParseFrameResult, P2PServiceReadInfo, P2PServiceWriteInfo},
             service::{P2PService, P2PServiceAdapter, P2PServiceListener, send_to_service},
             terminal::{DEFAULT_HOST_IP_KEY_STR, DEFAULT_HOST_PORT_KEY_STR, TerminalBuilder, Terminal},
             port::PortService};
use pi_p2p::port::{PortEvent, PortMode};

#[test]
fn test_frame() {
    let bin = vec![0x1, 0x7d, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x2, 0xfe, 0x7e, 0x0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x2, 0xfe, 0x7e, 0x0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x3, 0xff, 0x7f, 0x0, 0x0, 0x0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];

    //解析包括两条P2P消息的P2P帧
    let mut bytes = BytesMut::new();
    bytes.put_slice(bin.as_ref());
    let mut frame = P2PFrame::new();
    if let ParseFrameResult::Complated = frame.parse(&mut bytes) {
        println!("!!!!!!parse complated, frame: {:?}", frame);
    } else {
        println!("!!!!!!parse Partial, frame: {:?}", frame);
    }

    println!("");
    thread::sleep(Duration::from_millis(1000));

    //解析由全分包的P2P消息组成的P2P帧
    let mut frame = P2PFrame::new();
    for byte in &bin {
        match frame.parse(&mut BytesMut::from(&[*byte] as &[u8])) {
            ParseFrameResult::Partial => {
                continue;
            },
            ParseFrameResult::NextFrame => {
                unimplemented!();
            },
            ParseFrameResult::Complated => {
                println!("!!!!!!parse complated, frame: {:?}, payload: {:?}", frame, frame.payload());
                frame = P2PFrame::new(); //构建下一个P2P帧
            },
        }
    }

    println!("");
    thread::sleep(Duration::from_millis(1000));

    //解析由粘包的P2P消息组成的P2P帧
    let mut offset = 0;
    for top in [3, 73, 99, 101, 136, 179, 213, 256, 291, 365, 516, 518, 520] {
        let mut bytes = BytesMut::from(&bin[offset..top]);
        loop {
            match frame.parse(&mut bytes) {
                ParseFrameResult::Partial => {
                    println!("!!!!!!parse partial, frame: {:?}", frame);
                    offset = top;
                    break;
                },
                ParseFrameResult::NextFrame => {
                    println!("!!!!!!parse next, frame: {:?}, payload: {:?}", frame, frame.payload());
                    frame = P2PFrame::new(); //构建下一个P2P帧
                    continue;
                },
                ParseFrameResult::Complated => {
                    println!("!!!!!!parse complated, frame: {:?}, payload: {:?}", frame, frame.payload());
                    offset = top;
                    frame = P2PFrame::new(); //构建下一个P2P帧
                    break;
                },
            }
        }
    }
}

#[test]
fn test_frame_to_frame() {
    let mut bytes = BytesMut::new();

    let mut frame = P2PFrame::new();
    frame.set_tag(0x1);
    frame.set_len(0x7d);
    let bin: &[u8] = &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
    frame.append_payload(bin);
    bytes.put(frame.into_bytes());

    let mut frame = P2PFrame::new();
    frame.set_tag(0x2);
    frame.set_len(0x7e);
    let bin: &[u8] = &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
    frame.append_payload(bin);
    bytes.put(frame.into_bytes());

    let mut frame = P2PFrame::new();
    frame.set_tag(0x2);
    frame.set_len(0x7e);
    let bin: &[u8] = &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
    frame.append_payload(bin);
    bytes.put(frame.into_bytes());

    let mut frame = P2PFrame::new();
    frame.set_tag(0x3);
    frame.set_len(0x7f);
    let bin: &[u8] = &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
    frame.append_payload(bin);
    bytes.put(frame.into_bytes());

    let mut frame = P2PFrame::new();
    while bytes.remaining() > 0 {
        loop {
            match frame.parse(&mut bytes) {
                ParseFrameResult::Partial => {
                    println!("!!!!!!parse partial, frame: {:?}", frame);
                    break;
                },
                ParseFrameResult::NextFrame => {
                    println!("!!!!!!parse next, frame: {:?}, payload: {:?}", frame, frame.payload());
                    frame = P2PFrame::new(); //构建下一个P2P帧
                    continue;
                },
                ParseFrameResult::Complated => {
                    println!("!!!!!!parse complated, frame: {:?}, payload: {:?}", frame, frame.payload());
                    frame = P2PFrame::new(); //构建下一个P2P帧
                    break;
                },
            }
        }
    }
}

pub struct TestP2PService {
    terminal: RwLock<Option<Terminal>>,
    listener: RwLock<Option<Arc<P2PServiceListener>>>,
}

impl P2PService for TestP2PService {
    fn init(&self,
            local: GossipNodeID,
            terminal: Terminal,
            listener: Arc<P2PServiceListener>) {
        *self.terminal.write() = Some(terminal);
        *self.listener.write() = Some(listener);
    }

    fn get_terminal(&self) -> Option<Terminal> {
        if let Some(terminal) = self.terminal.read().as_ref() {
            Some(terminal.clone())
        } else {
            None
        }
    }

    fn get_listener(&self) -> Option<Arc<P2PServiceListener>> {
        if let Some(listener) = self.listener.read().as_ref() {
            Some(listener.clone())
        } else {
            None
        }
    }

    fn changed(&self, event: NodeChangingEvent) {
        println!("######Changed, event: {:?}", event);
    }

    fn handshaked(&self,
                  peer: GossipNodeID,
                  connection: Connection) -> LocalBoxFuture<'static, ()> {
        async move {

        }.boxed_local()
    }

    fn received(&self,
                from: Option<GossipNodeID>,
                connection: Connection,
                channel_id: ChannelId,
                info: P2PServiceReadInfo) -> LocalBoxFuture<'static, ()> {
        todo!()
    }

    fn closed_channel(&self,
                      peer: GossipNodeID,
                      connection: Connection,
                      channel_id: ChannelId,
                      code: u32,
                      result: std::io::Result<()>) -> LocalBoxFuture<'static, ()> {
        todo!()
    }

    fn closed(&self,
              peer: GossipNodeID,
              code: u32,
              result: std::io::Result<()>) -> LocalBoxFuture<'static, ()> {
        todo!()
    }
}

impl TestP2PService {
    pub fn new() -> Self {
        TestP2PService {
            terminal: RwLock::new(None),
            listener: RwLock::new(None),
        }
    }
}

#[test]
fn test_startup() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();

    let rt = MultiTaskRuntimeBuilder::default().build();

    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy,
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/A/a.crt",
                                            "./tests/A/a.key");
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();

            let service = Arc::new(TestP2PService::new());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            println!("!!!!!!local a: {:?}", terminal.get_self());
        });
    }

    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy,
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/B/b.crt",
                                            "./tests/B/b.key");
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();

            let service = Arc::new(TestP2PService::new());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            println!("!!!!!!local b: {:?}", terminal.get_self());
        });
    }

    thread::sleep(Duration::from_millis(1000000000));
}
static mut aaa: u8 = 0;
#[test]
fn test_connect_to_peer() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();

    let rt = MultiTaskRuntimeBuilder::default().build();

    //A主机
    let (send, recv_a) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy,
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/A/a.crt",
                                            "./tests/A/a.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38080));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("7DctBSLsx6EHXhE7tFN3qMV1vvzuxHwTKhiZKz65hF4u",
                                       IpAddr::from_str("192.168.35.65").unwrap(),
                                       38081);

            let service = Arc::new(TestP2PService::new());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            send.send(terminal).await;
        });
    }

    //B主机
    let (send, recv_b) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy,
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/B/b.crt",
                                            "./tests/B/b.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38081));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("VmvoecS91YdyzncoXp94iFgkmCu82SitFTfFYT4DeKR",
                                       IpAddr::from_str("192.168.35.65").unwrap(),
                                       38080);

            let service = Arc::new(TestP2PService::new());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            send.send(terminal).await;
        });
    }

    rt.spawn(rt.alloc(), async move {
        let terminal_a = recv_a.recv().await.unwrap();
        let host_a = terminal_a.get_self();
        println!("!!!!!!terminal a: {:?}", host_a);

        let terminal_b = recv_b.recv().await.unwrap();
        let host_b = terminal_b.get_self();
        println!("!!!!!!terminal b: {:?}", host_b);

        //A连接B
        assert_eq!(terminal_a.is_connected(host_b), false);
        assert_eq!(terminal_b.is_connected(host_a), false);
        match terminal_a.connect_to(host_b,
                                    Some(5000)).await {
            Err(e) => {
                //连接失败
                println!("!!!!!!a connect to b failed, reason: {:?}", e);
                assert_eq!(terminal_a.is_connected(host_b), false);
                assert_eq!(terminal_b.is_connected(host_a), false);
            },
            Ok(connection) => {
                //连接已成功
                println!("!!!!!!a connect to b ok, connection: {:#?}", connection);
                assert_eq!(terminal_a.is_connected(host_b), true);
                assert_eq!(terminal_b.is_connected(host_a), false);

                //B连接A
                match terminal_b.connect_to(host_a,
                                            Some(5000)).await {
                    Err(e) => {
                        //连接失败
                        println!("!!!!!!b connect to a failed, reason: {:?}", e);
                        assert_eq!(terminal_a.is_connected(host_b), true);
                        assert_eq!(terminal_b.is_connected(host_a), false);
                    },
                    Ok(connection) => {
                        //连接已成功
                        println!("!!!!!!b connect to a ok, connection: {:#?}", connection);
                        assert_eq!(terminal_a.is_connected(host_b), true);
                        assert_eq!(terminal_b.is_connected(host_a), true);
                    },
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_sync_between_host() {
//启动日志系统
    env_logger::builder().format_timestamp_millis().init();

    let rt = MultiTaskRuntimeBuilder::default().build();

    //A主机
    let (send, recv_a) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy,
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/A/a.crt",
                                            "./tests/A/a.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38080));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("7DctBSLsx6EHXhE7tFN3qMV1vvzuxHwTKhiZKz65hF4u",
                                       IpAddr::from_str("192.168.35.65").unwrap(),
                                    38081);

            let service = Arc::new(TestP2PService::new());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            send.send(terminal).await;
        });
    }

    //B主机
    let (send, recv_b) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy,
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/B/b.crt",
                                            "./tests/B/b.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38081));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("VmvoecS91YdyzncoXp94iFgkmCu82SitFTfFYT4DeKR",
                                    IpAddr::from_str("192.168.35.65").unwrap(),
                                    38080);

            let service = Arc::new(TestP2PService::new());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            send.send(terminal).await;
        });
    }

    rt.spawn(rt.alloc(), async move {
        let terminal_a = recv_a.recv().await.unwrap();
        let host_a = terminal_a.get_self();
        println!("!!!!!!terminal a: {:?}", host_a);

        let terminal_b = recv_b.recv().await.unwrap();
        let host_b = terminal_b.get_self();
        println!("!!!!!!terminal b: {:?}", host_b);

        //A同步B
        assert_eq!(terminal_a.is_connected(host_b), false);
        assert_eq!(terminal_b.is_connected(host_a), false);
        match terminal_a
            .sync_with(terminal_b.get_self(),
                       Duration::from_millis(5000))
            .await {
            Err(e) => {
                panic!("Sync with host failed, peer: {:?}, local: {:?}, reason: {:?}",
                       terminal_b.get_self(),
                       terminal_a.get_self(),
                       e);
            },
            Ok(statistics) => {
                let behavior = terminal_a.get_behavior();
                println!("{:?} sync host ok\nstatistics: {:?}\ntable: {:#?}",
                         behavior.get_id(),
                         statistics,
                         behavior.get_table());

                for node in behavior.get_table().private_nodes() {
                    let heartbeat = behavior.get_table().get_heartbeat(&node);
                    let ip = behavior.get_table().get_private_value(&node, &Key::from("ip"));
                    let port = behavior.get_table().get_private_value(&node, &Key::from("port"));
                    println!("node: {:?}\n\theartbeat: {:?}\n\tip: {:?}\n\tport: {:?}",
                             node.as_str(),
                             heartbeat,
                             ip,
                             port);
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_random_sync_between_host() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();

    let rt = MultiTaskRuntimeBuilder::default().build();

    //A主机
    let (send, recv_a) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy,
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/A/a.crt",
                                            "./tests/A/a.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38080));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("7DctBSLsx6EHXhE7tFN3qMV1vvzuxHwTKhiZKz65hF4u",
                                    IpAddr::from_str("192.168.35.65").unwrap(),
                                    38081);
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let service = Arc::new(TestP2PService::new());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            send.send(terminal).await;
        });
    }

    //B主机
    let (send, recv_b) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy,
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/B/b.crt",
                                            "./tests/B/b.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38081));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("VmvoecS91YdyzncoXp94iFgkmCu82SitFTfFYT4DeKR",
                                    IpAddr::from_str("192.168.35.65").unwrap(),
                                    38080);
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let service = Arc::new(TestP2PService::new());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            send.send(terminal).await;
        });
    }

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        let terminal_a = recv_a.recv().await.unwrap();
        let host_a = terminal_a.get_self();
        println!("!!!!!!terminal a: {:?}", host_a);

        let terminal_b = recv_b.recv().await.unwrap();
        let host_b = terminal_b.get_self();
        println!("!!!!!!terminal b: {:?}", host_b);

        //启动P2P终端
        rt_copy.timeout(1000).await;
        terminal_a.startup(rt_copy.clone(),
                           1,
                           1,
                           5000,
                           5000,
                           None,
                           Duration::from_millis(30000)).await;
        terminal_b.startup(rt_copy.clone(),
                           1,
                           1,
                           5000,
                           5000,
                           None,
                           Duration::from_millis(30000)).await;

        //等待上次同步完成后再本地修改属性
        rt_copy.timeout(5000).await;
        println!("!!!!!!terminal_a, peer_failure_valuation: {:?}, table: {:?}",
                 terminal_a.failure_valuation(terminal_b.get_self()).await,
                 terminal_a.get_behavior().get_table().get_private_attrs());
        println!("!!!!!!terminal_b, peer_failure_valuation: {:?}, table: {:?}",
                 terminal_b.failure_valuation(terminal_a.get_self()).await,
                 terminal_b.get_behavior().get_table().get_private_attrs());
        terminal_a.set_local_host_attr(Key::from("level"),
                                       Value::Integer(Integer::from(0xff)));
        terminal_b.set_local_host_attr(Key::from("exp"),
                                       Value::Integer(Integer::from(0xffff)));
        rt_copy.timeout(30000).await;
        println!("!!!!!!terminal_a, peer_failure_valuation: {:?}, table: {:?}",
                 terminal_a.failure_valuation(terminal_b.get_self()).await,
                 terminal_a.get_behavior().get_table().get_private_attrs());
        println!("!!!!!!terminal_b, peer_failure_valuation: {:?}, table: {:?}",
                 terminal_b.failure_valuation(terminal_a.get_self()).await,
                 terminal_b.get_behavior().get_table().get_private_attrs());

        rt_copy.timeout(35000).await;
        println!("!!!!!!terminal_a, peer_failure_valuation: {:?}, table: {:?}",
                 terminal_a.failure_valuation(terminal_b.get_self()).await,
                 terminal_a.get_behavior().get_table().get_private_attrs());
        println!("!!!!!!terminal_b, peer_failure_valuation: {:?}, table: {:?}",
                 terminal_b.failure_valuation(terminal_a.get_self()).await,
                 terminal_b.get_behavior().get_table().get_private_attrs());
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_heartbeat_between_host() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();

    let rt = MultiTaskRuntimeBuilder::default().build();

    //A主机
    let (send, recv_a) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy,
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/A/a.crt",
                                            "./tests/A/a.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38080));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("7DctBSLsx6EHXhE7tFN3qMV1vvzuxHwTKhiZKz65hF4u",
                                    IpAddr::from_str("192.168.35.65").unwrap(),
                                    38081);
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let service = Arc::new(TestP2PService::new());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            send.send(terminal).await;
        });
    }

    //B主机
    let (send, recv_b) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy,
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/B/b.crt",
                                            "./tests/B/b.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38081));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("VmvoecS91YdyzncoXp94iFgkmCu82SitFTfFYT4DeKR",
                                    IpAddr::from_str("192.168.35.65").unwrap(),
                                    38080);
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let service = Arc::new(TestP2PService::new());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            send.send(terminal).await;
        });
    }

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        let terminal_a = recv_a.recv().await.unwrap();
        let host_a = terminal_a.get_self();
        println!("!!!!!!terminal a: {:?}", host_a);

        let terminal_b = recv_b.recv().await.unwrap();
        let host_b = terminal_b.get_self();
        println!("!!!!!!terminal b: {:?}", host_b);

        rt_copy.timeout(1000).await;

        //启动P2P终端
        terminal_a.startup(rt_copy.clone(),
                           1,
                           1,
                           5000,
                           5000,
                           Some(Duration::from_millis(5000)),
                           Duration::from_millis(30000)).await;
        terminal_b.startup(rt_copy.clone(),
                           1,
                           1,
                           5000,
                           5000,
                           Some(Duration::from_millis(5000)),
                           Duration::from_millis(30000)).await;

        //输出终端上的所有主机的相关状态
        loop {
            for host in terminal_a.active_hosts().await {
                println!("######terminal_a, active_host: {:?}, failure_valuation: {:?}, delay: {:?}",
                         &host,
                         terminal_a.failure_valuation(&host).await,
                         terminal_a.get_connection_delay(&host));
            }
            for host in terminal_a.inactive_hosts().await {
                println!("######terminal_a, inactive_host: {:?}, failure_valuation: {:?}, delay: {:?}",
                         &host,
                         terminal_a.failure_valuation(&host).await,
                         terminal_a.get_connection_delay(&host));
            }
            for host in terminal_a.recycled_hosts().await {
                println!("######terminal_a, recycled_hosts: {:?}, failure_valuation: {:?}, delay: {:?}",
                         &host,
                         terminal_a.failure_valuation(&host).await,
                         terminal_a.get_connection_delay(&host));
            }
            for host in terminal_b.active_hosts().await {
                println!("######terminal_b, active_host: {:?}, failure_valuation: {:?}, delay: {:?}",
                         &host,
                         terminal_b.failure_valuation(&host).await,
                         terminal_b.get_connection_delay(&host));
            }
            for host in terminal_b.inactive_hosts().await {
                println!("######terminal_b, inactive_host: {:?}, failure_valuation: {:?}, delay: {:?}",
                         &host,
                         terminal_b.failure_valuation(&host).await,
                         terminal_b.get_connection_delay(&host));
            }
            for host in terminal_b.recycled_hosts().await {
                println!("######terminal_b, recycled_hosts: {:?}, failure_valuation: {:?}, delay: {:?}",
                         &host,
                         terminal_b.failure_valuation(&host).await,
                         terminal_b.get_connection_delay(&host));
            }
            println!("===================================");
            rt_copy.timeout(5000).await;
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_failure_between_host() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();

    let rt = MultiTaskRuntimeBuilder::default().build();

    //A主机
    let (send, recv_a) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy,
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/A/a.crt",
                                            "./tests/A/a.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38080));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("7DctBSLsx6EHXhE7tFN3qMV1vvzuxHwTKhiZKz65hF4u",
                                       IpAddr::from_str("192.168.35.65").unwrap(),
                                       38081);
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let service = Arc::new(TestP2PService::new());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            send.send(terminal).await;
        });
    }

    //B主机
    let (send, recv_b) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy,
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/B/b.crt",
                                            "./tests/B/b.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38081));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("VmvoecS91YdyzncoXp94iFgkmCu82SitFTfFYT4DeKR",
                                       IpAddr::from_str("192.168.35.65").unwrap(),
                                       38080);
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let service = Arc::new(TestP2PService::new());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            send.send(terminal).await;
        });
    }

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        let terminal_a = recv_a.recv().await.unwrap();
        let host_a = terminal_a.get_self();
        println!("!!!!!!terminal a: {:?}", host_a);

        let terminal_b = recv_b.recv().await.unwrap();
        let host_b = terminal_b.get_self();
        println!("!!!!!!terminal b: {:?}", host_b);

        rt_copy.timeout(1000).await;

        //启动P2P终端
        terminal_a.startup(rt_copy.clone(),
                           1,
                           1,
                           5000,
                           5000,
                           Some(Duration::from_millis(5000)),
                           Duration::from_millis(30000)).await;
        terminal_b.startup(rt_copy.clone(),
                           1,
                           1,
                           5000,
                           5000,
                           Some(Duration::from_millis(5000)),
                           Duration::from_millis(30000)).await;

        //输出终端上的所有主机的相关状态
        let mut count = 0;
        loop {
            count += 1;
            if count % 10 == 0 {
                if terminal_a.is_heartbeating() {
                    terminal_a.pause_heartbeating();
                    println!("!!!!!!Pause a heartbeating");
                } else {
                    terminal_a.recover_heartbeating();
                    println!("!!!!!!Recover a heartbeating");
                }
                if terminal_a.is_collecting() {
                    println!("!!!!!!Pause a collecting");
                    terminal_a.pause_collecting();
                } else {
                    println!("!!!!!!Recover a collecting");
                    terminal_a.recover_collecting();
                }
                if terminal_b.is_heartbeating() {
                    terminal_b.pause_heartbeating();
                    println!("!!!!!!Pause b heartbeating");
                } else {
                    terminal_b.recover_heartbeating();
                    println!("!!!!!!Recover b heartbeating");
                }
                if terminal_b.is_collecting() {
                    println!("!!!!!!Pause b collecting");
                    terminal_b.pause_collecting();
                } else {
                    println!("!!!!!!Recover b collecting");
                    terminal_b.recover_collecting();
                }
            }

            for host in terminal_a.active_hosts().await {
                println!("######terminal_a, active_host: {:?}, failure_valuation: {:?}, delay: {:?}",
                         &host,
                         terminal_a.failure_valuation(&host).await,
                         terminal_a.get_connection_delay(&host));
            }
            for host in terminal_a.inactive_hosts().await {
                println!("######terminal_a, inactive_host: {:?}, failure_valuation: {:?}, delay: {:?}",
                         &host,
                         terminal_a.failure_valuation(&host).await,
                         terminal_a.get_connection_delay(&host));
            }
            for host in terminal_a.recycled_hosts().await {
                println!("######terminal_a, recycled_hosts: {:?}, failure_valuation: {:?}, delay: {:?}",
                         &host,
                         terminal_a.failure_valuation(&host).await,
                         terminal_a.get_connection_delay(&host));
            }
            for host in terminal_b.active_hosts().await {
                println!("######terminal_b, active_host: {:?}, failure_valuation: {:?}, delay: {:?}",
                         &host,
                         terminal_b.failure_valuation(&host).await,
                         terminal_b.get_connection_delay(&host));
            }
            for host in terminal_b.inactive_hosts().await {
                println!("######terminal_b, inactive_host: {:?}, failure_valuation: {:?}, delay: {:?}",
                         &host,
                         terminal_b.failure_valuation(&host).await,
                         terminal_b.get_connection_delay(&host));
            }
            for host in terminal_b.recycled_hosts().await {
                println!("######terminal_b, recycled_hosts: {:?}, failure_valuation: {:?}, delay: {:?}",
                         &host,
                         terminal_b.failure_valuation(&host).await,
                         terminal_b.get_connection_delay(&host));
            }
            println!("===================================");
            rt_copy.timeout(5000).await;
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

pub struct TestByP2PService(Arc<(
    RwLock<Option<Terminal>>,
    RwLock<Option<Arc<P2PServiceListener>>>,
)>);

impl Clone for TestByP2PService {
    fn clone(&self) -> Self {
        TestByP2PService(self.0.clone())
    }
}

impl P2PService for TestByP2PService {
    fn init(&self,
            local: GossipNodeID,
            terminal: Terminal,
            listener: Arc<P2PServiceListener>) {
        *(self.0).0.write() = Some(terminal);
        *(self.0).1.write() = Some(listener);
        println!("!!!!!!{:?} inited", local);
    }

    fn get_terminal(&self) -> Option<Terminal> {
        if let Some(terminal) = (self.0).0.read().as_ref() {
            Some(terminal.clone())
        } else {
            None
        }
    }

    fn get_listener(&self) -> Option<Arc<P2PServiceListener>> {
        if let Some(listener) = (self.0).1.read().as_ref() {
            Some(listener.clone())
        } else {
            None
        }
    }

    fn changed(&self, event: NodeChangingEvent) {
        let local = self.get_terminal().unwrap().get_self().clone();
        println!("######Changed, local: {:?}, event: {:#?}", local, event);

        if event.node() == &local {
            //解除监听本地主机属性改变
            self.unlisten_hosts(vec![local]);
        }

        let mut b = true;
        for item in event.changed_attrs() {
            if item.0.to_string().as_str() == "$heartbeat" {
                //有心跳，则解除监听心跳状态改变
                self.unlisten_keys(vec![Key::from("$heartbeat")]);
                b = false;
            }
        }

        if b {
            //解除监听心跳状态改变后，再解除对端主机的状态改变
            self.unlisten_hosts(vec![event.node().clone()]);
        }
    }

    fn handshaked(&self,
                  peer: GossipNodeID,
                  connection: Connection) -> LocalBoxFuture<'static, ()> {
        let service = self.clone();
        async move {
            println!("!!!!!!handshaked, peer: {:?}, connection: {:?}", peer, connection);

            let terminal = service.get_terminal().unwrap();
            if let Ok(channel_id) = terminal.open_channel(&peer).await {
                let info = P2PServiceWriteInfo {
                    port: 80,
                    index: 1,
                    payload: BytesMut::new(),
                };
                if let Err(e) = send_to_service(&connection, &channel_id, info) {
                    panic!("Send service info failed, peer: {:?}, channel: {:?}, reason: {:?}",
                           connection,
                           channel_id,
                           e);
                } else {
                    println!("!!!!!!send service info ok, peer: {:?}, channel: {:?}",
                             connection,
                             channel_id);
                }
            }
        }.boxed_local()
    }

    fn received(&self,
                from: Option<GossipNodeID>,
                connection: Connection,
                channel_id: ChannelId,
                info: P2PServiceReadInfo) -> LocalBoxFuture<'static, ()> {
        async move {
            println!("!!!!!!received, from: {:?}, channel_id: {:?}, port: {:?}",
                     from,
                     channel_id,
                     info.port);
        }.boxed_local()
    }

    fn closed_channel(&self,
                      peer: GossipNodeID,
                      connection: Connection,
                      channel_id: ChannelId,
                      code: u32,
                      result: std::io::Result<()>) -> LocalBoxFuture<'static, ()> {
        todo!()
    }

    fn closed(&self,
              peer: GossipNodeID,
              code: u32,
              result: std::io::Result<()>) -> LocalBoxFuture<'static, ()> {
        todo!()
    }
}

impl TestByP2PService {
    pub fn new() -> Self {
        TestByP2PService(Arc::new((RwLock::new(None), RwLock::new(None))))
    }
}

#[test]
fn test_service_between_host() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();

    let rt = MultiTaskRuntimeBuilder::default().build();

    //A主机
    let (send, recv_a) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy,
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/A/a.crt",
                                            "./tests/A/a.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38080));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("7DctBSLsx6EHXhE7tFN3qMV1vvzuxHwTKhiZKz65hF4u",
                                       IpAddr::from_str("192.168.35.65").unwrap(),
                                       38081);
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let service = Arc::new(TestByP2PService::new());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            send.send(terminal).await;
        });
    }

    //B主机
    let (send, recv_b) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy,
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/B/b.crt",
                                            "./tests/B/b.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38081));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("VmvoecS91YdyzncoXp94iFgkmCu82SitFTfFYT4DeKR",
                                       IpAddr::from_str("192.168.35.65").unwrap(),
                                       38080);
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let service = Arc::new(TestByP2PService::new());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            send.send(terminal).await;
        });
    }

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        let terminal_a = recv_a.recv().await.unwrap();
        let host_a = terminal_a.get_self();
        println!("!!!!!!terminal a: {:?}", host_a);

        let terminal_b = recv_b.recv().await.unwrap();
        let host_b = terminal_b.get_self();
        println!("!!!!!!terminal b: {:?}", host_b);

        rt_copy.timeout(1000).await;

        //启动P2P终端
        terminal_a.startup(rt_copy.clone(),
                           1,
                           1,
                           5000,
                           5000,
                           Some(Duration::from_millis(5000)),
                           Duration::from_millis(30000)).await;
        terminal_b.startup(rt_copy.clone(),
                           1,
                           1,
                           5000,
                           5000,
                           Some(Duration::from_millis(5000)),
                           Duration::from_millis(30000)).await;
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_listen_between_host() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();

    let rt = MultiTaskRuntimeBuilder::default().build();

    //A主机
    let (send, recv_a) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy,
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/A/a.crt",
                                            "./tests/A/a.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38080));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("7DctBSLsx6EHXhE7tFN3qMV1vvzuxHwTKhiZKz65hF4u",
                                       IpAddr::from_str("192.168.35.65").unwrap(),
                                       38081);
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let service = Arc::new(TestByP2PService::new());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            send.send(terminal).await;
        });
    }

    //B主机
    let (send, recv_b) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy,
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/B/b.crt",
                                            "./tests/B/b.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38081));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("VmvoecS91YdyzncoXp94iFgkmCu82SitFTfFYT4DeKR",
                                       IpAddr::from_str("192.168.35.65").unwrap(),
                                       38080);
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let service = Arc::new(TestByP2PService::new());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            send.send(terminal).await;
        });
    }

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        let terminal_a = recv_a.recv().await.unwrap();
        let host_a = terminal_a.get_self();
        println!("!!!!!!terminal a: {:?}", host_a);

        let terminal_b = recv_b.recv().await.unwrap();
        let host_b = terminal_b.get_self();
        println!("!!!!!!terminal b: {:?}", host_b);

        //启动P2P终端
        rt_copy.timeout(1000).await;
        terminal_a.startup(rt_copy.clone(),
                           1,
                           1,
                           5000,
                           5000,
                           Some(Duration::from_millis(5000)),
                           Duration::from_millis(30000)).await;
        terminal_b.startup(rt_copy.clone(),
                           1,
                           1,
                           5000,
                           5000,
                           Some(Duration::from_millis(5000)),
                           Duration::from_millis(30000)).await;

        //等待上次同步完成后再本地修改属性
        rt_copy.timeout(5000).await;
        terminal_a.set_local_host_attr(Key::from("level"),
                                       Value::Integer(Integer::from(0xff)));
        terminal_b.set_local_host_attr(Key::from("exp"),
                                       Value::Integer(Integer::from(0xffff)));
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_open_port() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();

    let rt = MultiTaskRuntimeBuilder::default().build();

    //A主机
    let (send, recv_a) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy,
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/A/a.crt",
                                            "./tests/A/a.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38080));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("7DctBSLsx6EHXhE7tFN3qMV1vvzuxHwTKhiZKz65hF4u",
                                       IpAddr::from_str("192.168.35.65").unwrap(),
                                       38081);
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let port_service = PortService::new();
            let service = Arc::new(port_service.clone());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            send.send((terminal, port_service)).await;
        });
    }

    //B主机
    let (send, recv_b) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy,
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/B/b.crt",
                                            "./tests/B/b.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38081));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("VmvoecS91YdyzncoXp94iFgkmCu82SitFTfFYT4DeKR",
                                       IpAddr::from_str("192.168.35.65").unwrap(),
                                       38080);
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let port_service = PortService::new();
            let service = Arc::new(port_service.clone());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            send.send((terminal, port_service)).await;
        });
    }

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        let (terminal_a, service_a) = recv_a.recv().await.unwrap();
        let host_a = terminal_a.get_self().clone();
        println!("!!!!!!terminal a: {:?}", host_a);

        let (terminal_b, service_b) = recv_b.recv().await.unwrap();
        let host_b = terminal_b.get_self().clone();
        println!("!!!!!!terminal b: {:?}", host_b);

        //启动P2P终端
        rt_copy.timeout(1000).await;
        terminal_a.startup(rt_copy.clone(),
                           1,
                           1,
                           5000,
                           5000,
                           Some(Duration::from_millis(5000)),
                           Duration::from_millis(30000)).await;
        terminal_b.startup(rt_copy.clone(),
                           1,
                           1,
                           5000,
                           5000,
                           Some(Duration::from_millis(5000)),
                           Duration::from_millis(30000)).await;

        //等待上次同步完成后再打开端口
        rt_copy.timeout(5000).await;
        let rt_clone = rt_copy.clone();
        rt_copy.spawn(rt_copy.alloc(), async move {
            loop {
                if let Ok(event) = service_a.poll_event().await {
                    if event.is_inited() {
                        println!("!!!!!!service a inited");
                        break;
                    }
                }
            }

            match service_a.open_port(8080, Some("test port"), PortMode::Send) {
                Err(e) => {
                    println!("!!!!!!open service a port failed, reason: {:?}", e);
                },
                Ok((peer_port, pipeline)) => {
                    println!("!!!!!!open service a port ok\n\tpeer_port: {:#?}\n\tpipeline: {:#?}\n\tports: {:#?}",
                             peer_port,
                             pipeline,
                             service_a.local_ports());

                    rt_clone.timeout(30000).await;

                    println!("!!!!!!service a, peer ports: {:#?}", service_a.peer_ports(&host_b));
                },
            }
            println!("!!!!!!service a ports: {:#?}", service_a.local_ports());
        });
        let rt_clone = rt_copy.clone();
        rt_copy.spawn(rt_copy.alloc(), async move {
            loop {
                if let Ok(event) = service_b.poll_event().await {
                    if event.is_inited() {
                        println!("!!!!!!service b inited");
                        break;
                    }
                }
            }

            match service_b.open_port(8081, Some("test port"), PortMode::Send) {
                Err(e) => {
                    println!("!!!!!!open service b port failed, reason: {:?}", e);
                },
                Ok((peer_port, pipeline)) => {
                    println!("!!!!!!open service b port ok\n\tpeer_port: {:#?}\n\tpipeline: {:#?}\n\tports: {:#?}",
                             peer_port,
                             pipeline,
                             service_b.local_ports());

                    rt_clone.timeout(30000).await;

                    println!("!!!!!!service b, peer ports: {:#?}", service_b.peer_ports(&host_a));
                },
            }
            println!("!!!!!!service b ports: {:#?}", service_b.local_ports());
        });
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_access_port_between_host() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();

    let rt = MultiTaskRuntimeBuilder::default().build();

    //A主机
    let (send, recv_a) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy.clone(),
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/A/a.crt",
                                            "./tests/A/a.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38080));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("7DctBSLsx6EHXhE7tFN3qMV1vvzuxHwTKhiZKz65hF4u",
                                       IpAddr::from_str("192.168.35.65").unwrap(),
                                       38081);
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let port_service = PortService::new();
            let service = Arc::new(port_service.clone());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            terminal.pause_heartbeating();
            terminal.startup(rt_copy.clone(),
                               1,
                               1,
                               5000,
                               5000,
                               Some(Duration::from_millis(5000)),
                               Duration::from_millis(30000)).await;
            send.send(port_service).await;
        });
    }

    //B主机
    let (send, recv_b) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy.clone(),
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/B/b.crt",
                                            "./tests/B/b.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38081));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("VmvoecS91YdyzncoXp94iFgkmCu82SitFTfFYT4DeKR",
                                       IpAddr::from_str("192.168.35.65").unwrap(),
                                       38080);
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let port_service = PortService::new();
            let service = Arc::new(port_service.clone());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            terminal.pause_heartbeating();
            terminal.startup(rt_copy.clone(),
                               1,
                               1,
                               5000,
                               5000,
                               Some(Duration::from_millis(5000)),
                               Duration::from_millis(30000)).await;
            send.send(port_service).await;
        });
    }

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        let service_a = recv_a.recv().await.unwrap();
        let host_a = service_a.local_host().unwrap();
        println!("!!!!!!terminal a: {:?}", host_a);

        let service_b = recv_b.recv().await.unwrap();
        let host_b = service_b.local_host().unwrap();
        println!("!!!!!!terminal b: {:?}", host_b);

        //等待初始化后再打开端口
        rt_copy.timeout(1000).await;
        let mut count = 0;
        while count < 2 {
            if let Some(event) = service_a.try_poll_event() {
                if event.is_inited() {
                    println!("!!!!!!service a inited");
                    count += 1;
                }
            }

            if let Some(event) = service_b.try_poll_event() {
                if event.is_inited() {
                    println!("!!!!!!service b inited");
                    count += 1;
                }
            }
        }

        let rt_clone = rt_copy.clone();
        rt_copy.spawn(rt_copy.alloc(), async move {
            match service_a.open_port(8080, Some("test port"), PortMode::Send) {
                Err(e) => {
                    println!("!!!!!!open service a port failed, reason: {:?}", e);
                },
                Ok((self_port, pipeline)) => {
                    println!("!!!!!!open service a port ok\n\tpeer_port: {:#?}\n\tpipeline: {:#?}\n\tports: {:#?}",
                             self_port,
                             pipeline,
                             service_a.local_ports());

                    rt_clone.timeout(30000).await;
                    let ports = service_a.peer_ports(&host_b);
                    println!("!!!!!!service a, peer ports: {:?}", ports);

                    match service_a
                        .connect_to(&host_b,
                                    ports[0].0,
                                    5000)
                        .await {
                        Err(e) => {
                            println!("!!!!!!connect to peer service port failed, peer: {:?}, port: {:?}, reason: {:?}",
                                     host_b,
                                     ports[0].0,
                                     e);
                        },
                        Ok(peer_port) => {
                            self_port.send(0, "Hello a from a".as_bytes());
                            peer_port.send(0, "Hello b from a".as_bytes());

                            while let Ok((from, index, bytes)) = pipeline.poll().await {
                                println!("!!!!!!from: {:?}, index: {:?}, msg: {:?}",
                                         from,
                                         index,
                                         String::from_utf8_lossy(bytes.as_ref()));

                                if let Some((peer, _reply)) = from {
                                    if peer_port.peer().unwrap() == peer {
                                        peer_port.send(0, "Hello b from a".as_bytes());
                                    }
                                }
                            }
                        },
                    }
                },
            }
        });

        let rt_clone = rt_copy.clone();
        rt_copy.spawn(rt_copy.alloc(), async move {
            match service_b.open_port(8081, Some("test port"), PortMode::Send) {
                Err(e) => {
                    println!("!!!!!!open service b port failed, reason: {:?}", e);
                },
                Ok((self_port, pipeline)) => {
                    println!("!!!!!!open service b port ok\n\tpeer_port: {:#?}\n\tpipeline: {:#?}\n\tports: {:#?}",
                             self_port,
                             pipeline,
                             service_b.local_ports());

                    rt_clone.timeout(30000).await;
                    let ports = service_b.peer_ports(&host_a);
                    println!("!!!!!!service a, peer ports: {:?}", ports);

                    match service_b
                        .connect_to(&host_a,
                                    ports[0].0,
                                    5000)
                        .await {
                        Err(e) => {
                            println!("!!!!!!connect to peer service port failed, peer: {:?}, port: {:?}, reason: {:?}",
                                     host_a,
                                     ports[0].0,
                                     e);
                        },
                        Ok(peer_port) => {
                            self_port.send(0, "Hello b from b".as_bytes());
                            peer_port.send(0, "Hello a from b".as_bytes());

                            while let Ok((from, index, bytes)) = pipeline.poll().await {
                                println!("!!!!!!from: {:?}, index: {:?}, msg: {:?}",
                                         from,
                                         index,
                                         String::from_utf8_lossy(bytes.as_ref()));

                                if let Some((peer, _reply)) = from {
                                    if peer_port.peer().unwrap() == peer {
                                        peer_port.send(0, "Hello a from b".as_bytes());
                                    }
                                }
                            }
                        },
                    }
                },
            }
        });
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_close_port() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();

    let rt = MultiTaskRuntimeBuilder::default().build();

    //A主机
    let (send, recv_a) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy.clone(),
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/A/a.crt",
                                            "./tests/A/a.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38080));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("7DctBSLsx6EHXhE7tFN3qMV1vvzuxHwTKhiZKz65hF4u",
                                       IpAddr::from_str("192.168.35.65").unwrap(),
                                       38081);
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let port_service = PortService::new();
            let service = Arc::new(port_service.clone());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            terminal.pause_heartbeating();
            terminal.pause_collecting();
            terminal.startup(rt_copy.clone(),
                             1,
                             1,
                             5000,
                             5000,
                             Some(Duration::from_millis(5000)),
                             Duration::from_millis(30000)).await;
            send.send(port_service).await;
        });
    }

    //B主机
    let (send, recv_b) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy.clone(),
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/B/b.crt",
                                            "./tests/B/b.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38081));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("VmvoecS91YdyzncoXp94iFgkmCu82SitFTfFYT4DeKR",
                                       IpAddr::from_str("192.168.35.65").unwrap(),
                                       38080);
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let port_service = PortService::new();
            let service = Arc::new(port_service.clone());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            terminal.pause_heartbeating();
            terminal.pause_collecting();
            terminal.startup(rt_copy.clone(),
                             1,
                             1,
                             5000,
                             5000,
                             Some(Duration::from_millis(5000)),
                             Duration::from_millis(30000)).await;
            send.send(port_service).await;
        });
    }

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        let service_a = recv_a.recv().await.unwrap();
        let host_a = service_a.local_host().unwrap();
        println!("!!!!!!terminal a: {:?}", host_a);

        let service_b = recv_b.recv().await.unwrap();
        let host_b = service_b.local_host().unwrap();
        println!("!!!!!!terminal b: {:?}", host_b);

        //等待初始化后再打开端口
        let mut count = 0;
        while count < 2 {
            if let Some(event) = service_a.try_poll_event() {
                if event.is_inited() {
                    println!("!!!!!!service a inited");
                    count += 1;
                }
            }

            if let Some(event) = service_b.try_poll_event() {
                if event.is_inited() {
                    println!("!!!!!!service b inited");
                    count += 1;
                }
            }
        }

        rt_copy.spawn(rt_copy.alloc(), async move {
            match service_a.open_port(8080, Some("test port"), PortMode::Send) {
                Err(e) => {
                    println!("!!!!!!open service a port failed, reason: {:?}", e);
                },
                Ok((self_port, pipeline)) => {
                    println!("!!!!!!open service a port ok\n\tpeer_port: {:#?}\n\tpipeline: {:#?}\n\tports: {:#?}",
                             self_port,
                             pipeline,
                             service_a.local_ports());

                    match service_a
                        .connect_to(&host_b,
                                    8081,
                                    5000)
                        .await {
                        Err(e) => {
                            println!("!!!!!!connect to peer service port failed, peer: {:?}, port: 8081, reason: {:?}",
                                     host_b,
                                     e);
                        },
                        Ok(peer_port) => {
                            self_port.send(0, "Hello a from a".as_bytes());
                            peer_port.send(0, "Hello b from a".as_bytes());

                            let mut count = 0;
                            while let Ok((from, index, bytes)) = pipeline.poll().await {
                                println!("!!!!!!from: {:?}, index: {:?}, msg: {:?}",
                                         from,
                                         index,
                                         String::from_utf8_lossy(bytes.as_ref()));

                                if let Some((peer, _reply)) = from {
                                    if peer_port.peer().unwrap() == peer {
                                        peer_port.send(0, "Hello b from a".as_bytes());
                                    }
                                }

                                count += 1;
                                if count > 10 {
                                    break;
                                }
                            }
                            println!("!!!!!!port: {:?}, delay: {:?}, failure: {:?}",
                                     peer_port,
                                     peer_port.network_delay(),
                                     peer_port.peer_failure().await);
                        },
                    }

                    pipeline.close();
                },
            }

            while let Some(event) = service_a.try_poll_event() {
                println!("!!!!!!service a event: {:?}", event);
            }
        });

        rt_copy.spawn(rt_copy.alloc(), async move {
            match service_b.open_port(8081, Some("test port"), PortMode::Send) {
                Err(e) => {
                    println!("!!!!!!open service b port failed, reason: {:?}", e);
                },
                Ok((self_port, pipeline)) => {
                    println!("!!!!!!open service b port ok\n\tpeer_port: {:#?}\n\tpipeline: {:#?}\n\tports: {:#?}",
                             self_port,
                             pipeline,
                             service_b.local_ports());

                    match service_b
                        .connect_to(&host_a,
                                    8080,
                                    5000)
                        .await {
                        Err(e) => {
                            println!("!!!!!!connect to peer service port failed, peer: {:?}, port: 8080, reason: {:?}",
                                     host_a,
                                     e);
                        },
                        Ok(peer_port) => {
                            self_port.send(0, "Hello b from b".as_bytes());
                            peer_port.send(0, "Hello a from b".as_bytes());

                            let mut count = 0;
                            while let Ok((from, index, bytes)) = pipeline.poll().await {
                                println!("!!!!!!from: {:?}, index: {:?}, msg: {:?}",
                                         from,
                                         index,
                                         String::from_utf8_lossy(bytes.as_ref()));

                                if let Some((peer, _reply)) = from {
                                    if peer_port.peer().unwrap() == peer {
                                        peer_port.send(0, "Hello a from b".as_bytes());
                                    }
                                }

                                count += 1;
                                if count > 10 {
                                    break;
                                }
                            }
                            println!("!!!!!!port: {:?}, delay: {:?}, failure: {:?}",
                                     peer_port,
                                     peer_port.network_delay(),
                                     peer_port.peer_failure().await);
                        },
                    }

                    pipeline.close();
                },
            }

            while let Some(event) = service_b.try_poll_event() {
                println!("!!!!!!service b event: {:?}", event);
            }
        });
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_request_port() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();

    let rt = MultiTaskRuntimeBuilder::default().build();

    //A主机
    let (send, recv_a) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy.clone(),
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/A/a.crt",
                                            "./tests/A/a.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38080));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("7DctBSLsx6EHXhE7tFN3qMV1vvzuxHwTKhiZKz65hF4u",
                                       IpAddr::from_str("192.168.35.65").unwrap(),
                                       38081);
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let port_service = PortService::new();
            let service = Arc::new(port_service.clone());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            terminal.pause_heartbeating();
            terminal.pause_collecting();
            terminal.startup(rt_copy.clone(),
                             1,
                             1,
                             5000,
                             5000,
                             Some(Duration::from_millis(5000)),
                             Duration::from_millis(30000)).await;
            send.send(port_service).await;
        });
    }

    //B主机
    let (send, recv_b) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy.clone(),
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/B/b.crt",
                                            "./tests/B/b.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38081));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("VmvoecS91YdyzncoXp94iFgkmCu82SitFTfFYT4DeKR",
                                       IpAddr::from_str("192.168.35.65").unwrap(),
                                       38080);
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let port_service = PortService::new();
            let service = Arc::new(port_service.clone());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            terminal.pause_heartbeating();
            terminal.pause_collecting();
            terminal.startup(rt_copy.clone(),
                             1,
                             1,
                             5000,
                             5000,
                             Some(Duration::from_millis(5000)),
                             Duration::from_millis(30000)).await;
            send.send(port_service).await;
        });
    }

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        let service_a = recv_a.recv().await.unwrap();
        let host_a = service_a.local_host().unwrap();
        println!("!!!!!!terminal a: {:?}", host_a);

        let service_b = recv_b.recv().await.unwrap();
        let host_b = service_b.local_host().unwrap();
        println!("!!!!!!terminal b: {:?}", host_b);

        //等待初始化后再打开端口
        let mut count = 0;
        while count < 2 {
            if let Some(event) = service_a.try_poll_event() {
                if event.is_inited() {
                    println!("!!!!!!service a inited");
                    count += 1;
                }
            }

            if let Some(event) = service_b.try_poll_event() {
                if event.is_inited() {
                    println!("!!!!!!service b inited");
                    count += 1;
                }
            }
        }

        let rt_clone = rt_copy.clone();
        rt_copy.spawn(rt_copy.alloc(), async move {
            match service_a.open_port(8080, Some("test port"), PortMode::Send) {
                Err(e) => {
                    println!("!!!!!!open service a port failed, reason: {:?}", e);
                },
                Ok((self_port, pipeline)) => {
                    println!("!!!!!!open service a port ok\n\tpeer_port: {:#?}\n\tpipeline: {:#?}\n\tports: {:#?}",
                             self_port,
                             pipeline,
                             service_a.local_ports());

                    match service_a
                        .connect_to(&host_b,
                                    8081,
                                    5000)
                        .await {
                        Err(e) => {
                            println!("!!!!!!connect to peer service port failed, peer: {:?}, port: 8081, reason: {:?}",
                                     host_b,
                                     e);
                        },
                        Ok(peer_port) => {
                            self_port.send(0, "Hello a from a".as_bytes());

                            let service_a_copy = service_a.clone();
                            let peer_port_copy = peer_port.clone();
                            rt_clone.spawn(rt_clone.alloc(), async move {
                                while let Ok(event) = service_a_copy.poll_event().await {
                                    if let PortEvent::Handshaked(peer) = event {
                                        println!("!!!!!!handshaked, peer: {:?}", peer);
                                        if peer_port_copy.peer().unwrap() == peer {
                                            //与端口所在的对端主机已握手
                                            break;
                                        }
                                    }
                                }

                                for index in 0..10 {
                                    let r = peer_port_copy
                                        .request(index,
                                                 "Hello b from a".as_bytes(),
                                                 5000).await;
                                    println!("######response from b, index: {:?}, r: {:?}", index, r);
                                }
                            });

                            while let Ok((from, index, bytes)) = pipeline.poll().await {
                                if let Some((peer, reply_peer)) = from {
                                    if let Err(e) = reply_peer.reply("Hi b") {
                                        println!("!!!!!!reply failed, peer: {:?}, index: {:?}, reason: {:?}", peer, index, e);
                                    }
                                } else {
                                    println!("!!!!!!from: None, index: {:?}, msg: {:?}",
                                             index,
                                             String::from_utf8_lossy(bytes.as_ref()));
                                }
                            }
                        },
                    }
                },
            }
        });

        let rt_clone = rt_copy.clone();
        rt_copy.spawn(rt_copy.alloc(), async move {
            match service_b.open_port(8081, Some("test port"), PortMode::Send) {
                Err(e) => {
                    println!("!!!!!!open service b port failed, reason: {:?}", e);
                },
                Ok((self_port, pipeline)) => {
                    println!("!!!!!!open service b port ok\n\tpeer_port: {:#?}\n\tpipeline: {:#?}\n\tports: {:#?}",
                             self_port,
                             pipeline,
                             service_b.local_ports());

                    match service_b
                        .connect_to(&host_a,
                                    8080,
                                    5000)
                        .await {
                        Err(e) => {
                            println!("!!!!!!connect to peer service port failed, peer: {:?}, port: 8080, reason: {:?}",
                                     host_a,
                                     e);
                        },
                        Ok(peer_port) => {
                            self_port.send(0, "Hello b from b".as_bytes());

                            let service_b_copy = service_b.clone();
                            let peer_port_copy = peer_port.clone();
                            rt_clone.spawn(rt_clone.alloc(), async move {
                                while let Ok(event) = service_b_copy.poll_event().await {
                                    if let PortEvent::Handshaked(peer) = event {
                                        println!("!!!!!!handshaked, peer: {:?}", peer);
                                        if peer_port_copy.peer().unwrap() == peer {
                                            //与端口所在的对端主机已握手
                                            break;
                                        }
                                    }
                                }

                                for index in 0..10 {
                                    let r = peer_port_copy
                                        .request(index,
                                                 "Hello a from b".as_bytes(),
                                                 5000).await;
                                    println!("######response from a, index: {:?}, r: {:?}", index, r);
                                }
                            });

                            while let Ok((from, index, bytes)) = pipeline.poll().await {
                                if let Some((peer, reply_peer)) = from {
                                    if let Err(e) = reply_peer.reply("Hi a") {
                                        println!("!!!!!!reply failed, peer: {:?}, index: {:?}, reason: {:?}", peer, index, e);
                                    }
                                } else {
                                    println!("!!!!!!from: None, index: {:?}, msg: {:?}",
                                             index,
                                             String::from_utf8_lossy(bytes.as_ref()));
                                }
                            }
                        },
                    }
                },
            }
        });
    });

    thread::sleep(Duration::from_millis(1000000000));
}

//首先启动A主机，再启动B主机
#[test]
fn test_service_a() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();

    let rt = MultiTaskRuntimeBuilder::default().build();

    //A主机
    let (send, recv_a) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy.clone(),
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/A/a.crt",
                                            "./tests/A/a.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38080));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let port_service = PortService::new();
            let service = Arc::new(port_service.clone());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            terminal.startup(rt_copy.clone(),
                             1,
                             1,
                             5000,
                             5000,
                             Some(Duration::from_millis(5000)),
                             Duration::from_millis(30000)).await;
            send.send(port_service).await;
        });
    }

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        let service_a = recv_a.recv().await.unwrap();
        let host_a = service_a.local_host().unwrap();
        println!("!!!!!!terminal a: {:?}", host_a);

        //等待初始化后再打开端口
        let mut count = 0;
        while count < 1 {
            if let Some(event) = service_a.try_poll_event() {
                if event.is_inited() {
                    println!("!!!!!!service a inited");
                    count += 1;
                }
            }
        }

        rt_copy.spawn(rt_copy.alloc(), async move {
            match service_a.open_port(8080, Some("test port"), PortMode::Send) {
                Err(e) => {
                    println!("!!!!!!open service a port failed, reason: {:?}", e);
                },
                Ok((self_port, pipeline)) => {
                    println!("!!!!!!open service a port ok\n\tpeer_port: {:#?}\n\tpipeline: {:#?}\n\tports: {:#?}",
                             self_port,
                             pipeline,
                             service_a.local_ports());

                    while let Ok((from, index, bytes)) = pipeline.poll().await {
                        if let Some((peer, reply_peer)) = from {
                            println!("!!!!!!received, from: {:?}, bytes: {:?}", peer, bytes);
                            if let Err(e) = reply_peer.reply("Hi b") {
                                println!("!!!!!!reply failed, peer: {:?}, index: {:?}, reason: {:?}", peer, index, e);
                            }
                        } else {
                            println!("!!!!!!from: None, index: {:?}, msg: {:?}",
                                     index,
                                     String::from_utf8_lossy(bytes.as_ref()));
                        }
                    }
                },
            }
        });
    });

    thread::sleep(Duration::from_millis(1000000000));
}

//首先启动A主机，再启动B主机
#[test]
fn test_service_b() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();

    let rt = MultiTaskRuntimeBuilder::default().build();

    //B主机
    let (send, recv_b) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy.clone(),
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/B/b.crt",
                                            "./tests/B/b.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38081));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("VmvoecS91YdyzncoXp94iFgkmCu82SitFTfFYT4DeKR",
                                       IpAddr::from_str("192.168.35.65").unwrap(),
                                       38080);
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let port_service = PortService::new();
            let service = Arc::new(port_service.clone());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            terminal.startup(rt_copy.clone(),
                             1,
                             1,
                             5000,
                             5000,
                             Some(Duration::from_millis(5000)),
                             Duration::from_millis(30000)).await;
            send.send(port_service).await;
        });
    }

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        let service_b = recv_b.recv().await.unwrap();
        let host_b = service_b.local_host().unwrap();
        println!("!!!!!!terminal b: {:?}", host_b);

        //等待初始化后再打开端口
        let mut count = 0;
        while count < 1 {
            if let Some(event) = service_b.try_poll_event() {
                if event.is_inited() {
                    println!("!!!!!!service b inited");
                    count += 1;
                }
            }
        }

        let rt_clone = rt_copy.clone();
        rt_copy.spawn(rt_copy.alloc(), async move {
            let host_a = GossipNodeID::try_from("VmvoecS91YdyzncoXp94iFgkmCu82SitFTfFYT4DeKR/0".to_string()).unwrap();
            match service_b
                .connect_to(&host_a,
                            8080,
                            5000)
                .await {
                Err(e) => {
                    println!("!!!!!!connect to peer service port failed, peer: {:?}, port: 8080, reason: {:?}",
                             host_a,
                             e);
                },
                Ok(peer_port) => {
                    let service_b_copy = service_b.clone();
                    let peer_port_copy = peer_port.clone();
                    rt_clone.spawn(rt_clone.alloc(), async move {
                        while let Ok(event) = service_b_copy.poll_event().await {
                            if let PortEvent::Handshaked(peer) = event {
                                println!("!!!!!!handshaked, peer: {:?}", peer);
                                if peer_port_copy.peer().unwrap() == peer {
                                    //与端口所在的对端主机已握手
                                    break;
                                }
                            }
                        }

                        let mut index = 0;
                        loop {
                            index += 1;
                            let r = peer_port_copy
                                .request(index,
                                         "Hello a from b".as_bytes(),
                                         5000).await;
                            println!("######response from a, index: {:?}, r: {:?}", index, r);
                        }
                    });
                },
            }
        });
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_close_after_request_port() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();

    let rt = MultiTaskRuntimeBuilder::default().build();

    //A主机
    let (send, recv_a) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy.clone(),
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/A/a.crt",
                                            "./tests/A/a.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38080));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("7DctBSLsx6EHXhE7tFN3qMV1vvzuxHwTKhiZKz65hF4u",
                                       IpAddr::from_str("192.168.35.65").unwrap(),
                                       38081);
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let port_service = PortService::new();
            let service = Arc::new(port_service.clone());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            terminal.pause_heartbeating();
            terminal.pause_collecting();
            terminal.startup(rt_copy.clone(),
                             1,
                             1,
                             5000,
                             5000,
                             Some(Duration::from_millis(5000)),
                             Duration::from_millis(30000)).await;
            send.send(port_service).await;
        });
    }

    //B主机
    let (send, recv_b) = bounded(1);
    {
        let server_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let server_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];
        let client_udp_runtime = AsyncRuntimeBuilder::default_local_thread(None, None);
        let client_quic_runtimes = vec![AsyncRuntimeBuilder::default_local_thread(None, None)];

        let rt_copy = rt.clone();
        rt.spawn(rt.alloc(), async move {
            let mut builder = TerminalBuilder::default();
            builder = builder
                .bind_runtime(rt_copy.clone(),
                              server_udp_runtime,
                              server_quic_runtimes,
                              client_udp_runtime,
                              client_quic_runtimes);
            builder = builder
                .bind_terminal_cert_and_key("./tests/ca/ca_cert.pem",
                                            "./tests/B/b.crt",
                                            "./tests/B/b.key");
            builder = builder
                .bind_server_local_udp_address(SocketAddr::new(IpAddr::from_str("192.168.35.65").unwrap(), 38081));
            builder = builder
                .set_client_verify_level_to_server::<&str>(None);
            builder = builder.enable_server_verify_level_to_client();
            builder = builder
                .add_seed_host_address("VmvoecS91YdyzncoXp94iFgkmCu82SitFTfFYT4DeKR",
                                       IpAddr::from_str("192.168.35.65").unwrap(),
                                       38080);
            builder = builder.set_failure_detector_config(8.0,
                                                          1000,
                                                          Duration::from_millis(10000),
                                                          Duration::from_millis(5000),
                                                          Duration::from_millis(30000));

            let port_service = PortService::new();
            let service = Arc::new(port_service.clone());
            let adapter = Arc::new(P2PServiceAdapter::with_service(service.clone(), Some(5000)));
            builder = builder.set_service(adapter.clone());
            builder = builder.set_version(0, 1, 0);
            let terminal = builder.build().await.unwrap();
            terminal.pause_heartbeating();
            terminal.pause_collecting();
            terminal.startup(rt_copy.clone(),
                             1,
                             1,
                             5000,
                             5000,
                             Some(Duration::from_millis(5000)),
                             Duration::from_millis(30000)).await;
            send.send(port_service).await;
        });
    }

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        let service_a = recv_a.recv().await.unwrap();
        let host_a = service_a.local_host().unwrap();
        println!("!!!!!!terminal a: {:?}", host_a);

        let service_b = recv_b.recv().await.unwrap();
        let host_b = service_b.local_host().unwrap();
        println!("!!!!!!terminal b: {:?}", host_b);

        //等待初始化后再打开端口
        let mut count = 0;
        while count < 2 {
            if let Some(event) = service_a.try_poll_event() {
                if event.is_inited() {
                    println!("!!!!!!service a inited");
                    count += 1;
                }
            }

            if let Some(event) = service_b.try_poll_event() {
                if event.is_inited() {
                    println!("!!!!!!service b inited");
                    count += 1;
                }
            }
        }

        let rt_clone = rt_copy.clone();
        rt_copy.spawn(rt_copy.alloc(), async move {
            match service_a.open_port(8080, Some("test port"), PortMode::Send) {
                Err(e) => {
                    println!("!!!!!!open service a port failed, reason: {:?}", e);
                },
                Ok((self_port, pipeline)) => {
                    println!("!!!!!!open service a port ok\n\tpeer_port: {:#?}\n\tpipeline: {:#?}\n\tports: {:#?}",
                             self_port,
                             pipeline,
                             service_a.local_ports());

                    match service_a
                        .connect_to(&host_b,
                                    8081,
                                    5000)
                        .await {
                        Err(e) => {
                            println!("!!!!!!connect to peer service port failed, peer: {:?}, port: 8081, reason: {:?}",
                                     host_b,
                                     e);
                        },
                        Ok(peer_port) => {
                            self_port.send(0, "Hello a from a".as_bytes());

                            let service_a_copy = service_a.clone();
                            rt_clone.spawn(rt_clone.alloc(), async move {
                                while let Ok(event) = service_a_copy.poll_event().await {
                                    if let PortEvent::Handshaked(peer) = event {
                                        println!("!!!!!!handshaked, peer: {:?}", peer);
                                        if peer_port.peer().unwrap() == peer {
                                            //与端口所在的对端主机已握手
                                            break;
                                        }
                                    }
                                }

                                for index in 0..10 {
                                    let r = peer_port
                                        .request(index,
                                                 "Hello b from a".as_bytes(),
                                                 5000).await;
                                    println!("######response from b, index: {:?}, r: {:?}", index, r);
                                }

                                peer_port.close();
                                while let Ok(event) = service_a_copy.poll_event().await {
                                    println!("!!!!!!poll_event, local: {:?}, event: {:?}", service_a_copy.local_host().unwrap(), event);
                                }
                            });

                            while let Ok((from, index, bytes)) = pipeline.poll().await {
                                if let Some((peer, reply_peer)) = from {
                                    if let Err(e) = reply_peer.reply("Hi b") {
                                        println!("!!!!!!reply failed, peer: {:?}, index: {:?}, reason: {:?}", peer, index, e);
                                    }
                                } else {
                                    println!("!!!!!!from: None, index: {:?}, msg: {:?}",
                                             index,
                                             String::from_utf8_lossy(bytes.as_ref()));
                                }
                            }
                        },
                    }
                },
            }
        });

        let rt_clone = rt_copy.clone();
        rt_copy.spawn(rt_copy.alloc(), async move {
            match service_b.open_port(8081, Some("test port"), PortMode::Send) {
                Err(e) => {
                    println!("!!!!!!open service b port failed, reason: {:?}", e);
                },
                Ok((self_port, pipeline)) => {
                    println!("!!!!!!open service b port ok\n\tpeer_port: {:#?}\n\tpipeline: {:#?}\n\tports: {:#?}",
                             self_port,
                             pipeline,
                             service_b.local_ports());

                    match service_b
                        .connect_to(&host_a,
                                    8080,
                                    5000)
                        .await {
                        Err(e) => {
                            println!("!!!!!!connect to peer service port failed, peer: {:?}, port: 8080, reason: {:?}",
                                     host_a,
                                     e);
                        },
                        Ok(peer_port) => {
                            self_port.send(0, "Hello b from b".as_bytes());

                            let service_b_copy = service_b.clone();
                            rt_clone.spawn(rt_clone.alloc(), async move {
                                while let Ok(event) = service_b_copy.poll_event().await {
                                    if let PortEvent::Handshaked(peer) = event {
                                        println!("!!!!!!handshaked, peer: {:?}", peer);
                                        if peer_port.peer().unwrap() == peer {
                                            //与端口所在的对端主机已握手
                                            break;
                                        }
                                    }
                                }

                                for index in 0..10 {
                                    let r = peer_port
                                        .request(index,
                                                 "Hello a from b".as_bytes(),
                                                 5000).await;
                                    println!("######response from a, index: {:?}, r: {:?}", index, r);
                                }

                                peer_port.close();
                                while let Ok(event) = service_b_copy.poll_event().await {
                                    println!("!!!!!!poll_event, local: {:?}, event: {:?}", service_b_copy.local_host().unwrap(), event);
                                }
                            });

                            while let Ok((from, index, bytes)) = pipeline.poll().await {
                                if let Some((peer, reply_peer)) = from {
                                    if let Err(e) = reply_peer.reply("Hi a") {
                                        println!("!!!!!!reply failed, peer: {:?}, index: {:?}, reason: {:?}", peer, index, e);
                                    }
                                } else {
                                    println!("!!!!!!from: None, index: {:?}, msg: {:?}",
                                             index,
                                             String::from_utf8_lossy(bytes.as_ref()));
                                }
                            }
                        },
                    }
                },
            }
        });
    });

    thread::sleep(Duration::from_millis(1000000000));
}


