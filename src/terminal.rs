use std::ops::Add;
use std::str::FromStr;
use std::future::Future;
use std::fs::{File, read};
use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant, SystemTime};
use std::sync::{Arc,
                atomic::{AtomicBool, AtomicUsize, Ordering}};
use std::io::{BufRead, BufReader, Result, Error, ErrorKind, Cursor};

use futures::future::{BoxFuture, LocalBoxFuture};
use parking_lot::RwLock;
use async_lock::Mutex;
use dashmap::DashMap;
use crossbeam_channel::{Sender, Receiver, unbounded};
use rustls;
use rustls_pemfile::{certs, pkcs8_private_keys};
use x509_parser::{pem::Pem,
                  public_key::PublicKey};
use curve25519_parser::parse_openssl_25519_privkey;
use ciborium::{Value,
               value::Integer};
use futures::FutureExt;
use quinn_proto::{EndpointConfig, TransportConfig, Dir, StreamId, VarInt};
use bytes::BytesMut;
use der_parser::nom::Or;
use der_parser::nom::sequence::terminated;
use rand::prelude::*;
use log::{debug, info, error};
use pi_async::prelude::MultiTaskRuntime;

use pi_atom::Atom;
use pi_async::rt::{AsyncRuntime,
                   serial_local_thread::LocalTaskRuntime};
use pi_gossip::{FromPublicKey, GossipNodeID, GossipContext,
                summary::SummaryVesrion,
                increase::IncreaseVesrion,
                transport::{Transport, GossipTid, GossipSendBinary, HandlePullBinary, HandlePushPullBinary},
                service::{PullBehavior, PushBehavior, PushPullBehavior, ServiceContext, SyncStatistics},
                scuttlebutt::{phi::{PhiFailureDetector, PhiFailureDetectorConfig},
                              table::Key,
                              core::{DEFAULT_PULL_SYNC_REQ_TAG, DEFAULT_PULL_SYNC_ACK_TAG, DEFAULT_PUSH_SYNC_TAG,
                                     Scuttlebutt}}};
use udp::terminal::UdpTerminal;
use quic::{AsyncService, SocketHandle, SocketEvent,
           connect::QuicSocket,
           client::{QuicClient, ServerCertVerifyLevel},
           server::{QuicListener, ClientCertVerifyLevel},
           utils::{QuicSocketReady, load_key_file}};

use crate::{crypto::{ClientCertVerifier, ServerCertVerifier, host_id_to_host_dns},
            connection::{Connection, PeerSocketHandle, Channel},
            frame::{P2PFrame, P2PHeartBeatInfo},
            service::{DEFAULT_HEARTBEAT_TAG, P2PService, P2PServiceAdapter, P2PTransporter}};
use crate::connection::ChannelId;
use crate::service::P2PServiceListener;

// 默认的Udp本地ip
const DEFAULT_LOCAL_UDP_IP: Ipv4Addr = Ipv4Addr::new(0, 0, 0, 0);

// 默认的Udp本地端口
const DEFAULT_LOCAL_UDP_PORT: u16 = 0;

// 默认的UDP接收缓冲区大小
const DEFAULT_UDP_RECV_BUF_SIZE: usize = 8 * 1024 * 1024;

// 默认的UDP发送缓冲区大小
const DEFAULT_UDP_SENT_BUF_SIZE: usize = 8 * 1024 * 1024;

// 默认的UDP接收块大小
const DEFAULT_UDP_RECV_BLOCK_SIZE: usize = 0xffff;

// 默认的UDP发送块大小
const DEFAULT_UDP_SENT_BLOCK_SIZE: usize = 0xffff;

// 默认的最大通道数量
const DEFAULT_MAX_CHANNEL_SIZE: u32 = 0xffff;

// 默认的QUIC接收帧大小
const DEFAULT_QUIC_RECV_FRAME_SIZE: usize = 0xffff;

// 默认的QUIC发送帧大小
const DEFAULT_QUIC_SENT_FRAME_SIZE: usize = 0xffff;

// 默认的QUIC最大连接数
const DEFAULT_MAX_CONNECTION_SIZE: u32 = 8192;

// 默认的QUIC连接超时时长，单位ms
const DEFAULT_QUIC_CONNECT_TIMEOUT: usize = 5000;

// 默认的同步超时时长，单位ms
const DEFAULT_SYNC_TIMEOUT: usize = 5000;

// 默认的P2P主机本地心跳间隔时间
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(5000);

// 默认的P2P主机本地整理间隔时间
const DEFAULT_COLLECT_INTERVAL: Duration = Duration::from_millis(30000);

// 默认的最小P2P主机阻塞发送心中的时间
const DEFAULT_MIN_BLOCK_HEARTBEAT_TIME: Duration = Duration::from_millis(5000);

// 默认的心跳属性关键字字符串
const DEFAULT_HEARTBEAT_KEY_STR: &str = "$heartbeat";

pub(crate) const DEFAULT_SEED_HOST_KEY_STR: &str = "$seed";

/// 默认的P2P主机ip地址关键字字符串
pub const DEFAULT_HOST_IP_KEY_STR: &str = "ip";

/// 默认的P2P主机端口关键字字符串
pub const DEFAULT_HOST_PORT_KEY_STR: &str = "port";

///
/// P2P终端构建器
///
pub struct TerminalBuilder {
    terminal_runtime:               Option<MultiTaskRuntime<()>>,       //P2P终端运行时
    server_udp_runtime:             Option<LocalTaskRuntime<()>>,       //服务端Udp运行时
    server_local_udp_address:       Option<SocketAddr>,                 //服务端Udp本地地址
    server_udp_recv_buf_size:       Option<usize>,                      //服务端Udp接收缓冲区大小
    server_udp_sent_buf_size:       Option<usize>,                      //服务端Udp发送缓冲区大小
    server_udp_recv_block_size:     Option<usize>,                      //服务端Udp接收块大小
    server_udp_sent_block_size:     Option<usize>,                      //服务端Udp发送块大小
    server_quic_runtimes:           Option<Vec<LocalTaskRuntime<()>>>,  //服务端Quic运行时列表
    server_ca_cert_file:            Option<PathBuf>,                    //种子主机服务端CA证书文件
    server_cert_file:               Option<PathBuf>,                    //服务端证书文件
    server_key_file:                Option<PathBuf>,                    //服务端私钥文件
    server_verify_level:            Option<ClientCertVerifyLevel>,      //服务端使用的客户端证书验证等级
    server_quic_config:             Option<EndpointConfig>,             //服务端Quic端点配置
    server_quic_transport_config:   Option<TransportConfig>,            //服务端Quic传输配置
    server_quic_recv_frame_size:    Option<usize>,                      //服务端Quic接收帧大小
    server_quic_sent_frame_size:    Option<usize>,                      //服务端Quic发送帧大小
    server_max_connections_size:    Option<u32>,                        //服务端Quic最大连接数
    client_udp_runtime:             Option<LocalTaskRuntime<()>>,       //客户端Udp运行时
    client_local_udp_address:       Option<SocketAddr>,                 //客户端Udp本地地址
    client_udp_recv_buf_size:       Option<usize>,                      //客户端Udp接收缓冲区大小
    client_udp_sent_buf_size:       Option<usize>,                      //客户端Udp发送缓冲区大小
    client_udp_recv_block_size:     Option<usize>,                      //客户端Udp接收块大小
    client_udp_sent_block_size:     Option<usize>,                      //客户端Udp发送块大小
    client_quic_runtimes:           Option<Vec<LocalTaskRuntime<()>>>,  //客户端Quic运行时列表
    client_verify_level:            Option<ServerCertVerifyLevel>,      //客户端使用的服务端证书验证等级
    client_quic_config:             Option<EndpointConfig>,             //客户端Quic端点配置
    client_quic_transport_config:   Option<TransportConfig>,            //客户端Quic传输配置
    client_quic_recv_frame_size:    Option<usize>,                      //客户端Quic接收帧大小
    client_quic_sent_frame_size:    Option<usize>,                      //客户端Quic发送帧大小
    client_connect_timeout:         Option<usize>,                      //客户端Quic连接超时时长
    seed_hosts:                     BTreeMap<Atom, SocketAddr>,         //种子主机表
    is_seed:                        bool,                               //是否声明本地主机为种子主机
    failure_detector_config:        PhiFailureDetectorConfig,           //P2P对端故障侦听器配置
    blocking_timeout:               Duration,                           //阻塞发送心跳的时间
    service:                        Option<Arc<P2PServiceAdapter>>,     //P2P终端服务
    version:                        (u8, u8, u16),                      //P2P服务版本
}

impl Default for TerminalBuilder {
    /// 构建一个默认的P2P终端构建器
    fn default() -> Self {
        let terminal_runtime = None;

        let server_udp_runtime = None;
        let server_local_udp_address = Some(SocketAddr::new(IpAddr::V4(DEFAULT_LOCAL_UDP_IP), DEFAULT_LOCAL_UDP_PORT));
        let server_udp_recv_buf_size = Some(DEFAULT_UDP_RECV_BUF_SIZE);
        let server_udp_sent_buf_size = Some(DEFAULT_UDP_SENT_BUF_SIZE);
        let server_udp_recv_block_size = Some(DEFAULT_UDP_RECV_BLOCK_SIZE);
        let server_udp_sent_block_size = Some(DEFAULT_UDP_SENT_BLOCK_SIZE);

        let server_quic_runtimes = None;
        let server_ca_cert_file = None;
        let server_cert_file = None;
        let server_key_file = None;
        let server_verify_level = Some(ClientCertVerifyLevel::Ignore);
        let server_quic_config = Some(EndpointConfig::default());
        let mut transport_config = TransportConfig::default();
        transport_config.max_concurrent_bidi_streams(VarInt::from_u32(DEFAULT_MAX_CHANNEL_SIZE));
        let server_quic_transport_config = Some(transport_config);
        let server_quic_recv_frame_size = Some(DEFAULT_QUIC_RECV_FRAME_SIZE);
        let server_quic_sent_frame_size = Some(DEFAULT_QUIC_SENT_FRAME_SIZE);
        let server_max_connections_size = Some(DEFAULT_MAX_CONNECTION_SIZE);

        let client_udp_runtime = None;
        let client_local_udp_address = Some(SocketAddr::new(IpAddr::V4(DEFAULT_LOCAL_UDP_IP), DEFAULT_LOCAL_UDP_PORT));
        let client_udp_recv_buf_size = Some(DEFAULT_UDP_RECV_BUF_SIZE);
        let client_udp_sent_buf_size = Some(DEFAULT_UDP_SENT_BUF_SIZE);
        let client_udp_recv_block_size = Some(DEFAULT_UDP_RECV_BLOCK_SIZE);
        let client_udp_sent_block_size = Some(DEFAULT_UDP_SENT_BLOCK_SIZE);

        let client_quic_runtimes = None;
        let client_verify_level = Some(ServerCertVerifyLevel::Ignore);
        let client_quic_config = Some(EndpointConfig::default());
        let mut transport_config = TransportConfig::default();
        transport_config.max_concurrent_bidi_streams(VarInt::from_u32(DEFAULT_MAX_CHANNEL_SIZE));
        let client_quic_transport_config = Some(transport_config);
        let client_quic_recv_frame_size = Some(DEFAULT_QUIC_RECV_FRAME_SIZE);
        let client_quic_sent_frame_size = Some(DEFAULT_QUIC_SENT_FRAME_SIZE);
        let client_connect_timeout = Some(DEFAULT_QUIC_CONNECT_TIMEOUT);
        let seed_hosts = BTreeMap::new();
        let is_seed = false;
        let failure_detector_config = PhiFailureDetectorConfig::default();
        let blocking_timeout = DEFAULT_MIN_BLOCK_HEARTBEAT_TIME;
        let service = None;
        let version = (0, 0, 0);

        TerminalBuilder {
            terminal_runtime,
            server_udp_runtime,
            server_local_udp_address,
            server_udp_recv_buf_size,
            server_udp_sent_buf_size,
            server_udp_recv_block_size,
            server_udp_sent_block_size,
            server_quic_runtimes,
            server_ca_cert_file,
            server_cert_file,
            server_key_file,
            server_verify_level,
            server_quic_config,
            server_quic_transport_config,
            server_quic_recv_frame_size,
            server_quic_sent_frame_size,
            server_max_connections_size,
            client_udp_runtime,
            client_local_udp_address,
            client_udp_recv_buf_size,
            client_udp_sent_buf_size,
            client_udp_recv_block_size,
            client_udp_sent_block_size,
            client_quic_runtimes,
            client_verify_level,
            client_quic_config,
            client_quic_transport_config,
            client_quic_recv_frame_size,
            client_quic_sent_frame_size,
            client_connect_timeout,
            seed_hosts,
            is_seed,
            failure_detector_config,
            blocking_timeout,
            service,
            version,
        }
    }
}

/*
* P2P终端构建器同步方法
*/
impl TerminalBuilder {
    /// 为P2P终端绑定运行时
    pub fn bind_runtime(mut self,
                        terminal_runtime: MultiTaskRuntime<()>,
                        server_udp_runtime: LocalTaskRuntime<()>,
                        server_quic_runtimes: Vec<LocalTaskRuntime<()>>,
                        client_udp_runtime: LocalTaskRuntime<()>,
                        client_quic_runtimes: Vec<LocalTaskRuntime<()>>) -> Self {
        self.terminal_runtime = Some(terminal_runtime);
        self.server_udp_runtime = Some(server_udp_runtime);
        self.server_quic_runtimes = Some(server_quic_runtimes);
        self.client_udp_runtime = Some(client_udp_runtime);
        self.client_quic_runtimes = Some(client_quic_runtimes);

        self
    }

    /// 为P2P终端绑定种子主机CA证书，本地主机证书和本地主机私钥
    pub fn bind_terminal_cert_and_key<P: AsRef<Path>>(mut self,
                                                      ca_cert_file: P,
                                                      cert_file: P,
                                                      key_file: P) -> Self {
        if !ca_cert_file.as_ref().exists()
            || !ca_cert_file.as_ref().is_file() {
            panic!("Bind seed host ca cert, cert_file: {:?}, reason: invalid cert file",
                   ca_cert_file.as_ref());
        }

        if !cert_file.as_ref().exists()
            || !cert_file.as_ref().is_file() {
            panic!("Bind server cert, cert_file: {:?}, reason: invalid cert file",
                   cert_file.as_ref());
        }

        if !key_file.as_ref().exists()
            || !key_file.as_ref().is_file() {
            panic!("Bind server key, key_file: {:?}, reason: invalid key file",
                   key_file.as_ref());
        }

        self.server_ca_cert_file = Some(ca_cert_file.as_ref().to_path_buf());
        self.server_cert_file = Some(cert_file.as_ref().to_path_buf());
        self.server_key_file = Some(key_file.as_ref().to_path_buf());

        self
    }

    /// 为P2P终端绑定服务端Udp本地地址
    pub fn bind_server_local_udp_address(mut self, address: SocketAddr) -> Self {
        self.server_local_udp_address = Some(address);
        self
    }

    /// 为P2P终端设置服务端Udp接收缓冲区大小
    pub fn set_server_udp_recv_buf_size(mut self, size: usize) -> Self {
        if size == 0 {
            panic!("Set server udp recv buf size, reason: invalid size");
        }
        self.server_udp_recv_buf_size = Some(size);

        self
    }

    /// 为P2P终端设置服务端Udp发送缓冲区大小
    pub fn set_server_udp_sent_buf_size(mut self, size: usize) -> Self {
        if size == 0 {
            panic!("Set server udp sent buf size, reason: invalid size");
        }
        self.server_udp_sent_buf_size = Some(size);

        self
    }

    /// 为P2P终端设置服务端Udp接收块大小
    pub fn set_server_udp_recv_block_size(mut self, size: usize) -> Self {
        if size == 0 {
            panic!("Set server udp recv block size, reason: invalid size");
        }
        self.server_udp_recv_block_size = Some(size);

        self
    }

    /// 为P2P终端设置服务端Udp发送块大小
    pub fn set_server_udp_sent_block_size(mut self, size: usize) -> Self {
        if size == 0 {
            panic!("Set server udp sent block size, reason: invalid size");
        }
        self.server_udp_sent_block_size = Some(size);

        self
    }

    /// 为P2P终端设置的服务端设置客户端证书验证等级
    pub fn set_client_verify_level_to_server<P: AsRef<Path>>(mut self,
                                                             ca_cert_file: Option<P>) -> Self {
        let level = if let Some(cert_file) = ca_cert_file {
            let verifier = ClientCertVerifier::new(cert_file)
                .expect("Set client verify level failed");
            ClientCertVerifyLevel::Custom(Arc::new(verifier))
        } else {
            ClientCertVerifyLevel::Ignore
        };
        self.server_verify_level = Some(level);

        self
    }

    /// 为P2P终端设置服务端Quic的端点配置
    pub fn set_server_quic_config(mut self, config: EndpointConfig) -> Self {
        self.server_quic_config = Some(config);

        self
    }

    /// 为P2P终端设置服务端Quic的传输配置
    pub fn set_server_quic_transport_config(mut self, config: TransportConfig) -> Self {
        self.server_quic_transport_config = Some(config);

        self
    }

    /// 为P2P终端设置服务端Quic接收帧大小
    pub fn set_server_quic_recv_frame_size(mut self, size: usize) -> Self {
        if size == 0 {
            panic!("Set server quic recv frame size, reason: invalid size");
        }
        self.server_quic_recv_frame_size = Some(size);

        self
    }

    /// 为P2P终端设置服务端Quic发送帧大小
    pub fn set_server_quic_sent_frame_size(mut self, size: usize) -> Self {
        if size == 0 {
            panic!("Set server quic sent frame size, reason: invalid size");
        }
        self.server_quic_sent_frame_size = Some(size);

        self
    }

    /// 为P2P终端设置服务端Quic连接最大数量
    pub fn set_server_max_connections_size(mut self, max: u32) -> Self {
        if max == 0 {
            panic!("Set server max connections size, reason: invalid size");
        }
        self.server_max_connections_size = Some(max);

        self
    }

    /// 为P2P终端绑定客户端Udp本地地址
    pub fn bind_client_local_udp_address(mut self, address: SocketAddr) -> Self {
        self.client_local_udp_address = Some(address);
        self
    }

    /// 为P2P终端设置客户端Udp接收缓冲区大小
    pub fn set_client_udp_recv_buf_size(mut self, size: usize) -> Self {
        if size == 0 {
            panic!("Set client udp recv buf size, reason: invalid size");
        }
        self.client_udp_recv_buf_size = Some(size);

        self
    }

    /// 为P2P终端设置客服端Udp发送缓冲区大小
    pub fn set_client_udp_sent_buf_size(mut self, size: usize) -> Self {
        if size == 0 {
            panic!("Set client udp sent buf size, reason: invalid size");
        }
        self.client_udp_sent_buf_size = Some(size);

        self
    }

    /// 为P2P终端设置客户端Udp接收块大小
    pub fn set_client_udp_recv_block_size(mut self, size: usize) -> Self {
        if size == 0 {
            panic!("Set client udp recv block size, reason: invalid size");
        }
        self.client_udp_recv_block_size = Some(size);

        self
    }

    /// 为P2P终端设置客户端Udp发送块大小
    pub fn set_client_udp_sent_block_size(mut self, size: usize) -> Self {
        if size == 0 {
            panic!("Set client udp sent block size, reason: invalid size");
        }
        self.client_udp_sent_block_size = Some(size);

        self
    }

    /// 允许P2P终端设置客户端的服务端证书验证等级
    pub fn enable_server_verify_level_to_client(mut self) -> Self {
        let level = if let Some(cert_file) = &self.server_ca_cert_file {
            let verifier = ServerCertVerifier::new(cert_file)
                .expect("Set server verify level failed");
            ServerCertVerifyLevel::Custom(Arc::new(verifier))
        } else {
            ServerCertVerifyLevel::Ignore
        };
        self.client_verify_level = Some(level);

        self
    }

    /// 为P2P终端设置客户端Quic的端点配置
    pub fn set_client_quic_config(mut self, config: EndpointConfig) -> Self {
        self.client_quic_config = Some(config);

        self
    }

    /// 为P2P终端设置客户端Quic的传输配置
    pub fn set_client_quic_transport_config(mut self, config: TransportConfig) -> Self {
        self.client_quic_transport_config = Some(config);

        self
    }

    /// 为P2P终端设置客户端Quic接收帧大小
    pub fn set_client_quic_recv_frame_size(mut self, size: usize) -> Self {
        if size == 0 {
            panic!("Set client quic recv frame size, reason: invalid size");
        }
        self.client_quic_recv_frame_size = Some(size);

        self
    }

    /// 为P2P终端设置客户端Quic发送帧大小
    pub fn set_client_quic_sent_frame_size(mut self, size: usize) -> Self {
        if size == 0 {
            panic!("Set client quic sent frame size, reason: invalid size");
        }
        self.client_quic_sent_frame_size = Some(size);

        self
    }

    /// 为P2P终端设置客户端Quic连接超时时长，单位ms
    pub fn set_client_connect_timeout(mut self, timeout: usize) -> Self {
        if timeout == 0 {
            panic!("Set client quic connect timeout, reason: invalid time");
        }
        self.client_connect_timeout = Some(timeout);

        self
    }

    /// 为P2P终端增加种子主机地址
    pub fn add_seed_host_address(mut self,
                                 hostname: &str,
                                 ip: IpAddr,
                                 port: u16) -> Self {
        if hostname.is_empty() {
            panic!("Add seed host failed, hostname: {:?}, reason: require valided hostname",
                   hostname);
        }

        //注册种子主机
        let key = Atom::from(hostname);
        let value = SocketAddr::new(ip, port);
        self
            .seed_hosts
            .insert(key, value);

        self
    }

    /// 声明本地主机为种子主机
    pub fn enable_seed_local_host(mut self) -> Self {
        self.is_seed = true;

        self
    }

    /// 设置P2P对端故障侦听器配置
    pub fn set_failure_detector_config(mut self,
                                       phi_threshold: f64,
                                       sampling_window_size: usize,
                                       max_interval: Duration,
                                       initial_interval: Duration,
                                       unlive_node_grace_period: Duration) -> Self {
        self.failure_detector_config = PhiFailureDetectorConfig::new(phi_threshold,
                                                                     sampling_window_size,
                                                                     max_interval,
                                                                     initial_interval,
                                                                     unlive_node_grace_period);

        self
    }

    /// 设置P2P终端的阻塞发送心跳的时间
    pub fn set_blocking_heartbeat_timeout(mut self, timeout: usize) -> Self {
        let timeout = if timeout < DEFAULT_MIN_BLOCK_HEARTBEAT_TIME.as_millis() as usize {
            DEFAULT_MIN_BLOCK_HEARTBEAT_TIME
        } else {
            Duration::from_millis(timeout as u64)
        };
        self.blocking_timeout = timeout;

        self
    }

    /// 为P2P终端设置P2P服务
    pub fn set_service(mut self, service: Arc<P2PServiceAdapter>) -> Self {
        self.service = Some(service);

        self
    }

    /// 为P2P终端设置服务版本
    pub fn set_version(mut self, x: u8, y: u8, z: u16) -> Self {
        self.version = (x, y, z);

        self
    }
}

/*
* P2P终端构建器异步方法
*/
impl TerminalBuilder {
    /// 异步构建一个P2P终端
    pub async fn build(self) -> Result<Terminal> {
        if self.terminal_runtime.is_none()
            || self.server_udp_runtime.is_none()
            || self.server_quic_runtimes.is_none()
            || self.client_udp_runtime.is_none()
            || self.client_quic_runtimes.is_none() {
            //没有绑定运行时，则立即返回错误原因
            return Err(Error::new(ErrorKind::Other,
                                  format!("Build terminal failed, reason: require runtime")));
        }
        let rt = self.terminal_runtime.unwrap();

        if self.server_cert_file.is_none() {
            //没有设置服务端证书文件，则立即返回错误原因
            return Err(Error::new(ErrorKind::Other,
                                  format!("Build terminal failed, reason: require server cert file")));
        }

        if self.server_key_file.is_none() {
            //没有设置服务端私钥文件，则立即返回错误原因
            return Err(Error::new(ErrorKind::Other,
                                  format!("Build terminal failed, reason: require server key file")));
        }

        if self.service.is_none() {
            //没有设置P2P终端服务，则立即返回错误原因
            return Err(Error::new(ErrorKind::Other,
                                  format!("Build terminal failed, reason: require service")));
        }
        let service = self.service.unwrap();

        if let &(0, 0, 0) = &self.version {
            //没有设置有效的P2P服务版本，则立即返回错误原因
            return Err(Error::new(ErrorKind::Other,
                                  format!("Build terminal failed, reason: require version")));
        }

        //创建服务端Quic监听器
        let server_cert_file = self.server_cert_file.unwrap();
        let server_key_file = self.server_key_file.unwrap();
        let listener = QuicListener::new(self.server_quic_runtimes.unwrap(),
                                         server_cert_file.clone(),
                                         server_key_file.clone(),
                                         self.server_verify_level.unwrap(),
                                         self.server_quic_config.unwrap(),
                                         self.server_quic_recv_frame_size.unwrap(),
                                         self.server_quic_sent_frame_size.unwrap(),
                                         self.server_max_connections_size.unwrap(),
                                         Some(Arc::new(self.server_quic_transport_config.unwrap())),
                                         service.clone(),
                                         1)?;

        //创建服务端Udp终端
        let local_address = self.server_local_udp_address.unwrap();
        let _ = UdpTerminal::bind(local_address.clone(),
                                  self.server_udp_runtime.unwrap(),
                                  self.server_udp_recv_buf_size.unwrap(),
                                  self.server_udp_sent_buf_size.unwrap(),
                                  self.server_udp_recv_block_size.unwrap(),
                                  self.server_udp_sent_block_size.unwrap(),
                                  Box::new(listener))?;

        //创建Quic客户端
        let client = QuicClient::new(self.client_local_udp_address.unwrap(),
                                     self.client_udp_runtime.unwrap(),
                                     self.client_quic_runtimes.unwrap(),
                                     self.client_verify_level.unwrap(),
                                     self.client_quic_config.unwrap(),
                                     self.client_quic_recv_frame_size.unwrap(),
                                     self.client_quic_sent_frame_size.unwrap(),
                                     self.client_udp_recv_buf_size.unwrap(),
                                     self.client_udp_sent_buf_size.unwrap(),
                                     self.client_udp_recv_block_size.unwrap(),
                                     self.client_udp_sent_block_size.unwrap(),
                                     Some(Arc::new(self.client_quic_transport_config.unwrap())),
                                     Some(service.clone()),
                                     self.client_connect_timeout.unwrap(),
                                     1)?;

        //创建P2P服务行为
        let (x, y, z) = self.version;
        let mut buf_reader = BufReader::new(File::open(&server_cert_file)?);
        let key_bytes = match certs(&mut buf_reader) {
            Err(e) => {
                return Err(Error::new(ErrorKind::Other,
                                      format!("Build terminal failed, cert_file: {:?}, reason: {:?}",
                                              server_cert_file,
                                              e)));
            },
            Ok(mut cert_vec) => {
                let mut cert_bytes = cert_vec.remove(0);
                let pem = Pem {
                    label: String::new(),
                    contents: cert_bytes,
                };

                let mut pub_bytes = match pem.parse_x509() {
                    Err(e) => {
                        return Err(Error::new(ErrorKind::Other,
                                              format!("Build terminal failed, cert_file: {:?}, reason: {:?}",
                                                      server_cert_file,
                                                      e)));
                    },
                    Ok(cert) => {
                        match cert.public_key().parsed() {
                            Err(e) => {
                                return Err(Error::new(ErrorKind::Other,
                                                      format!("Build terminal failed, cert_file: {:?}, reason: {:?}",
                                                              server_cert_file,
                                                              e)));
                            },
                            Ok(public) => {
                                match public {
                                    PublicKey::Unknown(bin) => bin.to_vec(),
                                    _ => {
                                        return Err(Error::new(ErrorKind::Other,
                                                              format!("Build terminal failed, cert_file: {:?}, reason: invalid public key",
                                                                      server_cert_file)));
                                    },
                                }
                            },
                        }
                    },
                };

                match read(&server_key_file) {
                    Err(e) => {
                        return Err(Error::new(ErrorKind::Other,
                                              format!("Build terminal failed, key_file: {:?}, reason: {:?}",
                                                      server_key_file,
                                                      e)));
                    },
                    Ok(bin) => {
                        // match parse_openssl_25519_privkey(bin.as_slice()) {
                        //     Err(e) => {
                        //         return Err(Error::new(ErrorKind::Other,
                        //                               format!("Build terminal failed, key_file: {:?}, reason: {:?}",
                        //                                       server_key_file,
                        //                                       e)));
                        //     },
                        //     Ok(secret) => {
                        //         let mut key_bytes = secret.to_bytes().to_vec();
                        //         key_bytes.append(&mut pub_bytes);
                        //
                        //         key_bytes
                        //     },
                        // }

                        //直接从openssl的私钥文件中获取ED25519的私钥
                        let mut buf_reader = BufReader::new(Cursor::new(bin));
                        let mut key_bytes = (&pkcs8_private_keys(&mut buf_reader).unwrap().remove(0)[16..]).to_vec();
                        key_bytes.append(&mut pub_bytes);

                        key_bytes
                    },
                }
            },
        };
        let uptime = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
        let rng = Mutex::new(SmallRng::seed_from_u64(uptime.as_millis() as u64));
        let transporter = P2PTransporter::new(service.clone());
        let failure_detector = Arc::new(Mutex::new(PhiFailureDetector::new(self.failure_detector_config)));
        let listener = Arc::new(P2PServiceListener::new(rt.clone(), service.clone()));
        let behavior = Scuttlebutt::with_key_and_attrs(key_bytes,
                                                       uptime,
                                                       SummaryVesrion::new(x, y, z),
                                                       IncreaseVesrion::new(x, y, z),
                                                       vec![],
                                                       vec![],
                                                       transporter.clone(),
                                                       failure_detector.clone(),
                                                       Some(listener.clone())).unwrap();
        let local = behavior.get_id().clone();
        let local_copy = local.clone();
        let (frames_sent, frames_recv) = unbounded();
        let seed_hosts = RwLock::new(self.seed_hosts);
        let heartbeat_blocking = DashMap::new();
        let blocking_timeout = self.blocking_timeout;
        let heartbeat_interval = Arc::new(AtomicUsize::new(DEFAULT_HEARTBEAT_INTERVAL.as_millis() as usize));
        let collect_interval = Arc::new(AtomicUsize::new(DEFAULT_COLLECT_INTERVAL.as_millis() as usize));
        let heartbeating_pause = Arc::new(AtomicBool::new(false));
        let collecting_pause = Arc::new(AtomicBool::new(false));

        let inner = InnerTerminal {
            rt,
            local,
            client,
            seed_connections: DashMap::new(),
            connections: DashMap::new(),
            peer_binds: DashMap::new(),
            channels: DashMap::new(),
            channels_len_map: DashMap::new(),
            behavior,
            transporter,
            failure_detector,
            uptime,
            rng,
            frames_recv,
            frames_sent,
            seed_hosts,
            heartbeat_blocking,
            blocking_timeout,
            heartbeat_interval,
            collect_interval,
            heartbeating_pause,
            collecting_pause,
        };
        let terminal = Terminal(Arc::new(inner));

        //设置本地主机的基础属性
        if self.is_seed {
            //声明本地主机为种子主机
            terminal.set_local_host_attr(Key::from(DEFAULT_SEED_HOST_KEY_STR),
                                         Value::Bool(true));
        } else {
            //声明本地主机为非种子主机
            terminal.set_local_host_attr(Key::from(DEFAULT_SEED_HOST_KEY_STR),
                                         Value::Bool(false));
        }
        terminal.set_local_host_attr(Key::from(DEFAULT_HOST_IP_KEY_STR),
                                     Value::Text(local_address.ip().to_string()));
        terminal.set_local_host_attr(Key::from(DEFAULT_HOST_PORT_KEY_STR),
                                     Value::from(local_address.port()));

        //通知P2P服务本地终端已初始化
        service
            .get_service()
            .init(local_copy,
                  terminal.clone(),
                  listener);

        Ok(terminal)
    }
}

///
/// P2P终端
///
pub struct Terminal(Arc<InnerTerminal>);

impl Clone for Terminal {
    fn clone(&self) -> Self {
        Terminal(self.0.clone())
    }
}

/*
* P2P终端同步方法
*/
impl Terminal {
    /// 获取本地主机唯一id
    pub fn get_self(&self) -> &GossipNodeID {
        &self.0.local
    }

    /// 获取所有已知主机
    pub fn all_hosts(&self) -> Vec<GossipNodeID<32>> {
        let table = self.0.behavior.get_table();

        let mut hosts = Vec::new();
        for node in table.private_nodes() {
            if let Ok(host_id) = GossipNodeID::try_from(node.as_str().to_string()) {
                if &host_id == self.get_self() {
                    //忽略本地主机
                    continue;
                }

                //加入主机
                hosts.push(host_id);
            }
        }

        hosts
    }

    /// 获取过滤后的所有已知主机
    pub fn all_hosts_filter(&self,
                            filter: impl Fn(&GossipNodeID<32>) -> bool)
        -> Vec<GossipNodeID<32>>
    {
        let table = self.0.behavior.get_table();

        let mut hosts = Vec::new();
        for node in table.private_nodes() {
            if let Ok(host_id) = GossipNodeID::try_from(node.as_str().to_string()) {
                if &host_id == self.get_self()
                    || filter(&host_id) {
                    //忽略本地主机和指定的主机
                    continue;
                }

                //加入主机
                hosts.push(host_id);
            }
        }

        hosts
    }

    ///获取所有种子主机，忽略本地主机
    pub fn all_seeds(&self) -> Vec<GossipNodeID<32>> {
        let mut hosts = Vec::new();
        for (hostname, _address) in self.0.seed_hosts.read().iter() {
            if hostname.as_str() == self.get_self().get_name().as_str() {
                //忽略本地主机
                continue;
            }

            //加入种子主机
            hosts.push(GossipNodeID::try_from(hostname.as_str().to_string() + "/0").unwrap());
        }

        hosts
    }

    /// 获取所有已连接主机的唯一id列表
    pub fn all_connected(&self) -> Vec<GossipNodeID<32>> {
        let mut vec = Vec::new();
        let mut iter = self.0.connections.iter();
        for item in iter {
            vec.push(item.key().clone());
        }

        vec
    }

    /// 获取指定种子主机的地址
    pub fn get_seed_host_address(&self, hostname: &str) -> Option<SocketAddr> {
        if let Some(address) = self.0.seed_hosts.read().get(&Atom::from(hostname)) {
            Some(address.clone())
        } else {
            None
        }
    }

    /// 设置指定种子主机的地址，返回上个地址
    pub fn set_seed_host_address(&self,
                                 hostname: &str,
                                 ip: IpAddr,
                                 port: u16) -> Option<SocketAddr> {
        self
            .0
            .seed_hosts
            .write()
            .insert(Atom::from(hostname),
                    SocketAddr::new(ip, port))
    }

    /// 移除指定种子主机的地址，返回被移除的地址
    pub fn remove_seed_host_address(&self, hostname: &str) -> Option<SocketAddr> {
        self
            .0
            .seed_hosts
            .write()
            .remove(&Atom::from(hostname))
    }

    /// 判断指定主机唯一id的P2P连接是否已建立
    /// 判断一个节点是否有问题至少需要通过以下三个参数:
    ///     1.指定主机的Quic连接状态: is_connected
    ///     2.指定主机的Quic连接的通讯延迟: get_connection_delay
    ///     3.主机的故障估值: failure_valuation
    pub fn is_connected(&self, host_id: &GossipNodeID) -> bool {
        self
            .get_connection(host_id)
            .is_some()
    }

    /// 获取指定主机唯一id的P2P连接
    pub fn get_connection(&self, host_id: &GossipNodeID) -> Option<Connection> {
        if host_id.get_time() == &0 {
            //是未握手的种子主机
            let hostname = host_id.get_name(); //种子主机只需要主机名
            if let Some(item) = self.0.seed_connections.get(hostname.as_str()) {
                let connection = item.value().clone();
                Some(connection)
            } else {
                None
            }
        } else {
            if let Some(item) = self.0.connections.get(host_id) {
                let connection = item.value().clone();
                Some(connection)
            } else {
                None
            }
        }
    }

    /// 获取指定主机的P2P连接延迟
    /// 判断一个节点是否有问题至少需要通过以下三个参数:
    ///     1.指定主机的Quic连接状态: is_connected
    ///     2.指定主机的Quic连接的通讯延迟: get_connection_delay
    ///     3.主机的故障估值: failure_valuation
    pub fn get_connection_delay(&self, host_id: &GossipNodeID) -> Option<Duration> {
        if let Some(connection) = self.get_connection(host_id) {
            Some(connection.get_latency())
        } else {
            None
        }
    }

    /// 注册指定主机的连接
    pub fn register_connection(&self,
                               host_id: GossipNodeID,
                               connection: Connection) {
        if host_id.get_time() == &0 {
            //是未握手的种子主机
            let hostname = host_id.get_name(); //种子主机只需要主机名
            self
                .0
                .seed_connections
                .insert(hostname, connection);
        } else {
            self
                .0
                .connections
                .insert(host_id, connection);
        }
    }

    /// 将已握手的种子主机连接从临时连接表中复制到主机连接表中
    pub fn from_seed_connection_copy_to_connection(&self, host_id: &GossipNodeID) -> bool {
        if host_id.get_time() == &0 {
            //不是有效的当前种子主机唯一id，则忽略
            return false;
        }

        let hostname = host_id.get_name();
        if let Some(item) = self
            .0
            .seed_connections
            .get(hostname.as_str()) {
            //指定种子主机名的种子主机连接存在
            let connection = item.value();
            self
                .0
                .connections
                .insert(host_id.clone(),
                        connection.clone());
            true
        } else {
            //指定种子主机名的种子主机连接不存在，则忽略
            false
        }
    }

    /// 注销指定主机的连接
    pub fn unregister_connection(&self,
                                 host_id: &GossipNodeID) -> Option<Connection> {
        //移除可能的种子主机连接
        let hostname = host_id.get_name();
        self
            .0
            .seed_connections
            .remove(hostname.as_str());

        //移除指定主机的连接
        if let Some((_key, connection)) = self.0.connections.remove(host_id) {
            Some(connection)
        } else {
            None
        }
    }

    /// 判断是否已握手
    pub fn is_handshaked(&self, connection_id: &usize) -> bool {
        self
            .0
            .peer_binds
            .contains_key(connection_id)
    }

    /// 获取指定的对端连接绑定的主机唯一id
    pub fn get_peer_host_id(&self, connection_id: &usize) -> Option<GossipNodeID> {
        if let Some(item) = self.0.peer_binds.get(connection_id) {
            let host_id = item.value();
            Some(host_id.clone())
        } else {
            None
        }
    }

    /// 绑定指定对端的主机唯一id
    pub fn bind_peer_id(&self, connection_id: usize, host_id: GossipNodeID) {
        self
            .0
            .peer_binds
            .insert(connection_id, host_id.clone());
    }

    /// 解除已绑定的指定对端的主机唯一id
    pub fn unbind_peer_id(&self, connection_id: &usize) -> Option<GossipNodeID> {
        if let Some((_connection_id, host_id)) = self.0.peer_binds.remove(connection_id) {
            Some(host_id)
        } else {
            None
        }
    }

    /// 获取指定连接的扩展通道数量
    pub fn channels_len(&self, connection_id: &usize) -> usize {
        if let Some(item) = self
            .0
            .channels_len_map
            .get(connection_id) {
            *item.value()
        } else {
            0
        }
    }

    /// 判断指定连接的指定扩展通道是否存在
    pub fn contains_channel(&self,
                            connection_id: usize,
                            channel_id: ChannelId) -> bool {
        self
            .0
            .channels
            .contains_key(&(connection_id, channel_id))
    }

    /// 加入指定连接的指定扩展通道
    pub fn insert_channel(&self,
                          connection_id: usize,
                          channel_id: ChannelId) {
        self
            .0
            .channels
            .insert((connection_id, channel_id),
                    Channel::new());

        if self.0.channels_len_map.contains_key(&connection_id) {
            if let Some(mut item) = self.0.channels_len_map.get_mut(&connection_id) {
                //指定连接的通道数存在，则增加数量
                let value = item.value_mut();
                *value += 1;
            }
        } else {
            self.0.channels_len_map.insert(connection_id, 1);
        }
    }

    /// 移除指定连接的指定扩展通道
    pub fn remove_channel(&self,
                          connection_id: usize,
                          channel_id: ChannelId) {
        let mut is_full_remove = false;
        if let Some(_) = self.0.channels.remove(&(connection_id, channel_id)) {
            if let Some(mut item) = self.0.channels_len_map.get_mut(&connection_id) {
                let value = item.value_mut();
                *value -= 1;
                if value == &0 {
                    is_full_remove = true;
                }
            }
        }

        if is_full_remove {
            //指定连接的所有通道已移除，则从连接通道表中移除指定连接
            let _ = self.0.channels_len_map.remove(&connection_id);
        }
    }

    /// 获取P2P终端的行为
    pub fn get_behavior(&self) -> &Scuttlebutt<P2PTransporter> {
        &self.0.behavior
    }

    /// 获取P2P终端的传输器
    pub(crate) fn get_transporter(&self) -> &P2PTransporter {
        &self.0.transporter
    }

    /// 获取本地主机的心跳
    pub fn get_local_host_heartbeat(&self) -> u64 {
        self
            .get_behavior()
            .get_table()
            .local_heartbeat()
    }

    /// 获取本地主机属性的所有关键字
    pub fn get_local_host_attr_keys(&self) -> Vec<Key> {
        self
            .get_behavior()
            .get_table()
            .keys()
    }

    /// 获取指定关键字的本地主机属性
    pub fn get_local_host_attr(&self, key: &Key) -> Option<(Value, u64)> {
        self
            .get_behavior()
            .get_table()
            .get(key)
    }

    /// 设置本地主机属性，返回设置后属性数量
    pub fn set_local_host_attr(&self, key: Key, value: Value) -> usize {
        let table = self.get_behavior().get_table();
        let _ = table.insert(key, value);

        table.len()
    }

    /// 获取指定主机的心跳
    pub fn get_host_heartbeat(&self, host_id: &GossipNodeID) -> u64 {
        self
            .get_behavior()
            .get_table()
            .get_heartbeat(&Atom::from(host_id.to_string()))
    }

    /// 获取指定主机属性的所有关键字
    pub fn get_host_attr_keys(&self, host_id: &GossipNodeID) -> Option<Vec<Key>> {
        self
            .get_behavior()
            .get_table()
            .get_private_keys(&Atom::from(host_id.to_string()))
    }

    /// 获取指定主机属性的所有关键字
    pub fn get_host_attr(&self,
                         host_id: &GossipNodeID,
                         key: &Key) -> Option<(Value, u64)> {
        self
            .get_behavior()
            .get_table()
            .get_private_value(&Atom::from(host_id.to_string()), key)
    }

    /// 尝试弹出等待处理的P2P消息帧
    pub(crate) fn try_pop_wait_frame(&self) -> Option<P2PFrame> {
        match self.0.frames_recv.try_recv() {
            Err(_) => None,
            Ok(frame) => Some(frame),
        }
    }

    /// 尝试弹出等待处理的所有P2P消息帧
    pub(crate) fn try_pop_all_wait_frame(&self) -> Vec<P2PFrame> {
        self
            .0
            .frames_recv
            .try_iter()
            .collect()
    }

    /// 加入等待处理的P2P消息帧
    pub(crate) fn push_wait_frame(&self, frame: P2PFrame) -> Result<()> {
        if let Err(e) = self.0.frames_sent.send(frame) {
            Err(Error::new(ErrorKind::Other,
                           format!("Push wait frame failed, reason: {:?}", e)))
        } else {
            Ok(())
        }
    }

    /// 判断是否阻塞向指定的对端主机发送心跳
    pub(crate) fn contains_block_heartbeat(&self,
                                           host_id: &GossipNodeID<32>) -> bool {
        self
            .0
            .heartbeat_blocking
            .contains_key(host_id)
    }

    /// 在指定时间内阻塞向指定对端主机发送心跳
    /// 允许对相同对端主机反复调用此方法，只记录最近一次调用后的阻塞
    pub(crate) fn block_heartbeat(&self, host_id: GossipNodeID<32>) {
        let timeout = self.0.blocking_timeout;

        //记录阻塞向指定对端主机发送心跳
        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .add(self.0.blocking_timeout);
        self
            .0
            .heartbeat_blocking.insert(host_id.clone(), time);

        //设置异步定时器，在指定时间后移除阻塞
        let terminal = self.clone();
        let _ = self.0.rt.spawn(self.0.rt.alloc(), async move {
            //等待指定的时间
            terminal
                .0
                .rt
                .timeout(timeout.as_millis() as usize)
                .await;

            //移除指定对端主机发送心跳的记录
            let mut is_remove = false;
            if let Some(item) = terminal
                .0
                .heartbeat_blocking
                .get(&host_id) {
                let value = item.value();

                if &time == value {
                    //如果是对应的记录，则移除
                    is_remove = true;
                }
            }

            if is_remove {
                if let Some((peer, time)) = terminal
                    .0
                    .heartbeat_blocking
                    .remove(&host_id) {
                    debug!("Remove heartbeat blocking successed, peer: {:?}, time: {:?}",
                        peer,
                        time);
                }
            }
        });
    }

    /// 获取当前心跳间隔时长
    pub fn get_heartbeat_interval(&self) -> Duration {
        Duration::from_millis(self
            .0
            .heartbeat_interval
            .load(Ordering::Acquire) as u64)
    }

    /// 设置当前心跳间隔时长
    pub fn set_heartbeat_interval(&self, interval: Duration) {
        let interval = if interval.as_millis() == 0 {
            DEFAULT_HEARTBEAT_INTERVAL
        } else {
            interval
        };

        self
            .0
            .heartbeat_interval
            .store(interval.as_millis() as usize, Ordering::Release);
    }

    /// 获取当前整理间隔时长
    pub fn get_collect_interval(&self) -> Duration {
        Duration::from_millis(self
            .0
            .collect_interval
            .load(Ordering::Acquire) as u64)
    }

    /// 设置当前整理间隔时长
    pub fn set_collect_interval(&self, interval: Duration) {
        let interval = if interval.as_millis() == 0 {
            DEFAULT_COLLECT_INTERVAL
        } else {
            interval
        };

        self
            .0
            .collect_interval
            .store(interval.as_millis() as usize, Ordering::Release);
    }

    /// 当前是否发送心跳
    pub fn is_heartbeating(&self) -> bool {
        !self
            .0
            .collecting_pause
            .load(Ordering::Relaxed)
    }

    /// 恢复当前发送心跳
    pub fn recover_heartbeating(&self) {
        self
            .0
            .heartbeating_pause
            .store(false, Ordering::Relaxed);
    }

    /// 暂停当前发送心跳
    pub fn pause_heartbeating(&self) {
        self
            .0
            .heartbeating_pause
            .store(true, Ordering::Relaxed);
    }

    /// 当前是否整理
    pub fn is_collecting(&self) -> bool {
        !self
            .0
            .collecting_pause
            .load(Ordering::Relaxed)
    }

    /// 恢复当前整理
    pub fn recover_collecting(&self) {
        self
            .0
            .collecting_pause
            .store(false, Ordering::Relaxed);
    }

    /// 暂停当前整理
    pub fn pause_collecting(&self) {
        self
            .0
            .collecting_pause
            .store(true, Ordering::Relaxed);
    }

    /// 关闭指定主机连接的通道
    pub fn close_channel_to(&self,
                            host_id: &GossipNodeID<32>,
                            channel: ChannelId) -> Result<()> {
        if let Some(connection) = self.get_connection(host_id) {
            //指定主机连接的通道存在，则立即关闭
            connection
                .close_channel(channel)
        } else {
            Ok(())
        }
    }

    /// 关闭指定主机的连接
    pub fn close_to(&self,
                    host_id: &GossipNodeID<32>,
                    code: u32,
                    reason: Result<()>) -> Result<()> {
        if let Some(connection) = self.get_connection(host_id) {
            //指定的主机的连接存在，则立即关闭
            connection.close(code, reason)
        } else {
            Ok(())
        }
    }

    /// 派发一个异步任务到当前P2P终端
    pub fn spawn_to(&self,
                    task: impl Future<Output = ()> + Send + 'static) {
        let _ = self
            .0
            .rt
            .spawn(self.0.rt.alloc(),
                   task);
    }

    /// 派发一个在指定时间后执行的异步任务到当前P2P终端
    pub fn spawn_timeout(&self,
                         task: impl Future<Output = ()> + Send + 'static,
                         timeout: usize) {
        let rt = self.0.rt.clone();
        let _ = self
            .0
            .rt
            .spawn(self.0.rt.alloc(),
                   async move {
                       rt.timeout(timeout).await;
                       task.await;
                   });
    }
}

/*
* P2P终端异步方法
*/
impl Terminal {
    /// 启动P2P终端
    /// 在指定异步运行时上启动整理和心跳
    /// 每次整理时与指定数量的随机种子主机和指定数量的随机非种子主机进行同步
    pub async fn startup<R>(&self,
                            rt: R,
                            seed_amount: usize,
                            not_seed_amount: usize,
                            connect_timeout: usize,
                            sync_timeout: usize,
                            heartbeat_interval: Option<Duration>,
                            collect_interval: Duration)
        where R: AsyncRuntime
    {
        let terminal = self.clone();

        //初始化P2P对端故障侦听器
        {
            let hosts = terminal.all_hosts();
            let failure_detector = &mut *terminal
                .0
                .failure_detector
                .lock()
                .await;
            for host in &hosts {
                failure_detector.report_heartbeat(host);
            }
        }

        if let Some(heartbeat_interval) = heartbeat_interval {
            //需要启动独立的心跳通知
            terminal.set_heartbeat_interval(heartbeat_interval);

            let _ = rt.spawn(rt.alloc(),
                             heartbeat(rt.clone(),
                                       terminal.clone(),
                                       connect_timeout));
        }

        terminal.set_collect_interval(collect_interval);
        let _ = rt.spawn(rt.alloc(),
                         collect(
                             rt.clone(),
                             terminal,
                             seed_amount,
                             not_seed_amount,
                             Duration::from_millis(sync_timeout as u64)
                         ));
    }

    /// 更新指定的对端主机心跳，以更新P2P对端故障侦听器
    pub(crate) async fn update_host_heartbeat(&self, peer: &GossipNodeID<32>) {
        let mut locked = self
            .0
            .failure_detector
            .lock()
            .await;
        locked.report_heartbeat(peer);
        locked.update_node_liveliness(peer);
    }

    /// 获取指定主机的故障估值，根据实际初始化配置和运行情况会产生不同的误报
    /// 判断一个节点是否有问题至少需要通过以下三个参数:
    ///     1.指定主机的Quic连接状态: is_connected
    ///     2.指定主机的Quic连接的通讯延迟: get_connection_delay
    ///     3.主机的故障估值: failure_valuation
    pub async fn failure_valuation(&self,
                                   host_id: &GossipNodeID<32>) -> Option<f64> {
        self
            .0
            .failure_detector
            .lock()
            .await
            .phi(host_id)
    }

    /// 获取所有故障估值低的活动主机
    pub async fn active_hosts(&self) -> Vec<GossipNodeID<32>> {
        self
            .0
            .failure_detector
            .lock()
            .await
            .live_nodes()
            .map(|host_id| host_id.clone())
            .collect()
    }

    /// 获取所有故障估值高的非活动主机
    pub async fn inactive_hosts(&self) -> Vec<GossipNodeID<32>> {
        self
            .0
            .failure_detector
            .lock()
            .await
            .unlive_nodes()
            .map(|host_id| host_id.clone())
            .collect()
    }

    /// 获取所有因为故障估值长时间持续偏高，所以导致被回收的主机
    pub async fn recycled_hosts(&self) -> Vec<GossipNodeID<32>> {
        self
            .0
            .failure_detector
            .lock()
            .await
            .garbage_collect()
    }

    /// 连接指定的P2P主机
    pub async fn connect_to(&self,
                            host_id: &GossipNodeID<32>,
                            timeout: Option<usize>) -> Result<Connection> {
        let table = self
            .get_behavior()
            .get_table();

        let mut address = None;
        let mut is_seed = false;
        let node = Atom::from(host_id.to_string().as_str());
        if let Some((value, _version)) = table
            .get_private_value(&node, &Key::from(DEFAULT_HOST_IP_KEY_STR)) {
            if let Some(str) = value.as_text() {
                let ip = IpAddr::from_str(str).unwrap();

                if let Some((val, _version)) = table
                    .get_private_value(&node, &Key::from(DEFAULT_HOST_PORT_KEY_STR)) {
                    if let Some(integer) = val.as_integer() {
                        let port = i128::from(integer) as u16;
                        address = Some(SocketAddr::new(ip, port));
                    } else {
                        return Err(Error::new(ErrorKind::Other,
                                              format!("Connect to peer failed for p2p, peer: {:?}, port: {:?}. reason: invalid port string",
                                                      host_id,
                                                      val)));
                    }
                }
            } else {
                return Err(Error::new(ErrorKind::Other,
                                      format!("Connect to peer failed for p2p, peer: {:?}, ip: {:?}. reason: invalid ip string",
                                              host_id,
                                              value)));
            }
        }

        if address.is_none() {
            //指定的非种子主机不存在，则继续查询种子主机
            let hostname = host_id.get_name(); //种子主机只需要主机名
            address = if let Some(addr) = self.get_seed_host_address(hostname.as_str()) {
                is_seed = true;
                Some(addr)
            } else {
                //指定的主机不存在，则立即返回错误原因
                return Err(Error::new(ErrorKind::Other,
                                      format!("Connect to peer failed for p2p, address: {:?}, peer: {:?}, reason: host not exist",
                                              address,
                                              host_id)));
            };
        }

        //获取对端主机的dns名称
        let hostname_dns_str = match host_id_to_host_dns(host_id, is_seed) {
            Err(e) => {
                //获取对端主机的dns名失败，则立即返回错误原因
                return Err(Error::new(ErrorKind::Other,
                                      format!("Connect to peer failed for p2p, peer: {:?}, reason: {:?}",
                                              host_id,
                                              e)));
            },
            Ok(str) => {
                str
            },
        };

        //连接指定的主机
        let peer_address = address.unwrap();
        let connection = self
            .0
            .client
            .connect(peer_address,
                     hostname_dns_str.as_str(),
                     None,
                     timeout).await?;

        //注册已连接的主机
        let connection
            = Connection::new(PeerSocketHandle::ClientConnection(connection),
                              self.clone());
        self.register_connection(host_id.clone(), connection.clone());

        Ok(connection)
    }

    /// 与指定的对端节点进行同步
    pub async fn sync_with(&self,
                           peer: &GossipNodeID<32>,
                           timeout: Duration) -> Result<SyncStatistics> {
        let timeout = if timeout.as_millis() == 0 {
            DEFAULT_SYNC_TIMEOUT
        } else {
            timeout.as_millis() as usize
        };

        //开始与指定的对端主机同步
        let context = ServiceContext::default();
        let rt_copy = self.0.rt.clone();
        let context_copy = context.clone();
        let peer_copy = peer.clone();
        let local = self.get_self().clone();
        let _ = self.0.rt.spawn(self.0.rt.alloc(), async move {
            //设置本次同步任务的超时
            rt_copy.timeout(timeout).await;
            context_copy.end_sync(Err(Error::new(ErrorKind::TimedOut,
                                                 format!("Sync with peer failed, peer: {:?}, local: {:?}, timeout: {:?}, reason:  timeout",
                                                         peer_copy,
                                                         local,
                                                         timeout)))).await;
        });
        let statistics = self
            .0
            .behavior
            .sync_with(&context, peer).await?;

        //与指定的对端主机同步成功，则暂时阻塞向指定的对端发送心跳
        self.block_heartbeat(peer.clone());

        Ok(statistics)
    }

    /// 打开与指定对端主机连接的指定通道
    pub async fn open_channel(&self,
                              peer: &GossipNodeID<32>) -> Result<ChannelId> {
        if let Some(connection) = self.get_connection(peer) {
            //指定对端主机的连接存在
            connection
                .open_channel()
                .await
        } else {
            //指定对端主机的连接不存在，则立返回错误原因
            Err(Error::new(ErrorKind::NotConnected,
                           format!("Open channel failed, peer: {:?}, reason: require connection",
                                   peer)))
        }
    }
}

// 异步更新本地心跳
fn heartbeat<R>(rt: R,
                terminal: Terminal,
                connect_timeout: usize) -> BoxFuture<'static, ()>
    where R: AsyncRuntime
{
    async move {
        if terminal
            .0
            .heartbeating_pause
            .load(Ordering::Relaxed) {
            //暂停当前异步更新本地心跳
            rt
                .timeout(terminal
                    .0
                    .heartbeat_interval
                    .load(Ordering::Acquire))
                .await;
            let _ = rt.spawn(rt.alloc(),
                             heartbeat(rt.clone(),
                                       terminal,
                                       connect_timeout));
            return;
        }

        let now = Instant::now();
        let table = terminal
            .get_behavior()
            .get_table();

        //更新本地心跳
        table.upgrade_local_heartbeat();

        //构建本地当前心跳帧
        let heartbeat_info = P2PHeartBeatInfo::new(
            table.local_heartbeat()
        );
        let mut heartbeat_frame = P2PFrame::new();
        heartbeat_frame.set_tag(DEFAULT_HEARTBEAT_TAG);
        let payload = BytesMut::from(heartbeat_info);
        heartbeat_frame.set_len(payload.len());
        heartbeat_frame.append_payload(payload.freeze());
        let heartbeat_bytes = heartbeat_frame.into_vec();

        let timeout = if connect_timeout == 0 {
            //连接超时时长过小，则使用默认的连接超时时长
            Some(DEFAULT_QUIC_CONNECT_TIMEOUT)
        } else {
            Some(connect_timeout)
        };

        //向所有已连接的对端主机发送本地主机的心跳更新
        let hosts = terminal.all_connected();
        for host in &hosts {
            let peer = if let Some(connection) = terminal.get_connection(host) {
                //已连接对端主机
                connection
            } else {
                //未连接对端主机
                match terminal.connect_to(host, timeout).await {
                    Err(e) => {
                        //连接指定的对端主机失败，并继续通知下一个主机
                        error!("Upgrade heartbeat to peer failed, peer: {:?}, reason: {:?}",
                            host,
                            e);
                        continue;
                    },
                    Ok(connection) => connection,
                }
            };

            if terminal.contains_block_heartbeat(host) {
                //已阻塞向指定的对端主机发送心跳，则忽略当前的对端主机，并继续尝试向下一个对端主机发送心跳
                continue;
            }

            //向指定的主机发送本地主机当前心跳帧
            if let Err(e) = peer
                .send(peer.main_channel(),
                      heartbeat_bytes.clone()) {
                error!("Upgrade heartbeat to peer failed, peer: {:?}, reason: {:?}",
                        host,
                        e);
            }
        }

        info!("Upgrade heartbeat successed in {:?}, hosts: {:?}, time: {:?}",
            terminal.get_self(),
            hosts.len(),
            now.elapsed());

        //在指定间隔时间后继续派发异步更新本地心跳的任务
        rt
            .timeout(terminal
                .0
                .heartbeat_interval
                .load(Ordering::Acquire))
            .await;
        let _ = rt.spawn(rt.alloc(),
                         heartbeat(rt.clone(),
                                   terminal,
                                   connect_timeout));
    }.boxed()
}

// 异步定时整理本地P2P终端
fn collect<R>(rt: R,
              terminal: Terminal,
              seed_amount: usize,
              not_seed_amount: usize,
              sync_timeout: Duration) -> BoxFuture<'static, ()>
    where R: AsyncRuntime
{
    async move {
        if terminal
            .0
            .collecting_pause
            .load(Ordering::Relaxed) {
            //暂停当前异步定时整理本地P2P终端
            for host in terminal.all_hosts() {
                //更新已知对端主机的状态
                terminal
                    .0
                    .failure_detector
                    .lock()
                    .await
                    .update_node_liveliness(&host);
            }

            rt
                .timeout(terminal
                    .0
                    .collect_interval
                    .load(Ordering::Acquire))
                .await;
            let _ = rt.spawn(rt.alloc(),
                             collect(
                                 rt.clone(),
                                 terminal,
                                 seed_amount,
                                 not_seed_amount,
                                 sync_timeout));
            return;
        }

        let now = Instant::now();
        let table = terminal
            .get_behavior()
            .get_table();

        //更新本地心跳
        table.upgrade_local_heartbeat();

        //与种子主机进行同步
        let real_seed_amount = if seed_amount == 0 {
            //默认至少需要和一个种子主机进行同步
            1
        } else {
            seed_amount
        };
        let seeds = terminal.all_seeds();
        for _ in 0..real_seed_amount {
            let tmp_now = Instant::now();
            let rng = &mut *terminal
                .0
                .rng
                .lock()
                .await;

            if let Some(seed) = seeds.choose(rng) {
                //随机选择一个种子主机
                match terminal.sync_with(seed, sync_timeout).await {
                    Err(e) => {
                        //与指定种子主机同步失败
                        error!("Collect sync failed, peer: Seed({:?}), time: {:?}, reason: {:?}",
                            seed,
                            tmp_now.elapsed(),
                            e);
                    },
                    Ok(statistics) => {
                        //与指定种子主机同步成功
                        info!("Collect sync successed, peer: Seed({:?}), time: {:?}, statistics: {:?}",
                            seed,
                            tmp_now.elapsed(),
                            statistics);
                    },
                }
            }
        }

        //与非种子主机进行同步
        let real_not_seed_amount = if not_seed_amount == 0 {
            //默认至少需要和一个种子主机进行同步
            1
        } else {
            not_seed_amount
        };
        let not_seeds = terminal.all_hosts_filter(|host| {
            //忽略所有种子主机
            seeds.contains(host)
        });
        for _ in 0..real_not_seed_amount {
            let tmp_now = Instant::now();
            let rng = &mut *terminal
                .0
                .rng
                .lock()
                .await;

            if let Some(not_seed) = not_seeds.choose(rng) {
                //随机选择一个非种子主机
                match terminal.sync_with(not_seed, sync_timeout).await {
                    Err(e) => {
                        //与指定种子主机同步失败
                        error!("Collecting sync failed with peer, peer: NotSeed({:?}), time: {:?}, reason: {:?}",
                            not_seed,
                            tmp_now.elapsed(),
                            e);
                    },
                    Ok(statistics) => {
                        //与指定种子主机同步成功
                        info!("Collecting sync successed with peer, peer: NotSeed({:?}), time: {:?}, statistics: {:?}",
                            not_seed,
                            tmp_now.elapsed(),
                            statistics);
                    },
                }
            }
        }

        //更新已知对端主机的状态，并垃圾回收P2P终端故障侦听器
        for host in seeds {
            terminal
                .0
                .failure_detector
                .lock()
                .await
                .update_node_liveliness(&host);
        }
        for host in not_seeds {
            terminal
                .0
                .failure_detector
                .lock()
                .await
                .update_node_liveliness(&host);
        }
        let gc_len = terminal
            .0
            .failure_detector
            .lock()
            .await
            .garbage_collect()
            .len();

        info!("Collecting successed in {:?}, seeds: {:?}, not_seeds: {:?}, after_gc: {:?}, time: {:?}",
            terminal.get_self(),
            real_seed_amount,
            real_not_seed_amount,
            gc_len,
            now.elapsed());

        //在指定间隔时间后继续派发异步定时整理本地P2P终端的任务
        rt
            .timeout(terminal
                .0
                .collect_interval
                .load(Ordering::Acquire))
            .await;
        let _ = rt.spawn(rt.alloc(),
                         collect(
                             rt.clone(),
                             terminal,
                             seed_amount,
                             not_seed_amount,
                             sync_timeout));
    }.boxed()
}

// 内部P2P终端
pub(crate) struct InnerTerminal {
    rt:                 MultiTaskRuntime,                       //多线程运行时
    local:              GossipNodeID,                           //本地主机唯一id
    client:             QuicClient,                             //Quic客户端
    seed_connections:   DashMap<String, Connection>,            //种子主机临时连接表
    connections:        DashMap<GossipNodeID, Connection>,      //连接表，关键字为本地主机唯一id，值为本地主机的连接
    peer_binds:         DashMap<usize, GossipNodeID>,           //对端连接绑定表，关键字为对端连接的唯一id，值为对端主机的唯一id
    channels:           DashMap<(usize, ChannelId), Channel>,   //连接的通道表
    channels_len_map:   DashMap<usize, usize>,                  //连接通道数量表
    behavior:           Scuttlebutt<P2PTransporter>,            //P2P服务行为
    transporter:        P2PTransporter,                         //P2P传输器
    failure_detector:   Arc<Mutex<PhiFailureDetector>>,         //P2P对端故障侦听器
    uptime:             Duration,                               //P2P终端启动时间
    rng:                Mutex<SmallRng>,                        //P2P终端本地随机数生成器
    frames_recv:        Receiver<P2PFrame>,                     //等待处理的P2P消息帧接收器
    frames_sent:        Sender<P2PFrame>,                       //等待处理的P2P消息帧发送器
    seed_hosts:         RwLock<BTreeMap<Atom, SocketAddr>>,     //种子主机表
    heartbeat_blocking: DashMap<GossipNodeID, Duration>,        //发送心跳的阻塞表
    blocking_timeout:   Duration,                               //阻塞发送心中的时间
    heartbeat_interval: Arc<AtomicUsize>,                       //发送心跳间隔时长
    collect_interval:   Arc<AtomicUsize>,                       //整理间隔时长
    heartbeating_pause: Arc<AtomicBool>,                        //发送心跳暂停
    collecting_pause:   Arc<AtomicBool>,                        //整理暂停
}