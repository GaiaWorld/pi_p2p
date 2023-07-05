use std::sync::Arc;
use std::ops::Deref;
use std::str::FromStr;
use std::convert::TryFrom;
use std::net::{SocketAddr, IpAddr};
use std::result::Result as GenResult;
use std::io::{Cursor, Result, Error, ErrorKind};

use futures::future::{FutureExt, BoxFuture, LocalBoxFuture};
use quinn_proto::{Dir, StreamId};
use quic::{AsyncService, SocketEvent, SocketHandle,
           utils::QuicSocketReady};
use pi_gossip::{GossipNodeID, GossipContext,
                transport::{Transport, GossipTid, GossipSendBinary, HandlePullBinary, HandlePushPullBinary},
                service::{PullBehavior, PushBehavior, PushPullBehavior, ServiceContext},
                scuttlebutt::{table::Table,
                              core::{DEFAULT_PULL_SYNC_REQ_TAG, DEFAULT_PULL_SYNC_ACK_TAG, DEFAULT_PUSH_SYNC_TAG,
                                     Scuttlebutt}}};
use pi_async::rt::{AsyncRuntime,
                   multi_thread::MultiTaskRuntime};
use pi_gossip::scuttlebutt::table::{Key, NodeChangingEvent, TableChangeMonitor};
use pi_hash::XHashMap;
use pi_atom::Atom;
use parking_lot::RwLock;
use dashmap::DashMap;
use bytes::{Buf, Bytes, BytesMut};
use futures::StreamExt;
use log::{debug, warn, error};

use crate::{connection::{Connection, ChannelId, PeerSocketHandle},
            frame::{P2PFrame, ParseFrameResult, P2PHandShakeInfo, P2PHeartBeatInfo, P2PServiceReadInfo},
            terminal::{DEFAULT_SEED_HOST_KEY_STR, DEFAULT_HOST_IP_KEY_STR, DEFAULT_HOST_PORT_KEY_STR, Terminal}};
use crate::frame::P2PServiceWriteInfo;

// 默认的连接握手帧标记
pub(crate) const DEFAULT_CONNECT_HANDSHAKE_TAG: u8 = 0x0;

// 默认的P2P主机心跳帧标记
pub(crate) const DEFAULT_HEARTBEAT_TAG: u8 = 0x5;

// 默认的P2P主机服务帧标记
pub(crate) const DEFAULT_SERVICE_TAG: u8 = 0x6;

///
/// P2P服务
///
pub trait P2PService: Send + Sync + 'static {
    /// 初始化P2P服务
    fn init(&self,
            local: GossipNodeID,
            terminal: Terminal,
            listener: Arc<P2PServiceListener>);

    /// 获取当前P2P服务所属的P2P终端
    fn get_terminal(&self) -> Option<Terminal>;

    /// 获取当前P2P服务的监听器
    fn get_listener(&self) -> Option<Arc<P2PServiceListener>>;

    /// 监听指定主机的状态改变
    fn listen_hosts(&self, hosts: Option<Vec<GossipNodeID<32>>>) {
        if let Some(listener) = self.get_listener() {
            //当前P2P服务的监听器存在
            let mut locked = listener.host_listen_rule.write();

            if hosts.is_none() {
                //监听任意主机状态改变
                *locked = HostListenRule::Any(XHashMap::default());
                return;
            }

            //监听指定主机状态改变
            match &mut *locked {
                HostListenRule::Any(_map) => {
                    //从监听任意主机到监听指定主机
                    let mut map = XHashMap::default();
                    for host in hosts.unwrap() {
                        map.insert(host, ());
                    }

                    *locked = HostListenRule::Hosts(map);
                },
                HostListenRule::Hosts(map) => {
                    for host in hosts.unwrap() {
                        map.insert(host, ());
                    }
                },
            }
        }
    }

    /// 解除监听指定主机的状态改变
    fn unlisten_hosts(&self, hosts: Vec<GossipNodeID>) {
        if let Some(listener) = self.get_listener() {
            //当前P2P服务的监听器存在
            let mut locked = listener.host_listen_rule.write();

            match &mut *locked {
                HostListenRule::Any(map) => {
                    for host in hosts {
                        map.insert(host, ());
                    }
                },
                HostListenRule::Hosts(map) => {
                    for host in hosts {
                        map.remove(&host);
                    }
                },
            }
        }
    }

    /// 监听指定关键字的状态改变
    fn listen_keys(&self, keys: Option<Vec<Key>>) {
        if let Some(listener) = self.get_listener() {
            //当前P2P服务的监听器存在
            let mut locked = listener.key_listen_rule.write();

            if keys.is_none() {
                //监听任意主机状态改变
                *locked = KeyListenRule::Any(XHashMap::default());
                return;
            }

            //监听指定主机状态改变
            match &mut *locked {
                KeyListenRule::Any(_map) => {
                    //从监听任意主机到监听指定主机
                    let mut map = XHashMap::default();
                    for key in keys.unwrap() {
                        map.insert(key, ());
                    }

                    *locked = KeyListenRule::Keys(map);
                },
                KeyListenRule::Keys(map) => {
                    for key in keys.unwrap() {
                        map.insert(key, ());
                    }
                },
            }
        }
    }

    /// 解除监听指定关键字的状态改变
    fn unlisten_keys(&self, keys: Vec<Key>) {
        if let Some(listener) = self.get_listener() {
            //当前P2P服务的监听器存在
            let mut locked = listener.key_listen_rule.write();

            match &mut *locked {
                KeyListenRule::Any(map) => {
                    for host in keys {
                        map.insert(host, ());
                    }
                },
                KeyListenRule::Keys(map) => {
                    for key in keys {
                        map.remove(&key);
                    }
                },
            }
        }
    }

    /// 指定主机或关键字的状态已改变
    fn changed(&self, event: NodeChangingEvent<32>);

    /// 已握手对端主机
    fn handshaked(&self,
                  peer: GossipNodeID,
                  connection: Connection) -> LocalBoxFuture<'static, ()>;

    /// 接收到对端发送的P2P服务消息
    fn received(&self,
                peer: Option<GossipNodeID>,
                connection: Connection,
                channel_id: ChannelId,
                info: P2PServiceReadInfo) -> LocalBoxFuture<'static, ()>;

    /// 当前连接的指定通道已关闭
    fn closed_channel(&self,
                      connection: Connection,
                      channel_id: ChannelId,
                      code: u32,
                      result: Result<()>) -> LocalBoxFuture<'static, ()>;

    /// 当前连接已关闭
    fn closed(&self,
              connection: Connection,
              code: u32,
              result: Result<()>) -> LocalBoxFuture<'static, ()>;
}

/// 发送P2P服务消息到对端
pub fn send_to_service(connection: &Connection,
                       channel_id: &ChannelId,
                       info: P2PServiceWriteInfo) -> Result<()> {
    let connection = connection.clone();
    let channel_id = channel_id.clone();

    //构建P2P消息帧
    let mut frame = P2PFrame::new();
    frame.set_tag(DEFAULT_SERVICE_TAG);
    let payload: BytesMut = info.into();
    frame.set_len(payload.len());
    frame.append_payload(payload);

    //发送P2P消息帧
    connection.send(channel_id,
                    frame.into_vec())
}

///
/// 主机监听规则
///
#[derive(Debug, Clone)]
pub enum HostListenRule {
    Any(XHashMap<GossipNodeID<32>, ()>),    //监听任意主机，并排除监听指定主机
    Hosts(XHashMap<GossipNodeID<32>, ()>),  //监听指定主机列表
}

///
/// 关键字监听规则
///
#[derive(Debug, Clone)]
pub enum KeyListenRule {
    Any(XHashMap<Key, ()>),     //监听任意关键字，并排除监听指定关键字
    Keys(XHashMap<Key, ()>),    //监听指定关键字列表
}

// P2P服务监听器
pub struct P2PServiceListener {
    rt:                 MultiTaskRuntime,       //运行时
    adapter:            Arc<P2PServiceAdapter>, //P2P终端服务适配器
    host_listen_rule:   RwLock<HostListenRule>, //主机监听规则
    key_listen_rule:    RwLock<KeyListenRule>,  //关键字监听规则
}

impl TableChangeMonitor for P2PServiceListener {
    fn filter_changing(&self, node: &GossipNodeID<32>, key: &Key) -> bool {
        let mut result = true;

        //过滤监听的主机
        let locked = self.host_listen_rule.read();
        result = result && match &*locked {
            HostListenRule::Any(hosts) => !hosts.contains_key(node),
            HostListenRule::Hosts(hosts) => hosts.contains_key(node),
        };

        //过滤监听的关键字
        let locked = self.key_listen_rule.read();
        result = result && match &*locked {
            KeyListenRule::Any(keys) => !keys.contains_key(key),
            KeyListenRule::Keys(keys) => keys.contains_key(key),
        };

        result
    }

    fn spawn_changing(&self, event: NodeChangingEvent<32>) {
        let service = self
            .adapter
            .get_service()
            .clone();

        self.rt.spawn(self.rt.alloc(), async move {
            if let Some(terminal) = service.get_terminal() {
                if event.is_inserted() {
                    //新增主机事件
                    let mut is_seed = false;
                    let mut ip = None;
                    let mut port = None;
                    for (key, value) in event.changed_attrs() {
                        match key.to_string().as_str() {
                            DEFAULT_SEED_HOST_KEY_STR => {
                                if let Some(val) = value.inner() {
                                    if let Some(b) = val.as_bool() {
                                        is_seed = b;
                                    }
                                }
                            },
                            DEFAULT_HOST_IP_KEY_STR => {
                                if let Some(val) = value.inner() {
                                    if let Some(str) = val.as_text() {
                                        ip = Some(str.to_string());
                                    }
                                }
                            },
                            DEFAULT_HOST_PORT_KEY_STR => {
                                if let Some(val) = value.inner() {
                                    if let Some(num) = val.as_integer() {
                                        if let Ok(n) = u128::try_from(num) {
                                            port = Some(n as u16);
                                        }
                                    }
                                }
                            },
                            _ => (),
                        }
                    }

                    if is_seed {
                        //自动新增对应种子主机地址
                        if let Some(ip) = ip {
                            if let Ok(addr) = IpAddr::from_str(ip.as_str()) {
                                if let Some(port) = port {
                                    let _ = terminal
                                        .set_seed_host_address(event.node().get_name().as_str(),
                                                               addr,
                                                               port);
                                }
                            }
                        }
                    }
                } else if event.is_removed() {
                    //移除主机事件
                    for (key, value) in event.changed_attrs() {
                        match key.to_string().as_str() {
                            DEFAULT_SEED_HOST_KEY_STR => {
                                if let Some(val) = value.inner() {
                                    if let Some(b) = val.as_bool() {
                                        if b {
                                            //移除的是种子主机，则自动移除对应种子主机地址
                                            let _ = terminal
                                                .remove_seed_host_address(event.node().get_name().as_str());
                                        }
                                    }
                                }
                                break;
                            },
                            _ => (),
                        }
                    }
                }
            }

            //通知指定P2P服务，节点状态改变
            service.changed(event);
        });
    }
}

impl P2PServiceListener {
    /// 构建
    pub fn new(rt: MultiTaskRuntime,
               adapter: Arc<P2PServiceAdapter>) -> Self {
        P2PServiceListener {
            rt,
            adapter,
            host_listen_rule: RwLock::new(HostListenRule::Any(XHashMap::default())),
            key_listen_rule: RwLock::new(KeyListenRule::Any(XHashMap::default())),
        }
    }
}

///
/// P2P服务适配器
///
pub struct P2PServiceAdapter(Arc<InnerP2PServiceAdapter>);

impl Clone for P2PServiceAdapter {
    fn clone(&self) -> Self {
        P2PServiceAdapter(self.0.clone())
    }
}

impl AsyncService for P2PServiceAdapter {
    fn handle_connected(&self,
                        handle: SocketHandle,
                        result: Result<()>) -> LocalBoxFuture<'static, ()> {
        async move {
            if result.is_ok() {
                //开始首次读
                if let Some(main_stream_id) = handle.get_main_stream_id() {
                    handle
                        .set_ready(main_stream_id.clone(),
                                   QuicSocketReady::Readable);
                }
            }
        }.boxed_local()
    }

    fn handle_opened_expanding_stream(&self,
                                      handle: SocketHandle,
                                      stream_id: StreamId,
                                      _stream_type: Dir,
                                      result: Result<()>) -> LocalBoxFuture<'static, ()> {
        async move {
            if result.is_ok() {
                //开始首次读
                handle
                    .set_ready(stream_id,
                               QuicSocketReady::Readable);
            }
        }.boxed_local()
    }

    fn handle_readed(&self,
                     handle: SocketHandle,
                     stream_id: StreamId,
                     result: Result<usize>) -> LocalBoxFuture<'static, ()> {
        let adapter = self.clone();
        async move {
            if let Err(e) = result {
                //读quic数据失败，则立即返回
                error!("Handle readed failed for p2p, uid: {:?}, peer: {:?}, local: {:?}, stream_id: {:?}, reason: {:?}",
                    handle.get_uid(),
                    handle.get_remote(),
                    handle.get_local(),
                    stream_id,
                    e);
                return;
            }

            let mut frames = Vec::new();
            loop {
                let mut ready_len = 0;
                match handle.read_buffer_remaining(&stream_id) {
                    None => {
                        //获取指定连接的指定管道的读缓冲区失败，则立即返回
                        error!("Handle readed failed for p2p, uid: {:?}, peer: {:?}, local: {:?}, stream_id: {:?}, reason: invalid read buffer",
                        handle.get_uid(),
                        handle.get_remote(),
                        handle.get_local(),
                        stream_id);
                        return;
                    },
                    Some(0) => {
                        //当前读缓冲中没有数据，则异步准备继续读取数据
                        ready_len = match handle.read_ready(&stream_id, 0) {
                            Err(len) => len,
                            Ok(value) => {
                                value.await
                            },
                        };

                        if ready_len == 0 {
                            //当前连接已关闭，则立即返回
                            warn!("Handle readed failed for p2p, uid: {:?}, peer: {:?}, local: {:?}, stream_id: {:?}, reason: connection closed",
                            handle.get_uid(),
                            handle.get_remote(),
                            handle.get_local(),
                            stream_id);
                            return;
                        }
                    },
                    Some(remaining) => {
                        //当前读缓冲中有数据
                        if let Some(buf) = handle
                            .get_read_buffer(&stream_id)
                            .as_ref()
                            .unwrap()
                            .lock()
                            .as_mut() {
                            if parse_readed(&mut frames, buf) {
                                //分析出完整的P2P消息帧，则退出本次读取
                                break;
                            } else {
                                //未分析出至少一条完整的P2P消息帧，则等待读取新的quic数据后再继续分析
                                continue;
                            }
                        }
                    },
                }
            }

            if let Err(other_frames) = handle_gossip_frame(&handle, stream_id, &adapter, frames).await {
                //有非Gossip协议的P2P消息帧
                let connection = if handle.is_client() {
                    Connection::new(PeerSocketHandle::ClientSocket(handle),
                                    adapter.get_service().get_terminal().unwrap())
                } else {
                    Connection::new(PeerSocketHandle::ServerSocket(handle),
                                    adapter.get_service().get_terminal().unwrap())
                };

                let terminal = adapter
                    .get_service()
                    .get_terminal()
                    .unwrap();
                let peer = terminal.get_peer_host_id(&connection.get_uid());
                for frame in other_frames {
                    match frame.tag().unwrap() {
                        DEFAULT_SERVICE_TAG => {
                            //P2P服务消息帧，则通知P2P服务
                            adapter
                                .get_service()
                                .received(peer.clone(),
                                          connection.clone(),
                                          stream_id.0.into(),
                                          frame.payload().into()).await;
                        },
                        _ => {
                            //TODO 暂时未处理的P2P消息帧
                            ()
                        },
                    }
                }
            }
        }.boxed_local()
    }

    fn handle_writed(&self,
                     _handle: SocketHandle,
                     _stream_id: StreamId,
                     _result: Result<()>) -> LocalBoxFuture<'static, ()> {
        async move {

        }.boxed_local()
    }

    fn handle_closed(&self,
                     handle: SocketHandle,
                     stream_id: Option<StreamId>,
                     code: u32,
                     result: Result<()>) -> LocalBoxFuture<'static, ()> {
        let host = self.clone();
        let connection_id = handle.get_uid();

        async move {
            let terminal = host
                .get_service()
                .get_terminal()
                .unwrap();

            let host_id = if let Some(host_id) = terminal.unbind_peer_id(&connection_id) {
                //当前连接已解除绑定
                host_id
            } else {
                //当前连接未握手，则立即退出
                return;
            };

            let connection = if let Some(connection) = terminal.unregister_connection(&host_id) {
                //当前连接已注销
                connection
            } else {
                //当产胆连接未注销，则立即退出
                return;
            };

            if let Some(stream_id) = stream_id {
                host.get_service()
                    .closed_channel(connection,
                                    stream_id.0.into(),
                                    code,
                                    result).await;
            } else {
                host.get_service()
                    .closed(connection,
                            code,
                            result).await;
            }
        }.boxed_local()
    }

    fn handle_timeouted(&self,
                        _handle: SocketHandle,
                        _result: Result<SocketEvent>) -> LocalBoxFuture<'static, ()> {
        async move {

        }.boxed_local()
    }
}

// 分析已读的quic数据中的P2P消息帧
fn parse_readed(frames: &mut Vec<P2PFrame>,
                bytes: &mut BytesMut) -> bool {
    let mut b = if frames.is_empty() {
        //没有待填充的P2P消息帧
        false
    } else {
        //有待填充的P2P消息帧
        true
    };

    while bytes.remaining() > 0 {
        let mut frame = if b {
            //有待填充的P2P消息帧，则弹出
            b = false; //只弹出一次
            frames.pop().unwrap()
        } else {
            //没有待填充的P2P消息帧，则创建一个新的P2P消息帧
            P2PFrame::new()
        };

        loop {
            match frame.parse(bytes) {
                ParseFrameResult::Partial => {
                    //当前P2P消息帧的数据不完整，则等待后续分析
                    frames.push(frame);
                    return false;
                },
                ParseFrameResult::NextFrame => {
                    //当前P2P消息帧的数据完整，但还有下一帧的数据
                    frames.push(frame);
                    frame = P2PFrame::new(); //构建下一个P2P帧
                    continue;
                },
                ParseFrameResult::Complated => {
                    //当前P2P消息帧的数据完整，但还有未分析完的数据
                    frames.push(frame);
                    break;
                },
            }
        }
    }

    true
}

impl P2PServiceAdapter {
    /// 使用P2P服务构建一个P2P服务适配器，连接超时时长单位为ms
    pub fn with_service(service: Arc<dyn P2PService>,
                        connect_timeout: Option<usize>) -> Self {
        let contexts = DashMap::new();
        let inner = InnerP2PServiceAdapter {
            contexts,
            connect_timeout,
            service,
        };

        P2PServiceAdapter(Arc::new(inner))
    }

    /// 获取P2P服务适配器中的P2P服务
    pub fn get_service(&self) -> &Arc<dyn P2PService> {
        &self.0.service
    }
}

// 内部P2P服务适配器
struct InnerP2PServiceAdapter {
    contexts:           DashMap<u64, ServiceContext>,   //P2P服务上下文表
    connect_timeout:    Option<usize>,                  //P2P连接超时时长
    service:            Arc<dyn P2PService>,            //P2P服务
}

///
/// P2P传输器
///
pub struct P2PTransporter(Arc<P2PServiceAdapter>);

impl Clone for P2PTransporter {
    fn clone(&self) -> Self {
        P2PTransporter(self.0.clone())
    }
}

impl Deref for P2PTransporter {
    type Target = Arc<P2PServiceAdapter>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Transport<32> for P2PTransporter {
    fn nodes(&self) -> Vec<GossipNodeID<32>> {
        self
            .0
            .get_service()
            .get_terminal()
            .unwrap()
            .all_hosts()
    }

    fn actives(&self) -> BoxFuture<'static, Vec<GossipNodeID<32>>> {
        let transport = self.clone();
        async move {
            if let Some(terminal) = transport.0.get_service().get_terminal() {
                terminal.active_hosts().await
            } else {
                vec![]
            }
        }.boxed()
    }

    fn inactives(&self) -> BoxFuture<'static, Vec<GossipNodeID<32>>> {
        let transport = self.clone();
        async move {
            if let Some(terminal) = transport.0.get_service().get_terminal() {
                terminal.inactive_hosts().await
            } else {
                vec![]
            }
        }.boxed()
    }

    fn transmit<C, T, O>(&self,
                         context: C,
                         tag: u8,
                         tid: T,
                         to: Option<GossipNodeID<32>>,
                         msg: O) -> BoxFuture<'static, Result<()>>
        where C: GossipContext + Send + Sync + 'static,
              T: GossipTid + Send + Sync + 'static,
              O: GossipSendBinary + Send + Sync + 'static
    {
        let transporter = self.clone();
        async move {
            if let Some(to) = to.clone() {
                //尝试连接对端主机
                if tag == DEFAULT_PULL_SYNC_REQ_TAG {
                    //如果是同步拉取请求发起方，则尝试建立连接
                    //连接已建立且已握手，则忽略连接，否则建立连接并握手
                    transporter
                        .connect_to(&to,
                                    (transporter.0).0.connect_timeout)
                        .await?;
                }

                match tag {
                    DEFAULT_PULL_SYNC_REQ_TAG | DEFAULT_PULL_SYNC_ACK_TAG | DEFAULT_PUSH_SYNC_TAG => {
                        //传输Gossip协议的同步拉取请求，注册本次同步的本地上下文
                        (transporter.0)
                            .0
                            .contexts
                            .insert(tid.get(),
                                    context.get::<ServiceContext>().clone());

                        let mut frame = P2PFrame::new();
                        frame.set_tag(tag);
                        let payload = msg.into_vec();
                        frame.set_len(payload.len());
                        frame.append_payload(Cursor::new(payload));
                        transporter.send_gossip_to(&to, frame.into_vec())
                    },
                    any => {
                        //无效的Gossip协议传输标记，则立即退出
                        Err(Error::new(ErrorKind::Other,
                                              format!("Transmit gossip message failed for p2p, peer: {:?}, tag: {:?}, tid: {:?}, reason: invalid tag",
                                                      to,
                                                      any,
                                                      tid.get())))
                    },
                }
            } else {
                //暂时不支持随机发送
                unimplemented!()
            }
        }.boxed()
    }

    fn handle_push_events<C, P>(&self,
                                _context: C,
                                _from: GossipNodeID<32>,
                                _behavior: P,
                                _bin: P::HandlePushData)
        -> BoxFuture<'static, Result<()>>
        where C: GossipContext + Send + Sync + 'static,
              P: PushBehavior<32>
    {
        unimplemented!()
    }

    fn handle_pull_events<C, P>(&self,
                                _context: C,
                                _from: GossipNodeID<32>,
                                _behavior: P,
                                _bin: HandlePullBinary<P, 32>)
        -> BoxFuture<'static, Result<()>>
        where C: GossipContext + Send + Sync + 'static,
              P: PullBehavior<32>
    {
        unimplemented!()
    }

    fn handle_push_pull_events<P>(&self,
                                  from: GossipNodeID<32>,
                                  behavior: P,
                                  tag: u8,
                                  bin: HandlePushPullBinary<P, 32>)
                                  -> BoxFuture<'static, Result<()>>
        where P: PushPullBehavior<32>
    {
        match tag {
            DEFAULT_PULL_SYNC_REQ_TAG => {
                let msg = bin.into_handle_pull_data().unwrap();
                let mut tmp = msg.chunk();
                let pull_id = tmp.get_u64_le();
                let context = if let Some(item) = (self.0).0.contexts.get(&pull_id) {
                    item.value().clone()
                } else {
                    //注册本次同步的对端的上下文
                    let context = ServiceContext::default();
                    (self.0)
                        .0
                        .contexts
                        .insert(pull_id.get(),
                                context.clone());
                    context
                };

                async move {
                    if let Err(e) = behavior
                        .handle_begin_pull(&context,
                                           from,
                                           msg).await {
                        context
                            .get::<ServiceContext>()
                            .end_sync(Err(Error::new(ErrorKind::Other,
                                                     format!("Handle begin pull failed, reason: {:?}",
                                                             e))))
                            .await;
                        Err(e)
                    } else {
                        Ok(())
                    }
                }.boxed()
            },
            DEFAULT_PULL_SYNC_ACK_TAG => {
                let msg = bin.into_handle_pull_ack_data().unwrap();
                let mut tmp = msg.chunk();
                let pull_id = tmp.get_u64_le();
                let context = if let Some(item) = (self.0).0.contexts.get(&pull_id) {
                    item.value().clone()
                } else {
                    return async move {
                        Err(Error::new(ErrorKind::Other,
                                       format!("Handle then pull ack failed, pull_id: {:?}, reason: invalid pull id",
                                               pull_id)))
                    }.boxed();
                };

                async move {
                    if let Err(e) = behavior
                        .handle_then_pull_ack(&context,
                                              from,
                                              msg).await {
                        context
                            .get::<ServiceContext>()
                            .end_sync(Err(Error::new(ErrorKind::Other,
                                                     format!("Handle then pull ack failed, reason: {:?}",
                                                             e))))
                            .await;
                        Err(e)
                    } else {
                        context
                            .get::<ServiceContext>()
                            .end_sync(Ok(()))
                            .await;
                        Ok(())
                    }
                }.boxed()
            },
            DEFAULT_PUSH_SYNC_TAG => {
                let msg = bin.into_handle_push_data().unwrap();
                let mut tmp = msg.chunk();
                let pull_id = tmp.get_u64_le();
                let context = if let Some(item) = (self.0).0.contexts.get(&pull_id) {
                    item.value().clone()
                } else {
                    return async move {
                        Err(Error::new(ErrorKind::Other,
                                       format!("Handle end push faile, pull_id: {:?}, reason: invalid pull id",
                                               pull_id)))
                    }.boxed();
                };

                async move {
                    if let Err(e) = behavior
                        .handle_end_push(from,
                                         msg).await {
                        context
                            .get::<ServiceContext>()
                            .end_sync(Err(Error::new(ErrorKind::Other,
                                                     format!("Handle end push failed, reason: {:?}",
                                                             e))))
                            .await;
                        Err(e)
                    } else {
                        context
                            .get::<ServiceContext>()
                            .end_sync(Ok(()))
                            .await;
                        Ok(())
                    }
                }.boxed()
            },
            _ => {
                async move {
                    Ok(())
                }.boxed()
            }
        }
    }
}

// 处理基于Gossip协议的P2P消息帧
async fn handle_gossip_frame(handle: &SocketHandle,
                             stream_id: StreamId,
                             adapter: &P2PServiceAdapter,
                             mut frames: Vec<P2PFrame>) -> GenResult<(), Vec<P2PFrame>> {
    let connection_id = handle.get_uid();
    let terminal = adapter
        .get_service()
        .get_terminal()
        .unwrap();

    //加入等待处理的P2P消息帧
    let mut wait_frames = terminal.try_pop_all_wait_frame();
    frames.append(&mut wait_frames);

    debug!("Handle p2p frame\n\tlocal: {:?}\n\tframes: {:#?}",
        terminal.get_self(),
        frames);
    let mut other_frames = Vec::new();
    for frame in frames {
        match frame.tag().unwrap() {
            DEFAULT_CONNECT_HANDSHAKE_TAG => {
                //处理连接握手
                let info = P2PHandShakeInfo::from(frame.payload());

                if info.is_req() {
                    //P2P握手请求消息
                    if terminal.is_handshaked(&connection_id) {
                        //已绑定对端连接，则忽略握手请求，并继续处理其它P2P消息帧
                        warn!("Handle gossip frame failed, uid: {:?}, peer: {:?}, local: {:?}, stream_id: {:?}, peer_host: {:?}, reason: already handshaked",
                            handle.get_uid(),
                            handle.get_remote(),
                            handle.get_local(),
                            stream_id,
                            info.host_id());
                        continue;
                    }

                    //更新本地主机的P2P对端故障侦听器
                    let peer_host = info.host_id();
                    terminal
                        .update_host_heartbeat(peer_host)
                        .await;

                    //绑定对端连接和主机唯一id
                    terminal.bind_peer_id(connection_id,
                                          peer_host.clone());

                    //注册对端主机唯一id和对端连接
                    let peer_connection
                        = Connection::new(PeerSocketHandle::ServerSocket(handle.clone()),
                                          terminal.clone());
                    terminal.register_connection(peer_host.clone(),
                                                 peer_connection.clone());

                    //通知本地P2P服务与对端主机已握手
                    adapter
                        .get_service()
                        .handshaked(peer_host.clone(),
                                    peer_connection)
                        .await;

                    //向对端异步发送握手回应消息
                    let ack_info = P2PHandShakeInfo::new_ack(terminal.get_self().clone());
                    let mut ack_frame = P2PFrame::new();
                    ack_frame.set_tag(DEFAULT_CONNECT_HANDSHAKE_TAG);
                    let payload = BytesMut::from(ack_info);
                    ack_frame.set_len(payload.len());
                    ack_frame.append_payload(payload.freeze());
                    handle.write_ready(stream_id, ack_frame.into_bytes());
                } else if info.is_ack() {
                    //P2P握手回应消息
                    if terminal.is_handshaked(&connection_id) {
                        //已绑定对端连接，则忽略握手回应，并继续处理其它P2P消息帧
                        warn!("Handle gossip frame failed, uid: {:?}, peer: {:?}, local: {:?}, stream_id: {:?}, peer_host: {:?}, reason: already handshaked",
                            handle.get_uid(),
                            handle.get_remote(),
                            handle.get_local(),
                            stream_id,
                            info.host_id());
                        continue;
                    }

                    //更新本地主机的P2P对端故障侦听器
                    let peer_host = info.host_id();
                    terminal
                        .update_host_heartbeat(peer_host)
                        .await;

                    //绑定对端连接和主机唯一id
                    terminal.bind_peer_id(connection_id,
                                          peer_host.clone());

                    //注册对端种子主机唯一id和对端连接
                    terminal.from_seed_connection_copy_to_connection(peer_host);

                    if let Some(peer_connection) = terminal.get_connection(peer_host) {
                        //指定对端主机的连接存在，则通知本地P2P服务与对端主机已握手
                        adapter
                            .get_service()
                            .handshaked(peer_host.clone(),
                                        peer_connection)
                            .await;
                    }
                }
            },
            tag@DEFAULT_PULL_SYNC_REQ_TAG | tag@DEFAULT_PULL_SYNC_ACK_TAG | tag@DEFAULT_PUSH_SYNC_TAG => {
                //处理Gossip的同步拉取请求
                let from = if let Some(host_id) = terminal.get_peer_host_id(&connection_id) {
                    //获取发送同步拉取请求的主机唯一id
                    host_id
                } else {
                    //与对端连接还未完成握手，则加入P2P消息帧等待队列，等待后续处理
                    if let Err(e) = terminal.push_wait_frame(frame) {
                        warn!("Handle gossip failed, uid: {:?}, peer: {:?}, local: {:?}, stream_id: {:?}, tag: {:?}, reason: {:?}",
                            handle.get_uid(),
                            handle.get_remote(),
                            handle.get_local(),
                            stream_id,
                            tag,
                            e);
                    }

                    break;
                };

                //更新本地主机的P2P对端故障侦听器
                if tag == DEFAULT_PULL_SYNC_REQ_TAG || tag == DEFAULT_PULL_SYNC_ACK_TAG {
                    terminal
                        .update_host_heartbeat(&from)
                        .await;
                }

                let behavior = terminal.get_behavior().clone();
                let bin = match tag {
                    DEFAULT_PULL_SYNC_REQ_TAG => HandlePushPullBinary::HandlePullData(Cursor::new(frame.payload().to_vec())),
                    DEFAULT_PULL_SYNC_ACK_TAG => HandlePushPullBinary::HandlePullAckData(Cursor::new(frame.payload().to_vec())),
                    DEFAULT_PUSH_SYNC_TAG => HandlePushPullBinary::HandlePushData(Cursor::new(frame.payload().to_vec())),
                    _ => unimplemented!(),
                };
                if let Err(e) = terminal
                    .get_transporter()
                    .handle_push_pull_events(from,
                                             behavior,
                                             tag,
                                             bin).await {
                    if e.kind() == ErrorKind::NotConnected {
                        //与对端还未建立连接，则加入P2P消息帧等待队列，等待后续处理
                        if let Err(e) = terminal.push_wait_frame(frame) {
                            warn!("Handle gossip failed, uid: {:?}, peer: {:?}, local: {:?}, stream_id: {:?}, tag: {:?}, reason: {:?}",
                            handle.get_uid(),
                            handle.get_remote(),
                            handle.get_local(),
                            stream_id,
                            tag,
                            e);
                        }

                        break;
                    }

                    error!("Handle gossip failed, uid: {:?}, peer: {:?}, local: {:?}, stream_id: {:?}, tag: {:?}, reason: {:?}",
                        handle.get_uid(),
                        handle.get_remote(),
                        handle.get_local(),
                        stream_id,
                        tag,
                        e);
                }
            },
            DEFAULT_HEARTBEAT_TAG => {
                //处理主机心跳
                let from = if let Some(host_id) = terminal.get_peer_host_id(&connection_id) {
                    //获取发送同步拉取请求的主机唯一id
                    host_id
                } else {
                    //与对端连接还未完成握手，则忽略对端主机心跳
                    continue;
                };

                let info = P2PHeartBeatInfo::from(frame.payload());

                //更新本地主机的P2P对端故障侦听器
                terminal
                    .update_host_heartbeat(&from)
                    .await;
            },
            tag => {
                //非Gossip协议的P2P消息帧
                let from = if let Some(host_id) = terminal.get_peer_host_id(&connection_id) {
                    //获取非Gossip协议的P2P消息帧的主机唯一id
                    host_id
                } else {
                    //与对端连接还未完成握手，则忽略非Gossip协议的P2P消息帧和其它P2P消息帧
                    if let Err(e) = terminal.push_wait_frame(frame) {
                        warn!("Handle gossip frame failed, uid: {:?}, peer: {:?}, local: {:?}, stream_id: {:?}, tag: {:?}, reason: {:?}",
                            handle.get_uid(),
                            handle.get_remote(),
                            handle.get_local(),
                            stream_id,
                            tag,
                            e);
                    }

                    break;
                };

                //更新本地主机的P2P对端故障侦听器
                terminal
                    .update_host_heartbeat(&from)
                    .await;

                other_frames.push(frame);
            },
        }
    }

    if other_frames.is_empty() {
        Ok(())
    } else {
        Err(other_frames)
    }
}

/*
* P2P传输器同步方法
*/
impl P2PTransporter {
    /// 创建P2P传输器
    pub fn new(shared: Arc<P2PServiceAdapter>) -> Self {
        P2PTransporter(shared)
    }

    /// 发送Gossip协议消息到指定的对端主机的主通道
    pub fn send_gossip_to<B>(&self,
                             to: &GossipNodeID,
                             bin: B) -> Result<()>
        where B: AsRef<[u8]> + 'static {
        if let Some(connection) = self
            .0
            .get_service()
            .get_terminal()
            .unwrap()
            .get_connection(to) {
            connection
                .send(connection.main_channel(),
                      bin)
        } else {
            Err(Error::new(ErrorKind::NotConnected,
                           format!("Send gossip to peer failed, to: {:?}, reason: peer not connected",
                                   to)))
        }
    }

    /// 发送非Gossip协议消息到指定的对端主机的指定通道
    pub fn send_to<B>(&self,
                      to: &GossipNodeID,
                      channel: ChannelId,
                      bin: B) -> Result<()>
        where B: AsRef<[u8]> + 'static {
        if let Some(connection) = self
            .0
            .get_service()
            .get_terminal()
            .unwrap()
            .get_connection(to) {
            connection
                .send(channel,
                      bin)
        } else {
            Err(Error::new(ErrorKind::NotConnected,
                           format!("Send to peer failed, to: {:?}, reason: peer not connected",
                                   to)))
        }
    }
}

/*
* P2P传输器异步方法
*/
impl P2PTransporter {
    /// 连接指定P2P主机，如果已连接则忽略连接
    /// 连接成功后会立即进行P2P握手，握手失败会立即关闭连接，如果已握手则忽略握手
    pub async fn connect_to(&self,
                            host_id: &GossipNodeID,
                            timeout: Option<usize>) -> Result<()> {
        let terminal = self
            .0
            .get_service()
            .get_terminal()
            .unwrap();

        //连接
        let connection = if let Some(connection) = terminal.get_connection(host_id) {
            //与指定的主机已建立连接，则忽略连接并立即握手
            connection
        } else {
            terminal
                .connect_to(host_id, timeout)
                .await?
        };

        //握手
        if terminal.is_handshaked(&connection.get_uid()) {
            //与指定的主机已握手，则忽略握手并立即返回
            Ok(())
        } else {
            //与指定的主机未握手，则向对端的主通道异步发送握手请求消息
            let req_info = P2PHandShakeInfo::new_req(terminal.get_self().clone());
            let mut req_frame = P2PFrame::new();
            req_frame.set_tag(DEFAULT_CONNECT_HANDSHAKE_TAG);
            let payload = BytesMut::from(req_info);
            req_frame.set_len(payload.len());
            req_frame.append_payload(payload.freeze());
            self
                .send_gossip_to(host_id,
                                req_frame.into_bytes())
        }
    }
}

