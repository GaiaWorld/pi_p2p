use std::sync::Arc;
use std::time::Duration;
use std::convert::TryFrom;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::io::{Error, Result, ErrorKind};

use futures::future::{FutureExt, LocalBoxFuture};
use crossbeam::sync::ShardedLock;
use async_channel::{Sender as AsyncSender, Receiver as AsyncReceiver, unbounded as async_unbounded};
use pi_async::rt::AsyncValueNonBlocking;
use pi_gossip::{GossipNodeID,
                scuttlebutt::table::{Key, NodeChangingEvent}};
use pi_atom::Atom;
use dashmap::DashMap;
use bytes::{Bytes, BytesMut};
use ciborium::Value;
use log::{info, error};

use crate::{connection::{ChannelId, Connection},
            frame::{DEFAULT_SERVICE_SEND_INDEX, P2PFrame, P2PHandShakeInfo, P2PServiceReadInfo, P2PServiceWriteInfo},
            service::{DEFAULT_CONNECT_HANDSHAKE_TAG, P2PService, P2PServiceListener, send_to_service},
            terminal::Terminal};

// 默认的回应消息端口号
const DEFAULT_REPLY_PORT: u16 = 0;

// 默认的最小服务端口号
const DEFAULT_MIN_SERVICE_PORT: u16 = 1;

// 默认的最大服务端口号
const DEFAULT_MAX_SERVICE_PORT: u16 = 0xffff;

///
/// 默认的端口服务属性名字符串
///
pub const DEFAULT_PORT_SERVICE_KEY_STR: &str = "ports";

///
/// 端口服务
///
pub struct PortService(Arc<InnerPortService>);

impl Clone for PortService {
    fn clone(&self) -> Self {
        PortService(self.0.clone())
    }
}

impl P2PService for PortService {
    fn init(&self,
            local: GossipNodeID,
            terminal: Terminal,
            listener: Arc<P2PServiceListener>) {
        *self
            .0
            .terminal
            .write()
            .unwrap() = Some(terminal);
        *self
            .0
            .listener
            .write()
            .unwrap() = Some(listener);

        //解除监听本地主机属性改变
        self.unlisten_hosts(vec![local.clone()]);

        //通知本地主机已初始化
        if let Some(terminal) = self.get_terminal() {
            let service = self.clone();

            terminal.spawn_to(async move {
                let _ = service
                    .0
                    .event_sent
                    .send(PortEvent::Inited(local))
                    .await;
            });
        }
    }

    fn get_terminal(&self) -> Option<Terminal> {
        if let Some(terminal) = &*self.0.terminal.read().unwrap() {
            Some(terminal.clone())
        } else {
            None
        }
    }

    fn get_listener(&self) -> Option<Arc<P2PServiceListener>> {
        if let Some(listener) = &*self.0.listener.read().unwrap() {
            Some(listener.clone())
        } else {
            None
        }
    }

    fn changed(&self, event: NodeChangingEvent<32>) {
        if let Some(terminal) = self.get_terminal() {
            let service = self.clone();

            terminal.spawn_to(async move {
                let _ = service
                    .0
                    .event_sent
                    .send(PortEvent::HostChanged(event))
                    .await;
            });
        }
    }

    fn handshaked(&self,
                  peer: GossipNodeID,
                  _connection: Connection) -> LocalBoxFuture<'static, ()> {
        let service = self.clone();

        async move {
            let _ = service
                .0
                .event_sent
                .send(PortEvent::Handshaked(peer))
                .await;
        }.boxed_local()
    }

    fn received(&self,
                from: Option<GossipNodeID>,
                connection: Connection,
                channel_id: ChannelId,
                info: P2PServiceReadInfo) -> LocalBoxFuture<'static, ()> {
        let port = info.port;
        let index = info.index;
        let payload = info.payload;

        let service = self.clone();
        async move {
            if port == DEFAULT_REPLY_PORT {
                //接收到对端主机的请求回应消息
                if let Some((_key, result)) = service
                    .0
                    .response_map
                    .remove(&index) {
                    //指定端口和序号的消息是对应服务请求的回应消息，则立即回应
                    result.set(Ok(payload));
                }
            } else {
                //接收到对端主机发给指定服务端口的服务消息
                if let Some(item) = service.0.services.get(&port) {
                    //指定端口号的端口服务存在
                    if let Some(from) = from {
                        //来自对端主机的服务消息
                        let value = item.value();
                        let _ = value
                            .2
                            .send((Some((from, ReplyPeer(connection, channel_id, index))), index, payload))
                            .await;
                    } else {
                        //来自本地主机的服务消息
                        let value = item.value();
                        let _ = value
                            .2
                            .send((None, index, payload))
                            .await;
                    }
                }
            }
        }.boxed_local()
    }

    fn closed_channel(&self,
                      connection: Connection,
                      channel_id: ChannelId,
                      code: u32,
                      result: Result<()>) -> LocalBoxFuture<'static, ()> {
        if let Some(terminal) = self.get_terminal() {
            if let Some(peer) = terminal.get_peer_host_id(&connection.get_uid()) {
                let service = self.clone();

                return async move {
                    let _ = service
                        .0
                        .event_sent
                        .send(PortEvent::ClosedChannel(peer.clone(),
                                                       connection.channels_len(),
                                                       code,
                                                       result))
                        .await;

                    //注销已连接对端主机的端口
                    if let Some(((peer, _channel_id), port)) = service.0.ports_map.remove(&(peer, channel_id)) {
                        if let Some((_key, peer_port)) = service.0.ports.remove(&(peer, port)) {
                            peer_port.close();
                        }
                    }
                }.boxed_local();
            }
        }

        async move {

        }.boxed_local()
    }

    fn closed(&self,
              connection: Connection,
              code: u32,
              result: Result<()>) -> LocalBoxFuture<'static, ()> {
        if let Some(terminal) = self.get_terminal() {
            if let Some(peer) = terminal.get_peer_host_id(&connection.get_uid()) {
                let service = self.clone();

                return async move {
                    let _ = service
                        .0
                        .event_sent
                        .send(PortEvent::Closed(peer,
                                                code,
                                                result))
                        .await;
                }.boxed_local();
            }
        }

        async move {

        }.boxed_local()
    }
}

/*
* 端口服务同步方法
*/
impl PortService {
    /// 构建一个端口服务
    pub fn new() -> Self {
        let services = DashMap::new();
        let services_map = DashMap::new();
        let ports = DashMap::new();
        let ports_map = DashMap::new();
        let (event_sent, event_recv) = async_unbounded();
        let response_map = DashMap::new();

        let inner = InnerPortService {
            terminal: ShardedLock::new(None),
            listener: ShardedLock::new(None),
            services,
            services_map,
            ports,
            ports_map,
            event_recv,
            event_sent,
            response_map,
        };

        PortService(Arc::new(inner))
    }

    /// 获取端口服务所属P2P主机的唯一id
    pub fn local_host(&self) -> Option<GossipNodeID<32>> {
        if let Some(terminal) = self.get_terminal() {
            Some(terminal.get_self().clone())
        } else {
            None
        }
    }

    /// 判断是否有指定端口号的本地服务
    pub fn contains_local_port(&self, port: &u16) -> bool {
        self
            .0
            .services
            .contains_key(port)
    }

    /// 判断是否有指定端口名称的本地服务
    pub fn contains_local_port_name(&self, name: &str) -> bool {
        self
            .0
            .services_map
            .contains_key(&Atom::from(name))
    }

    /// 获取当前本地端口服务中的所有端口
    pub fn local_ports(&self) -> Vec<(u16, Option<String>, PortMode)> {
        let mut ports = Vec::with_capacity(self.0.services.len());

        for item in self.0.services.iter() {
            let key = item.key();
            let value = item.value();
            if let Some(name) = &value.0 {
                ports.push((*key, Some(name.as_str().to_string()), value.1));
            } else {
                ports.push((*key, None, value.1));
            }
        }

        ports
    }

    /// 获取指定对端主机的端口服务中的所有端口
    pub fn peer_ports(&self, peer: &GossipNodeID<32>) -> Vec<(u16, Option<String>, PortMode)> {
        if let Some(terminal) = self.get_terminal() {
            if let Some((value, _version)) = terminal
                .get_host_attr(peer,
                               &Key::from(DEFAULT_PORT_SERVICE_KEY_STR)) {
                if let Some(vec) = value.as_map() {
                    vec
                        .iter()
                        .map(|(key, val)| {
                            let port = u128::try_from(key.as_integer().unwrap()).unwrap() as u16;
                            let array = val.as_array().unwrap();
                            let name = if let Some(name) = array[0].as_text() {
                                Some(name.to_string())
                            } else {
                                None
                            };
                            let mode: PortMode = (u128::try_from(array[1].as_integer().unwrap()).unwrap() as u8).into();
                            (port, name, mode)
                        })
                        .collect()
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        } else {
            vec![]
        }
    }

    /// 在本地P2P主机上打开一个服务端口，可以设置服务名称，此服务端口将被延时广播到整个主机网络
    pub fn open_port(&self,
                     port: u16,
                     name: Option<&str>,
                     mode: PortMode) -> Result<(PeerPort, PortPipeLine)> {
        if self.0.services.contains_key(&port) {
            //指定的端口服务已存在
            return Err(Error::new(ErrorKind::Other,
                                  format!("Register service port failed, local: {:?}, port: {:?}, name: {:?}, reason: port already exist",
                                          self.local_host(),
                                          port,
                                          name)));
        }

        if port < DEFAULT_MIN_SERVICE_PORT
            || port > DEFAULT_MAX_SERVICE_PORT {
            //不允许的端口号
            return Err(Error::new(ErrorKind::Other,
                                  format!("Register service port failed, local: {:?}, port: {:?}, name: {:?}, reason: invalid port",
                                          self.local_host(),
                                          port,
                                          name)));
        }

        if let Some(local_host) = self.local_host() {
            if let Some(terminal) = self.get_terminal() {
                let key = Key::from(DEFAULT_PORT_SERVICE_KEY_STR);
                let mut ports_map = if let Some((value, _version)) = terminal.get_local_host_attr(&key) {
                    //本地主机当前有端口服务属性
                    if let Some(vec) = value.as_map() {
                        let mut map = vec
                            .iter()
                            .map(|(key, val)| {
                                (u128::try_from(key.as_integer().unwrap()).unwrap() as u16,
                                 val.clone())
                            })
                            .collect::<BTreeMap<u16, Value>>();

                        if map.contains_key(&port) {
                            //指定的端口已被占用
                            return Err(Error::new(ErrorKind::Other,
                                                  format!("Register service port failed, local: {:?}, port: {:?}, name: {:?}, reason: the specified port is already in use",
                                                          self.local_host(),
                                                          port,
                                                          name)));
                        }

                        map
                    } else {
                        BTreeMap::new()
                    }
                } else {
                    //本地主机当前没有端口服务属性
                    BTreeMap::new()
                };

                let (port_sent, port_recv) = async_unbounded();
                let peer_port = PeerPort::with_local(self.clone(),
                                                     port,
                                                     port_sent.clone());

                let pipeline = PortPipeLine::new(self.clone(),
                                                 local_host,
                                                 port,
                                                 port_recv);

                //注册服务端口
                let port_name = if let Some(name) = name {
                    let atom = Atom::from(name);
                    self
                        .0
                        .services
                        .insert(port, (Some(atom.clone()), mode, port_sent));
                    self.0.services_map.insert(atom, port);

                    name.to_string()
                } else {
                    self
                        .0
                        .services
                        .insert(port, (None, mode, port_sent));

                    String::new()
                };

                //在本地主机上绑定新注册的服务端口
                ports_map.insert(port,
                                 Value::Array(vec![Value::from(port_name), Value::from(mode as u8)]));
                let ports_vec = ports_map
                    .into_iter()
                    .map(|(key, val)| (Value::from(key), val))
                    .collect();
                terminal
                    .set_local_host_attr(Key::from(DEFAULT_PORT_SERVICE_KEY_STR),
                                         Value::Map(ports_vec));

                Ok((peer_port, pipeline))
            } else {
                Err(Error::new(ErrorKind::Other,
                               format!("Register service port failed, local: {:?}, port: {:?}, name: {:?}, reason: terminal not exist",
                                       self.local_host(),
                                       port,
                                       name)))
            }
        } else {
            Err(Error::new(ErrorKind::Other,
                           format!("Register service port failed, local: {:?}, port: {:?}, name: {:?}, reason: local host not exist",
                                   self.local_host(),
                                   port,
                                   name)))
        }
    }

    /// 尝试轮询当前端口服务的端口事件
    pub fn try_poll_event(&self) -> Option<PortEvent> {
        match self.0.event_recv.try_recv() {
            Err(_) => {
                None
            },
            Ok(event) => {
                Some(event)
            },
        }
    }
}

/*
* 端口服务异步方法
*/
impl PortService {
    /// 异步连接到指定对端主机的指定服务端口号，成功返回对端主机的端口，失败则返回错误原因
    /// 连接者必须首先发起通讯，否则对端将无法知道已经连接
    pub async fn connect_to(&self,
                            peer: &GossipNodeID<32>,
                            port: u16,
                            timeout: usize) -> Result<PeerPort> {
        if let Some(local_host) = self.local_host() {
            if peer == &local_host {
                //连接本地主机的服务端口
                if let Some(item) = self.0.services.get(&port) {
                    //指定的端口号存在
                    let value = item.value();
                    return Ok(PeerPort::with_local(self.clone(),
                                                   port,
                                                   value.2.clone()));
                }
            }
        }

        //连接对端主机的服务端口
        if let Some(item) = self.0.ports.get(&(peer.clone(), port)) {
            //已连接对端主机的指定服务端口，则立即返回
            let peer_port = item.value();
            Ok(peer_port.clone())
        } else {
            //未连接对端主机的指定服务端口
            if let Some(terminal) = self.get_terminal() {
                //建立网络连接
                let connection = if let Some(connection) = terminal.get_connection(peer) {
                    //与对端主机已建立连接
                    connection
                } else {
                    //与对端主机未建立连接
                    match terminal.connect_to(peer, Some(timeout)).await {
                        Err(e) => {
                            //与对端主机建立连接失败，则立即返回错误原因
                            return Err(Error::new(ErrorKind::Other,
                                                  format!("Connect to peer port failed, local: {:?}, peer: {:?}, port: {:?}, reason: {:?}",
                                                          self.local_host(),
                                                          peer,
                                                          port,
                                                          e)));
                        },
                        Ok(connection) => connection,
                    }
                };

                //进行连接握手
                if !terminal.is_handshaked(&connection.get_uid()) {
                    //与指定的主机未握手，则向对端的主通道异步发送握手请求消息
                    let req_info = P2PHandShakeInfo::new_req(terminal.get_self().clone());
                    let mut req_frame = P2PFrame::new();
                    req_frame.set_tag(DEFAULT_CONNECT_HANDSHAKE_TAG);
                    let payload = BytesMut::from(req_info);
                    req_frame.set_len(payload.len());
                    req_frame.append_payload(payload.freeze());

                    if let Err(e) = connection
                        .send(connection.main_channel(),
                              req_frame.into_bytes()) {
                        //发送握手请求失败，则立即返回错误原因
                        return Err(Error::new(ErrorKind::Other,
                                              format!("Connect to peer port failed, local: {:?}, peer: {:?}, port: {:?}, reason: {:?}",
                                                      self.local_host(),
                                                      peer,
                                                      port,
                                                      e)));
                    }
                }

                //打开连接通道
                let channel_id = match connection.open_channel().await {
                    Err(e) => {
                        //打开连接的通道失败，则立即返回错误原因
                        return Err(Error::new(ErrorKind::Other,
                                              format!("Connect to peer port failed, local: {:?}, peer: {:?}, port: {:?}, reason: {:?}",
                                                      self.local_host(),
                                                      peer,
                                                      port,
                                                      e)));
                    },
                    Ok(channel_id) => {
                        //打开连接的通道成功
                        channel_id
                    },
                };

                //注册已连接对端主机的指定服务端口
                let peer_port = PeerPort::with_peer(self.clone(),
                                                    peer.clone(),
                                                    connection,
                                                    channel_id,
                                                    port);
                self
                    .0
                    .ports
                    .insert((peer.clone(), port),
                            peer_port.clone());
                self
                    .0
                    .ports_map
                    .insert((peer.clone(), channel_id),
                            port);

                //建立通道端，必须首先发起请求
                peer_port.send(0, &[]);

                Ok(peer_port)
            } else {
                Err(Error::new(ErrorKind::Other,
                               format!("Connect to peer port failed, local: {:?}, peer: {:?}, port: {:?}, reason: terminal not exist",
                                       self.local_host(),
                                       peer,
                                       port)))
            }
        }
    }

    /// 异步轮询当前端口服务的端口事件
    pub async fn poll_event(&self) -> Result<PortEvent> {
        match self.0.event_recv.recv().await {
            Err(e) => {
                Err(Error::new(ErrorKind::Other,
                               format!("Poll port event failed, reason: {:?}", e)))
            },
            Ok(event) => {
                Ok(event)
            },
        }
    }
}

// 内部端口服务
struct InnerPortService {
    //P2P终端
    terminal:           ShardedLock<Option<Terminal>>,
    //P2P服务端监听器
    listener:           ShardedLock<Option<Arc<P2PServiceListener>>>,
    //本地端口服务表
    services:           DashMap<u16, (Option<Atom>, PortMode, AsyncSender<(Option<(GossipNodeID<32>, ReplyPeer)>, u32, Bytes)>)>,
    //本地端口服务名称映射表
    services_map:       DashMap<Atom, u16>,
    //已连接到对端主机的端口表
    ports:              DashMap<(GossipNodeID<32>, u16), PeerPort>,
    //已连接到对端主机的通道映射表
    ports_map:          DashMap<(GossipNodeID<32>, ChannelId), u16>,
    //端口服务事件接收器
    event_recv:         AsyncReceiver<PortEvent>,
    //端口服务事件发送器
    event_sent:         AsyncSender<PortEvent>,
    //端口请求回应表
    response_map:       DashMap<u32, AsyncValueNonBlocking<Result<Bytes>>>,
}

///
/// 端口服务管道，用于接收对端主机通过端口发送的P2P服务消息
///
pub struct PortPipeLine {
    service:    PortService,                                                        //端口服务管道所属的端口服务
    local:      GossipNodeID<32>,                                                   //本地主机唯一id
    port:       u16,                                                                //端口号
    receiver:   AsyncReceiver<(Option<(GossipNodeID<32>, ReplyPeer)>, u32, Bytes)>, //P2P服务消息接收器
}

impl Debug for PortPipeLine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f,
               "PortPipeLine[local = {:?}, port = {:?}, queue: {:?}]",
               self.local,
               self.port,
               self.receiver.len())
    }
}

impl Drop for PortPipeLine {
    fn drop(&mut self) {
        if let Some((_key, (name, mode, _sender))) = self.service.0.services.remove(&self.port) {
            //注销指定的端口服务
            let port_name = if let Some(name) = name {
                self
                    .service
                    .0
                    .services_map
                    .remove(&name);

                Some(name.as_str().to_string())
            } else {
                None
            };

            //解绑定指定的端口服务
            if let Some(terminal) = self.service.get_terminal() {
                let mut ports_map = if let Some((value, _version)) = terminal.get_local_host_attr(&Key::from(DEFAULT_PORT_SERVICE_KEY_STR)) {
                    //本地主机当前端口服务属性
                    if let Some(vec) = value.as_map() {
                        let mut map = vec
                            .iter()
                            .map(|(key, val)| {
                                (u128::try_from(key.as_integer().unwrap()).unwrap() as u16,
                                 val.clone())
                            })
                            .collect::<BTreeMap<u16, Value>>();

                        //从本地主机当前端口服务属性中移除指定端口号的服务
                        let _ = map.remove(&self.port);
                        map
                    } else {
                        BTreeMap::new()
                    }
                } else {
                    //本地主机当前没有端口服务属性
                    BTreeMap::new()
                };

                ports_map.remove(&self.port);
                let ports_vec = ports_map
                    .into_iter()
                    .map(|(key, val)| (Value::from(key), val))
                    .collect();
                terminal
                    .set_local_host_attr(Key::from(DEFAULT_PORT_SERVICE_KEY_STR),
                                         Value::Map(ports_vec));
            }

            info!("Unregister service port successed, local: {:?}, port: {:?}, name: {:?}, mode: {:?}",
                self.local,
                self.port,
                mode,
                port_name);
        }
    }
}

/*
* 端口服务管道同步方法
*/
impl PortPipeLine {
    /// 构建一个端口服务管道
    pub fn new(service: PortService,
               local: GossipNodeID<32>,
               port: u16,
               receiver: AsyncReceiver<(Option<(GossipNodeID<32>, ReplyPeer)>, u32, Bytes)>) -> Self {
        PortPipeLine {
            service,
            local,
            port,
            receiver,
        }
    }

    /// 获取端口服务管道绑定的端口号
    pub fn port(&self) -> u16 {
        self.port
    }

    /// 获取端口服务管道绑定的名称
    pub fn name(&self) -> Option<String> {
        if let Some(item) = self.service.0.services.get(&self.port()) {
            let value = item.value();
            if let Some(name) = &value.0 {
                Some(name.as_str().to_string())
            } else {
                None
            }
        } else {
            None
        }
    }

    /// 获取端口服务管道的长度
    pub fn len(&self) -> usize {
        self.receiver.len()
    }

    /// 判断是否已连接指定对端主机
    pub fn is_connected(&self, peer: &GossipNodeID<32>) -> bool {
        if let Some(terminal) = self.service.get_terminal() {
            terminal.is_connected(peer)
        } else {
            false
        }
    }

    /// 获取与指定对端主机的网络延迟
    pub fn network_delay(&self, peer: &GossipNodeID<32>) -> Option<Duration> {
        if let Some(terminal) = self.service.get_terminal() {
            terminal.get_connection_delay(peer)
        } else {
            None
        }
    }

    /// 关闭端口服务管道
    pub fn close(self) {
        info!("Closeing service port, local: {:?}, port: {:?}",
                self.local,
                self.port);
    }
}

/*
* 端口服务管道异步方法
*/
impl PortPipeLine {
    /// 异步轮询端口服务管道中的P2P服务消息，返回发送消息的对端主机唯一id，消息序号和P2P服务消息负载
    pub async fn poll(&self) -> Result<(Option<(GossipNodeID<32>, ReplyPeer)>, u32, Bytes)> {
        match self.receiver.try_recv() {
            Ok(msg) => {
                //当前有服务消息，则立即返回
                Ok(msg)
            },
            Err(e) if e.is_closed() => {
                //当前通道已关闭，则立即返回空
                Ok((None, 0, Bytes::new()))
            },
            Err(_) => {
                //当前没有任何服务消息，则异步等待
                match self
                    .receiver
                    .recv()
                    .await {
                    Err(e) => {
                        Err(Error::new(ErrorKind::Other,
                                       format!("Poll port failed, self: {:?}, reason: {:?}",
                                               self,
                                               e)))
                    },
                    Ok((peer_info, index, bytes)) => {
                        Ok((peer_info, index, bytes))
                    },
                }
            },
        }
    }
}

///
/// 对端主机端口，用于向对端主机的端口服务发送P2P服务消息
///
pub struct PeerPort(Arc<InnerPeerPort>);

impl Clone for PeerPort {
    fn clone(&self) -> Self {
        PeerPort(self.0.clone())
    }
}

impl Debug for PeerPort {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0.as_ref() {
            InnerPeerPort::Local(_service, port, _sender) => {
                let local = self.peer();
                write!(f,
                       "PeerPort::Local[local = {:?}, port = {:?}]",
                       local,
                       port)
            },
            InnerPeerPort::Peer(service, peer, connection, channel_id, port) => {
                write!(f,
                       "PeerPort::Peer[local = {:?}, peer = {:?}, connection = {:?}, channel_id = {:?}, port = {:?}]",
                       service.local_host(),
                       peer,
                       connection,
                       channel_id,
                       port)
            },
        }
    }
}

/*
* 对端主机端口同步方法
*/
impl PeerPort {
    /// 构建一个用于指向本地主机的服务端口
    pub fn with_local(service: PortService,
                      port: u16,
                      sender: AsyncSender<(Option<(GossipNodeID<32>, ReplyPeer)>, u32, Bytes)>) -> Self {
        let inner = InnerPeerPort::Local(service,
                                         port,
                                         sender);

        PeerPort(Arc::new(inner))
    }

    /// 构建一个用于指定向对端主机的服务端口
    pub fn with_peer(service: PortService,
                     peer: GossipNodeID<32>,
                     connection: Connection,
                     channel_id: ChannelId,
                     port: u16) -> Self {
        let inner = InnerPeerPort::Peer(service,
                                        peer,
                                        connection,
                                        channel_id,
                                        port);

        PeerPort(Arc::new(inner))
    }

    /// 是否是本地主机的服务端口
    pub fn is_local(&self) -> bool {
        if let InnerPeerPort::Local(_, _, _) = self.0.as_ref() {
            true
        } else {
            false
        }
    }

    /// 是否是对端主机的服务端口
    pub fn is_peer(&self) -> bool {
        if let InnerPeerPort::Peer(_, _, _, _, _) = self.0.as_ref() {
            true
        } else {
            false
        }
    }

    /// 获取对端主机的唯一id
    pub fn peer(&self) -> Option<GossipNodeID<32>> {
        match self.0.as_ref() {
            InnerPeerPort::Local(service, _port, _sender) => {
                service.local_host()
            },
            InnerPeerPort::Peer(_service, peer, _connection, _channel_id, _port) => {
                Some(peer.clone())
            },
        }
    }

    /// 获取对端主机的端口号
    pub fn port(&self) -> u16 {
        match self.0.as_ref() {
            InnerPeerPort::Local(_service, port, _sender) => *port,
            InnerPeerPort::Peer(_service, _peer, _connection, _channel_id, port) => *port,
        }
    }

    /// 判断是否已连接端口所在对端主机
    pub fn is_connected(&self) -> bool {
        if let InnerPeerPort::Peer(service, peer, _connection, _channel_id, _port) = self.0.as_ref() {
            //是对端主机的端口
            if let Some(terminal) = service.get_terminal() {
                terminal.is_connected(peer)
            } else {
                false
            }
        } else {
            //是本地主机的端口
            true
        }
    }

    /// 获取与端口所在对端主机的网络延迟
    pub fn network_delay(&self) -> Option<Duration> {
        if let InnerPeerPort::Peer(service, peer, _connection, _channel_id, _port) = self.0.as_ref() {
            //是对端主机的端口
            if let Some(terminal) = service.get_terminal() {
                terminal.get_connection_delay(peer)
            } else {
                None
            }
        } else {
            //是本地主机的端口
            None
        }
    }

    /// 向对端主机的端口发送指定序号的P2P服务消息
    pub fn send<B: AsRef<[u8]>>(&self,
                                index: u32,
                                payload: B) -> Result<()> {
        match self.0.as_ref() {
            InnerPeerPort::Local(service, _port, sender) => {
                //向本地主机的服务端口发送P2P服务消息的负载
                if let Some(terminal) = service.get_terminal() {
                    let sender = sender.clone();
                    let bytes = BytesMut::from(payload.as_ref());

                    terminal.spawn_to(async move {
                        let _ = sender
                            .send((None, index, bytes.freeze()))
                            .await;
                    });
                }

                Ok(())
            },
            InnerPeerPort::Peer(_service, _peer, connection, channel_id, port) => {
                //向对端主机的服务端口发送P2P服务消息
                let bytes = BytesMut::from(payload.as_ref());
                let info = P2PServiceWriteInfo {
                    port: *port,
                    index,
                    payload: bytes,
                };
                send_to_service(connection, channel_id, info)
            },
        }
    }

    /// 关闭当前共享的对端主机的服务端口
    pub fn close(self) {

    }
}

/*
* 对端主机端口异步方法
*/
impl PeerPort {
    /// 获取端口所在对端主机的故障估值
    pub async fn peer_failure(&self) -> Option<f64> {
        if let InnerPeerPort::Peer(service, peer, _connection, _channel_id, _port) = self.0.as_ref() {
            //是对端主机的端口
            if let Some(terminal) = service.get_terminal() {
                terminal.failure_valuation(peer).await
            } else {
                None
            }
        } else {
            //是本地主机的端口
            None
        }
    }

    /// 向对端主机的端口发送指定序号的P2P服务消息，并等待对端的回应消息
    pub async fn request<B: AsRef<[u8]>>(&self,
                                         index: u32,
                                         payload: B,
                                         timeout: usize) -> Result<Bytes> {
        match self.0.as_ref() {
            InnerPeerPort::Local(service, port, _sender) => {
                //不允许向本地主机端口发送请求
                Err(Error::new(ErrorKind::Other,
                               format!("Request to peer service failed, peer: {:?}, port: {:?}, index: {:?}, timeout: {:?}, reason: disable local request",
                                       service.local_host(),
                                       port,
                                       index,
                                       timeout)))
            },
            InnerPeerPort::Peer(service, peer, _connection, _channel_id, port) => {
                if let Some(terminal) = service.get_terminal() {
                    //注册请求到端口服务的请求回应表
                    let result = AsyncValueNonBlocking::new();
                    let result_copy = result.clone();
                    let service = service.clone();
                    service
                        .0
                        .response_map
                        .insert(index, result_copy);

                    //设置请求超时
                    let peer = peer.clone();
                    let port = *port;
                    terminal.spawn_timeout(async move {
                        if let Some((_key, result)) = service
                            .0
                            .response_map
                            .remove(&index) {
                            //指定端口和序号的请求存在，则回应已超时
                            result.set(Err(Error::new(ErrorKind::TimedOut,
                                                      format!("Request to peer service failed, peer: {:?}, port: {:?}, index: {:?}, timeout: {:?}, reason: request already timeout",
                                                              peer,
                                                              port,
                                                              index,
                                                              timeout))));
                        }
                    }, timeout);

                    //发送请求到对端主机的端口
                    let _ = self.send(index, payload)?;

                    //异步等待对端主机的回应
                    result.await
                } else {
                    Err(Error::new(ErrorKind::Other,
                                   format!("Request to peer service failed, peer: {:?}, port: {:?}, index: {:?}, timeout: {:?}, reason: terminal not exist",
                                           peer,
                                           port,
                                           index,
                                           timeout)))
                }
            },
        }
    }
}

// 内部对端主机的端口
pub(crate) enum InnerPeerPort {
    Local(PortService, u16, AsyncSender<(Option<(GossipNodeID<32>, ReplyPeer)>, u32, Bytes)>),  //指向本地主机端口
    Peer(PortService, GossipNodeID<32>, Connection, ChannelId, u16),                            //指定向对端主机端口
}

impl Drop for InnerPeerPort {
    fn drop(&mut self) {
        if let InnerPeerPort::Peer(service,
                                   peer,
                                   connection,
                                   channel_id,
                                   port) = self {
            //当前端口指向的是对端主机的端口
            let _ = service
                .0
                .ports_map
                .remove(&(peer.clone(), channel_id.clone()));
            let _ = service
                .0
                .ports
                .remove(&(peer.clone(), *port));

            //关闭连接对端主机端口的通道
            if let Some(terminal) = service.get_terminal() {
                if let Err(e) = terminal.close_channel_to(peer, channel_id.clone()) {
                    error!("Unregister peer port failed, local: {:?}, peer: {:?}, port: {:?}, connection: {:?}, channel_id: {:?}, reason: {:?}",
                        service.local_host(),
                        peer,
                        port,
                        connection,
                        channel_id,
                        e);
                    return;
                }
            }

            info!("Unregister peer port successed, local: {:?}, peer: {:?}, port: {:?}, connection: {:?}, channel_id: {:?}",
                service.local_host(),
                peer,
                port,
                connection,
                channel_id);
        }
    }
}

///
/// 端口工作模式
///
#[derive(Debug, Clone, Copy)]
pub enum PortMode {
    Send = 1,   //发送
    Request,    //请求
    Unreliable, //不可靠
}

impl From<PortMode> for u8 {
    fn from(value: PortMode) -> Self {
        match value {
            PortMode::Send => 1,
            PortMode::Request => 2,
            PortMode::Unreliable => 3,
        }
    }
}

impl From<u8> for PortMode {
    fn from(value: u8) -> Self {
        match value {
            1 => PortMode::Send,
            2 => PortMode::Request,
            _ => PortMode::Unreliable,
        }
    }
}

///
/// 端口事件
///
#[derive(Debug)]
pub enum PortEvent {
    Inited(GossipNodeID<32>),                                   //本地主机已初始化
    Handshaked(GossipNodeID<32>),                               //与对端主机已握手
    HostChanged(NodeChangingEvent),                             //主机已改变
    ClosedChannel(GossipNodeID<32>, usize, u32, Result<()>),    //关闭与对端主机的通道
    Closed(GossipNodeID<32>, u32, Result<()>),                  //关闭与对端主机的连接
}

impl PortEvent {
    /// 是否是本机已初始化事件
    pub fn is_inited(&self) -> bool {
        if let PortEvent::Inited(_) = self {
            true
        } else {
            false
        }
    }

    /// 是否是与对端主机已握手事件
    pub fn is_handshaked(&self) -> bool {
        if let PortEvent::Handshaked(_) = self {
            true
        } else {
            false
        }
    }

    /// 是否是主机已改变事件
    pub fn is_host_changed(&self) -> bool {
        if let PortEvent::HostChanged(_) = self {
            true
        } else {
            false
        }
    }

    /// 是否是关闭与对端主机的通道事件
    pub fn is_closed_channel(&self) -> bool {
        if let PortEvent::ClosedChannel(_, _, _, _) = self {
            true
        } else {
            false
        }
    }

    /// 是否是关闭与对端主机的连接事件
    pub fn is_close(&self) -> bool {
        if let PortEvent::Closed(_, _, _) = self {
            true
        } else {
            false
        }
    }
}

/// 回应对端
#[derive(Debug)]
pub struct ReplyPeer(Connection, ChannelId, u32);

impl ReplyPeer {
    /// 获取回应消息序号
    pub fn port(&self) -> u32 {
        self.2
    }

    /// 回应对端
    pub fn reply<B: AsRef<[u8]>>(self, payload: B) -> Result<()> {
        let bytes = BytesMut::from(payload.as_ref());
        let info = P2PServiceWriteInfo {
            port: DEFAULT_REPLY_PORT,
            index: self.2,
            payload: bytes,
        };

        send_to_service(&self.0, &self.1, info)
    }
}