use std::sync::Arc;
use std::time::Duration;
use std::net::SocketAddr;
use std::fmt::{Debug, Formatter};
use std::io::{Error, Result, ErrorKind};

use dashmap::DashMap;
use quinn_proto::{StreamId, Dir};
use bytes::{Buf, Bytes};

use pi_async::rt::{AsyncValueNonBlocking,
                   serial_local_thread::LocalTaskRuntime};
use quic::{SocketHandle,
           client::QuicClientConnection};

use crate::terminal::Terminal;

///
/// P2P连接
///
pub struct Connection(Arc<InnerConnection>);

unsafe impl Send for Connection {}
unsafe impl Sync for Connection {}

impl Clone for Connection {
    fn clone(&self) -> Self {
        Connection(self.0.clone())
    }
}

impl Debug for Connection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f,
               "Connection[uid = {:?}, local = {:?}, peer = {:?}, channels = {:?}, closed: {:?}]",
               self.get_uid(),
               self.get_local(),
               self.get_peer(),
               self.channels_len(),
               self.is_closed())
    }
}

/*
* P2P连接同步方法
*/
impl Connection {
    /// 构建一个P2P连接
    #[inline]
    pub fn new(socket: PeerSocketHandle,
               terminal: Terminal) -> Self {
        let rt = match &socket {
            PeerSocketHandle::ClientConnection(con) => con.get_handle().get_runtime(),
            PeerSocketHandle::ClientSocket(handle) => handle.get_runtime(),
            PeerSocketHandle::ServerSocket(handle) => handle.get_runtime(),
        };

        let inner = InnerConnection {
            rt,
            socket,
            channels: DashMap::new(),
            terminal,
        };

        Connection(Arc::new(inner))
    }

    /// 获取Quic连接句柄
    pub fn get_handle(&self) -> &SocketHandle {
        match &self.0.socket {
            PeerSocketHandle::ClientConnection(con) => &con.get_handle(),
            PeerSocketHandle::ClientSocket(handle) => handle,
            PeerSocketHandle::ServerSocket(handle) => handle,
        }
    }

    /// 判断是否是客户端连接
    pub fn is_client(&self) -> bool {
        !self.is_server()
    }

    /// 判断是否是服务端连接
    pub fn is_server(&self) -> bool {
        if let PeerSocketHandle::ServerSocket(_) = &self.0.socket {
            true
        } else {
            false
        }
    }

    /// 获取当前连接的扩展通道数量
    pub fn channels_len(&self) -> usize {
        self
            .0
            .channels
            .len()
    }

    /// 检查指定唯一id的通道是否打开
    pub fn contains_channel(&self,
                            channel_id: &ChannelId) -> bool {
        if channel_id.0 == 0 {
            //主通道默认存在
            return true;
        }

        self
            .0
            .channels
            .contains_key(channel_id)
    }

    /// 获取连接的主通道
    pub fn main_channel(&self) -> ChannelId {
        let main_stream_id = self
            .get_handle()
            .get_main_stream_id()
            .unwrap()
            .0;

        ChannelId::new(main_stream_id)
    }

    /// 线程安全的判断连接是否关闭
    pub fn is_closed(&self) -> bool {
        self
            .get_handle()
            .is_closed()
    }

    /// 线程安全的获取连接唯一id
    pub fn get_uid(&self) -> usize {
        self
            .get_handle()
            .get_uid()
    }

    /// 获取Udp连接本地地址
    pub fn get_local_udp(&self) -> &SocketAddr {
        self.get_handle().get_local_udp()
    }

    /// 获取Udp连接对端地址
    pub fn get_peer_udp(&self) -> Option<&SocketAddr> {
        self.get_handle().get_remote_udp()
    }

    /// 线程安全的获取连接本地地址
    pub fn get_local(&self) -> SocketAddr {
        self
            .get_handle()
            .get_local()
    }

    /// 线程安全的获取连接对端地址
    pub fn get_peer(&self) -> SocketAddr {
        self
            .get_handle()
            .get_remote()
    }

    /// 获取当前连接的延迟估计
    pub fn get_latency(&self) -> Duration {
        self
            .get_handle()
            .get_latency()
    }

    /// 尝试同步非阻塞得接收指定流的当前数据
    pub fn try_recv(&self, channel_id: ChannelId) -> Option<Bytes> {
        self.try_recv_with_len(channel_id, 0)
    }

    /// 尝试同步非阻塞得接收指定流的指定长度的数据，如果len为0，则表示接收任意长度的数据，如果确实接收到数据，则保证接收到的数据长度>0
    /// 如果len>0，则表示最多只接收指定数量的数据，如果确实接收到数据，则保证接收到的数据长度==len
    pub fn try_recv_with_len(&self,
                             channel_id: ChannelId,
                             len: usize) -> Option<Bytes> {
        if !self.contains_channel(&channel_id) {
            //指定的通道不存在，则立即返回空
            return None;
        }

        let stream_id = StreamId(channel_id.into());
        if let Some(buffer) = self.get_handle().get_read_buffer(&stream_id) {
            if let Some(buf) = buffer.lock().as_mut() {
                //当前连接有读缓冲
                let remaining = buf.remaining();
                if (len > 0) && (remaining < len) {
                    //当前读缓冲中没有足够的数据
                    return None;
                }

                if (len == 0) && (remaining > 0) {
                    //用户需要读取任意长度的数据，且当前读缓冲区有足够的数据
                    Some(buf.copy_to_bytes(remaining))
                } else if (len > 0) && (remaining >= len) {
                    //用户需要读取指定长度的数据，且当前读缓冲区有足够的数据
                    Some(buf.copy_to_bytes(len))
                } else {
                    //用户需要读取任意长度的数据，且当前缓冲区没有足够的数据
                    None
                }
            } else {
                //当前连接没有读缓冲
                None
            }
        } else {
            //当前连接没有读缓冲
            None
        }
    }

    /// 线程安全的发送
    pub fn send<B>(&self,
                   channel_id: ChannelId,
                   buf: B) -> Result<()>
        where B: AsRef<[u8]> + 'static {
        if !self.contains_channel(&channel_id) {
            //指定的通道不存在，则忽略
            return Err(Error::new(ErrorKind::Other,
                                  format!("Send to connection failed, connection: {:?}, channel_id: {:?}, reason: channel not exist",
                                          self,
                                          channel_id)));
        }

        if let Some(host_id) = self
            .0
            .terminal
            .get_peer_host_id(&self.get_uid()) {
            //与指定连接的对端已握手，则暂时阻塞向指定连接的对端发送心跳
            self
                .0
                .terminal
                .block_heartbeat(host_id);
        }

        self
            .get_handle()
            .write_ready(StreamId(channel_id.into()), buf)
    }

    /// 关闭当前连接的指定通道
    pub fn close_channel(&self, channel_id: ChannelId) -> Result<()> {
        if !self.contains_channel(&channel_id) {
            //指定的通道不存在，则忽略
            return Ok(());
        }

        let _ = self.0.channels.remove(&channel_id);
        self
            .get_handle()
            .close_expanding_stream(StreamId(channel_id.into()))?;

        //注销关闭的指定通道
        let _ = self.0.channels.remove(&channel_id);

        Ok(())
    }

    /// 关闭连接，会关闭所有的通道
    pub fn close(self,
                 code: u32,
                 reason: Result<()>) -> Result<()> {
        self
            .get_handle()
            .close(code, reason)
    }
}

/*
* P2P连接异步方法
*/
impl Connection {
    /// 线程安全的异步打开新的通道
    pub async fn open_channel(&self) -> Result<ChannelId> {
        let value = AsyncValueNonBlocking::new();
        let value_copy = value.clone();
        if let Some(rt) = &self.0.rt {
            //连接所在运行时存在
            let connection = self.clone();
            rt.spawn(async move {
                match connection
                    .get_handle()
                    .open_expanding_stream(Dir::Bi)
                    .await {
                    Err(e) => {
                        value_copy.set(Err(e));
                    },
                    Ok(stream_id) => {
                        value_copy.set(Ok(ChannelId::new(stream_id.0)));
                    },
                }
            });
        } else {
            //连接所在运行时不存在
            return Err(Error::new(ErrorKind::Other,
                           format!("Open channel failed, connection: {:?}, reason: connection runtime not exist",
                                   self)));
        }

        //注册打开的新通道
        let channel_id = value.await?;
        self
            .0
            .channels
            .insert(channel_id, Channel::new());

        Ok(channel_id)
    }

    /// 线程安全的异步接收指定流的当前数据
    pub async fn recv(&self, channel_id: ChannelId) -> Option<Bytes> {
        self.recv_with_len(channel_id, 0).await
    }

    /// 线程安全的异步接收指定流的指定长度的数据，如果len为0，则表示接收任意长度的数据，如果确实接收到数据，则保证接收到的数据长度>0
    /// 如果len>0，则表示最多只接收指定数量的数据，如果确实接收到数据，则保证接收到的数据长度==len
    pub async fn recv_with_len(&self,
                               channel_id: ChannelId,
                               mut len: usize) -> Option<Bytes> {
        if !self.contains_channel(&channel_id) {
            //指定的通道不存在，则立即返回空
            return None;
        }

        let stream_id = StreamId(channel_id.into());
        let remaining = if let Some(len) = self.get_handle().read_buffer_remaining(&stream_id) {
            //当前连接有读缓冲
            len
        } else {
            //当前连接没有读缓冲
            return None;
        };

        let mut readed_len = 0;
        if remaining == 0 {
            //当前连接的读缓冲中没有数据，则异步准备读取数据
            readed_len = match self.get_handle().read_ready(&stream_id, len) {
                Err(r) => r,
                Ok(value) => {
                    value.await
                },
            };

            if readed_len == 0 {
                //当前连接已关闭，则立即退出
                return None;
            }
        } else {
            //当前连接的读缓冲中有数据
            readed_len = remaining;
        }

        if let Some(buffer) =  self.get_handle().get_read_buffer(&stream_id) {
            if let Some(buf) = buffer.lock().as_mut() {
                //当前连接有读缓冲
                if len == 0 {
                    //用户需要读取任意长度的数据
                    Some(buf.copy_to_bytes(readed_len))
                } else {
                    //用户需要读取指定长度的数据
                    Some(buf.copy_to_bytes(len))
                }
            } else {
                //当前连接没有读缓冲
                None
            }
        } else {
            //当前连接没有读缓冲
            None
        }
    }
}

// 内部P2P连接
struct InnerConnection {
    rt:         Option<LocalTaskRuntime<()>>,   //P2P连接所在运行时
    socket:     PeerSocketHandle,               //quic对等连接句柄
    channels:   DashMap<ChannelId, Channel>,    //连接的通道表
    terminal:   Terminal,                       //P2P终端
}

// 对等连接句柄
pub enum PeerSocketHandle {
    ClientConnection(QuicClientConnection), //客户端连接
    ClientSocket(SocketHandle),             //客户端原始连接
    ServerSocket(SocketHandle),             //服务端原始连接
}

///
/// P2P连接通道的唯一id
///
#[derive(PartialOrd, Ord, PartialEq, Eq, Hash, Debug, Clone, Copy)]
pub struct ChannelId(u64);

impl From<u64> for ChannelId {
    fn from(value: u64) -> Self {
        ChannelId(value)
    }
}

impl From<ChannelId> for u64 {
    fn from(value: ChannelId) -> Self {
        value.0
    }
}

impl ChannelId {
    /// 构建一个P2P连接通道的唯一id
    #[inline]
    pub fn new(inner: u64) -> Self {
        ChannelId(inner)
    }

    /// 获取P2P连接通道的内部值
    pub fn inner(&self) -> u64 {
        self.0
    }
}

// P2P连接通道
struct Channel {

}

impl Channel {
    /// 构建一个P2P连接通道
    pub fn new() -> Self {
        Channel {}
    }
}