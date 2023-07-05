use std::convert::TryFrom;
use std::fmt::{Debug, Formatter};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use pi_gossip::GossipNodeID;

// 单字节负载长度限制
const SINGLE_BYTE_LEN_LIMIT: u8 = 0xfe;

// 双字节负载长度限制
const DOUBLE_BYTE_LEN_LIMIT: u8 = 0xff;

///
/// P2P消息帧
///
pub struct P2PFrame {
    tag:        Option<u8>,             //帧标记
    len:        ParsedPayloadLength,    //帧负载长度
    payload:    BytesMut,               //帧负载
}

impl Debug for P2PFrame {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f,
               "P2PFrame[tag = {:?}, len = {:?}, payload_len = {:?}]",
               self.tag,
               self.len,
               self.payload.len())
    }
}

impl P2PFrame {
    /// 构建一个P2P消息帧
    pub fn new() -> Self {
        P2PFrame {
            tag: None,
            len: ParsedPayloadLength::Wait,
            payload: BytesMut::new(),
        }
    }

    /// 获取P2P消息帧的标记
    pub fn tag(&self) -> Option<u8> {
        self.tag
    }

    /// 设置P2P消息帧的标记
    pub fn set_tag(&mut self, tag: u8) {
        self.tag = Some(tag);
    }

    /// 获取P2P消息帧的负载长度
    pub fn len(&self) -> Option<usize> {
        if let ParsedPayloadLength::Complated(len) = self.len {
            Some(len)
        } else {
            None
        }
    }

    /// 设置P2P消息帧的负载长度
    pub fn set_len(&mut self, len: usize) {
        self.len = ParsedPayloadLength::Complated(len);
    }

    /// 获取P2P消息帧的负载
    pub fn payload(&self) -> &[u8] {
        self.payload.as_ref()
    }

    /// 向P2P消息帧追加负载
    pub fn append_payload<B: Buf>(&mut self, buf: B) {
        self
            .payload
            .put(buf);
    }

    /// 分析指定的P2P通讯数据，生成完整的P2P消息帧
    pub fn parse(&mut self, bin: &mut BytesMut) -> ParseFrameResult {
        //分析标记和负载长度
        if self.tag.is_none() {
            //需要分析P2P消息帧的标记
            if bin.remaining() > 0 {
                //当前可以分析P2P消息帧的标记
                self.tag = Some(bin.get_u8());

                let remaining = bin.remaining();
                if remaining > 0 {
                    //开始分析P2P消息帧的负载长度
                    match bin.get_u8() {
                        SINGLE_BYTE_LEN_LIMIT => {
                            //已达单字节长度的上限
                            self.len = ParsedPayloadLength::Partial(2,
                                                                    Vec::with_capacity(2));
                            if bin.remaining() == 0 {
                                //没有未分析的字节
                                return ParseFrameResult::Partial;
                            }
                        },
                        DOUBLE_BYTE_LEN_LIMIT => {
                            //已达双节字长度的上限
                            self.len = ParsedPayloadLength::Partial(4,
                                                                    Vec::with_capacity(4));
                            if bin.remaining() == 0 {
                                //没有未分析的字节
                                return ParseFrameResult::Partial;
                            }
                        },
                        len => {
                            //未达单字节长度的上限
                            self.len = ParsedPayloadLength::Complated(len as usize);
                        },
                    }
                } else {
                    //当前无法继续分析P2P消息帧的负载长度
                    self.len = ParsedPayloadLength::Wait;
                    return ParseFrameResult::Partial;
                }
            } else {
                //当前无法分析P2P消息帧的标记
                return ParseFrameResult::Partial;
            }
        }

        //分析负载长度
        match self.len.is_partial() {
            Some(0) => {
                //分析负载长度已完成
            },
            Some(n) => {
                //还需要等待n个字节来完成负载长度分析
                let remaining = bin.remaining();
                let readable_len = if remaining >= n {
                    //可以继续分析长度
                    n
                } else {
                    //还需要继续等待指定数量的字节来完成长度分析
                    remaining
                };

                match readable_len {
                    1 => {
                        if let ParsedPayloadLength::Partial(current, buf) = &mut self.len {
                            *current = n - readable_len;
                            buf.put_u8(bin.get_u8());
                        } else {
                            unimplemented!()
                        }
                    },
                    2 => {
                        if let ParsedPayloadLength::Partial(current, buf) = &mut self.len {
                            *current = n - readable_len;
                            buf.put_u16_le(bin.get_u16_le());
                        } else {
                            unimplemented!()
                        }
                    },
                    3 => {
                        if let ParsedPayloadLength::Partial(current, buf) = &mut self.len {
                            *current = n - readable_len;
                            buf.put_u8(bin.get_u8());
                            buf.put_u8(bin.get_u8());
                            buf.put_u8(bin.get_u8());
                        } else {
                            unimplemented!()
                        }
                    },
                    4 => {
                        if let ParsedPayloadLength::Partial(current, buf) = &mut self.len {
                            *current = n - readable_len;
                            buf.put_u32_le(bin.get_u32_le());
                        } else {
                            unimplemented!()
                        }
                    },
                    _ => unimplemented!(),
                }

                let mut real_len = None;
                if let ParsedPayloadLength::Partial(current, buf) = &mut self.len {
                    if *current > 0 {
                        //还需要继续等待指定数量的字节来完成长度分析
                        return ParseFrameResult::Partial;
                    } else {
                        //有足够数量的字节来完成长度分析
                        match buf.len() {
                            2 => {
                                //分析双字节长度
                                real_len = Some(buf.as_slice().get_u16_le() as usize);
                            },
                            4 => {
                                //分析四字节长度
                                real_len = Some(buf.as_slice().get_u32_le() as usize);
                            },
                            _ => unimplemented!(),
                        }
                    }
                }

                //完成负载长度分析
                let real_len = real_len.unwrap();
                self.len = ParsedPayloadLength::Complated(real_len);
            },
            None => {
                //开始分析P2P消息帧的负载长度
                let remaining = bin.remaining();
                if remaining > 0 {
                    //继续分析P2P消息帧的负载长度
                    match bin.get_u8() {
                        SINGLE_BYTE_LEN_LIMIT => {
                            //已达单字节长度的上限
                            self.len = ParsedPayloadLength::Partial(2,
                                                                    Vec::with_capacity(2));
                            return ParseFrameResult::Partial;
                        },
                        DOUBLE_BYTE_LEN_LIMIT => {
                            //已达双节字长度的上限
                            self.len = ParsedPayloadLength::Partial(4,
                                                                    Vec::with_capacity(4));
                            return ParseFrameResult::Partial;
                        },
                        len => {
                            //未达单字节长度的上限，则完成负载长度的分析
                            self.len = ParsedPayloadLength::Complated(len as usize);
                        },
                    }
                } else {
                    //当前无法继续分析P2P消息帧的负载长度
                    self.len = ParsedPayloadLength::Wait;
                    return ParseFrameResult::Partial;
                }
            },
        }

        //分析负载
        let remaining = bin.remaining();
        let require_payload_len = self
            .len
            .complated_len()
            .checked_sub(self.payload.remaining())
            .unwrap_or(0);
        let readable_len = if remaining >= require_payload_len {
            require_payload_len
        } else {
            remaining
        };
        let bytes = bin.copy_to_bytes(readable_len);
        self.payload.put(bytes);

        if self
            .len
            .complated_len() <= self.payload.remaining() {
            //解析当前P2P消息已完成
            if bin.remaining() > 0 {
                //有剩余未解析的数据
                ParseFrameResult::NextFrame
            } else {
                //没有剩余未解析的数据
                ParseFrameResult::Complated
            }
        } else {
            //解析当前P2P消息未完成
            ParseFrameResult::Partial
        }
    }

    /// 将P2P消息帧序列化为二进制缓冲
    pub fn into_bytes(self) -> Bytes {
        let mut buf = BytesMut::new();

        //序列化消息帧的tag
        if let Some(tag) = self.tag {
            buf.put_u8(tag);
        } else {
            buf.put_u8(0);
        }

        //序列化消息帧的负载长度
        if let ParsedPayloadLength::Complated(len) = self.len {
            if len < SINGLE_BYTE_LEN_LIMIT as usize {
                //单字节长度
                buf.put_u8(len as u8);
            } else if len < 0x10000 {
                //两字节长度
                buf.put_u8(SINGLE_BYTE_LEN_LIMIT);
                buf.put_u16_le(len as u16);
            } else {
                //四字节长度
                buf.put_u8(DOUBLE_BYTE_LEN_LIMIT);
                buf.put_u32_le(len as u32);
            }
        } else {
            buf.put_u8(0);
        }

        //序列化消息帧的负载
        buf.put(self.payload);

        buf.freeze()
    }

    /// 将P2P消息帧序列化为二进制缓冲
    pub fn into_vec(self) -> Vec<u8> {
        let mut buf = Vec::new();

        //序列化消息帧的tag
        if let Some(tag) = self.tag {
            buf.put_u8(tag);
        } else {
            buf.put_u8(0);
        }

        //序列化消息帧的负载长度
        if let ParsedPayloadLength::Complated(len) = self.len {
            if len < SINGLE_BYTE_LEN_LIMIT as usize {
                //单字节长度
                buf.put_u8(len as u8);
            } else if len < 0x10000 {
                //两字节长度
                buf.put_u8(SINGLE_BYTE_LEN_LIMIT);
                buf.put_u16_le(len as u16);
            } else {
                //四字节长度
                buf.put_u8(DOUBLE_BYTE_LEN_LIMIT);
                buf.put_u32_le(len as u32);
            }
        } else {
            buf.put_u8(0);
        }

        //序列化消息帧的负载
        buf.put(self.payload);

        buf
    }
}

// 分析后的负载长度
enum ParsedPayloadLength {
    Wait,                       //等待开始分析负载长度
    Partial(usize, Vec<u8>),    //已分析了部分
    Complated(usize),           //已分析完成
}

impl Debug for ParsedPayloadLength {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ParsedPayloadLength::Wait => {
                write!(f,
                       "ParsedPayloadLength::Wait")
            },
            ParsedPayloadLength::Partial(n, buf) => {
                write!(f,
                       "ParsedPayloadLength::Partial({:?}, {:?})",
                       n,
                       buf.len())
            },
            ParsedPayloadLength::Complated(len) => {
                write!(f,
                       "ParsedPayloadLength::Complated({:?})",
                       len)
            },
        }
    }
}

impl ParsedPayloadLength {
    /// 判断是否已分析了部分，并返回至少还需要多少个字节才能完成分析
    pub fn is_partial(&self) -> Option<usize> {
        match self {
            ParsedPayloadLength::Wait => {
                //还未分析过负载长度
                None
            },
            ParsedPayloadLength::Partial(n, _queue) => {
                //还需要等待指定长度的字节可以完成负载长度的分析
                Some(*n)
            },
            ParsedPayloadLength::Complated(_) => {
                //已完成负载长度的分析
                Some(0)
            },
        }
    }

    /// 获取分析完成的负载大小
    pub fn complated_len(&self) -> usize {
        if let ParsedPayloadLength::Complated(len) = self {
            *len
        } else {
            0
        }
    }
}

///
/// 分析P2P消息帧结果
///
pub enum ParseFrameResult {
    Partial,    //已分析了部分
    NextFrame,  //已分析完成，还有未分析的数据
    Complated,  //已分析完成，没有未分析的数据
}

///
/// P2P握手消息
///
pub struct P2PHandShakeInfo {
    info_type:  P2PHandShakeType,   //P2P握手消息类型
    host_id:    GossipNodeID,       //握手消息发送者的主机唯一id
}

impl<B: Buf> From<B> for P2PHandShakeInfo {
    fn from(mut value: B) -> Self {
        let val = value.get_u8();
        let info_type = P2PHandShakeType::from(val);

        let len = value.get_u16_le();
        let host_id = if len == 0 {
            match GossipNodeID::try_from("".to_string()) {
                Err(e) => {
                    panic!("From bytes to P2PHandShakeInfo failed, reason: {:?}", e);
                },
                Ok(id) => id,
            }
        } else {
            let bytes = value.copy_to_bytes(len as usize);
            let str = String::from_utf8_lossy(bytes.as_ref());
            match GossipNodeID::<32>::try_from(str.to_string()) {
                Err(e) => {
                    panic!("From bytes to P2PHandShakeInfo failed, reason: {:?}", e);
                },
                Ok(id) => id,
            }
        };

        P2PHandShakeInfo {
            info_type,
            host_id,
        }
    }
}

impl From<P2PHandShakeInfo> for Vec<u8> {
    fn from(value: P2PHandShakeInfo) -> Self {
        let mut buf = Vec::new();
        buf.put_u8(value.info_type.into());

        let str = value.host_id.to_string();
        let bytes = str.as_bytes();
        buf.put_u16_le(bytes.len() as u16);

        buf.put_slice(bytes);

        buf
    }
}

impl From<P2PHandShakeInfo> for BytesMut {
    fn from(value: P2PHandShakeInfo) -> Self {
        let mut buf = BytesMut::new();
        buf.put_u8(value.info_type.into());

        let str = value.host_id.to_string();
        let bytes = str.as_bytes();
        buf.put_u16_le(bytes.len() as u16);

        buf.put_slice(bytes);

        buf
    }
}

impl P2PHandShakeInfo {
    /// 构建一个P2P握手请求消息
    pub fn new_req(local_host_id: GossipNodeID) -> Self {
        P2PHandShakeInfo {
            info_type: P2PHandShakeType::Req,
            host_id: local_host_id,
        }
    }

    /// 构建一个P2P握手回应消息
    pub fn new_ack(local_host_id: GossipNodeID) -> Self {
        P2PHandShakeInfo {
            info_type: P2PHandShakeType::Ack,
            host_id: local_host_id,
        }
    }

    /// 判断是否是P2P握手请求消息
    pub fn is_req(&self) -> bool {
        if let &P2PHandShakeType::Req = &self.info_type {
            true
        } else {
            false
        }
    }

    /// 判断是否是P2P握手回应消息
    pub fn is_ack(&self) -> bool {
        if let &P2PHandShakeType::Ack = &self.info_type {
            true
        } else {
            false
        }
    }

    /// 判断是否是无效的P2P握手消息
    pub fn is_invalid(&self) -> bool {
        if let &P2PHandShakeType::Invalid = &self.info_type {
            true
        } else {
            false
        }
    }

    /// 获取当前P2P握手消息中的主机唯一id
    pub fn host_id(&self) -> &GossipNodeID {
        &self.host_id
    }
}

///
/// P2P握手消息类型
///
#[derive(Debug, Clone)]
pub enum P2PHandShakeType {
    Req = 0x1,      //P2P握手请求
    Ack,            //P2P握手回应
    Invalid = 0xff, //无效的消息类型
}

impl From<u8> for P2PHandShakeType {
    fn from(value: u8) -> Self {
        match value {
            0x1 => P2PHandShakeType::Req,
            0x2 => P2PHandShakeType::Ack,
            _ => P2PHandShakeType::Invalid,
        }
    }
}

impl From<P2PHandShakeType> for u8 {
    fn from(value: P2PHandShakeType) -> Self {
        match value {
            P2PHandShakeType::Req => 0x1,
            P2PHandShakeType::Ack => 0x2,
            P2PHandShakeType::Invalid => 0xff,
        }
    }
}

///
/// P2P主机心跳信息
///
pub struct P2PHeartBeatInfo {
    heartbeat:  u64,    //主机当前心跳计数
}

impl<B: Buf> From<B> for P2PHeartBeatInfo {
    fn from(mut value: B) -> Self {
        let heartbeat = value.get_u64_le();

        P2PHeartBeatInfo {
            heartbeat,
        }
    }
}

impl From<P2PHeartBeatInfo> for Vec<u8> {
    fn from(value: P2PHeartBeatInfo) -> Self {
        let mut buf = Vec::new();

        buf.put_u64_le(value.heartbeat);

        buf
    }
}

impl From<P2PHeartBeatInfo> for BytesMut {
    fn from(value: P2PHeartBeatInfo) -> Self {
        let mut buf = BytesMut::new();

        buf.put_u64_le(value.heartbeat);

        buf
    }
}

impl P2PHeartBeatInfo {
    /// 构建一个P2P主机心跳信息
    pub fn new(heartbeat: u64) -> Self {
        P2PHeartBeatInfo {
            heartbeat,
        }
    }

    /// 获取P2P主机当前心跳计数
    pub fn heartbeat(&self) -> u64 {
        self.heartbeat
    }
}

///
/// P2P服务读信息
///
pub struct P2PServiceReadInfo {
    pub port:       u16,    //P2P服务端口
    pub payload:    Bytes,  //服务负载
}

impl<B: Buf> From<B> for P2PServiceReadInfo {
    fn from(mut value: B) -> Self {
        let port = value.get_u16_le();
        let payload = value.copy_to_bytes(value.remaining());

        P2PServiceReadInfo {
            port,
            payload,
        }
    }
}

///
/// P2P服务写信息
///
pub struct P2PServiceWriteInfo {
    pub port:       u16,        //P2P服务端口
    pub payload:    BytesMut,   //服务负载
}

impl From<P2PServiceWriteInfo> for Vec<u8> {
    fn from(value: P2PServiceWriteInfo) -> Self {
        let mut buf = Vec::new();

        buf.put_u16_le(value.port);
        buf.put(value.payload);

        buf
    }
}

impl From<P2PServiceWriteInfo> for BytesMut {
    fn from(value: P2PServiceWriteInfo) -> Self {
        let mut buf = BytesMut::new();

        buf.put_u16_le(value.port);
        buf.put(value.payload);

        buf
    }
}