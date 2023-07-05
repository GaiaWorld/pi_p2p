use std::fs::File;
use std::path::Path;
use std::time::SystemTime;
use std::io::{BufRead, BufReader, Result as IOResult, Error, ErrorKind};

use rustls;
use rustls_pemfile::certs;
use ed25519_dalek::PUBLIC_KEY_LENGTH;
use x509_parser::{pem::Pem,
                  public_key::PublicKey};
use hex::encode;
use hex::decode_to_slice;
use log::error;

use pi_gossip::GossipNodeID;

// 默认的种子主机DNS名前缀
const DEFAULT_SEED_HOST_DNS_PREFIX: &str = "seed.";

// 默认的主机DNS名后缀
const DEFAULT_HOST_DNS_SUFFIX: &str = ".dns";

///
/// 用于服务端的客户端证书验证器
/// 可以用于限制对端主机是否允许连接本地主机
///
pub struct ClientCertVerifier(Pem);

impl rustls::server::ClientCertVerifier for ClientCertVerifier {
    fn client_auth_root_subjects(&self) -> Option<rustls::DistinguishedNames> {
        None
    }

    fn verify_client_cert(&self,
                          end_entity: &rustls::Certificate,
                          _intermediates: &[rustls::Certificate],
                          _now: SystemTime) -> Result<rustls::server::ClientCertVerified, rustls::Error> {
        let mut pem = Pem {
            label: String::new(),
            contents: end_entity.0.clone(),
        };

        match pem.parse_x509() {
            Err(e) => {
                //从PEM数据中解析证书失败，则立即返回验证错误
                error!("Verify client cert failed, bin: {:?}, reason: {:?}",
                    end_entity.0,
                    e);
                Err(rustls::Error::InvalidCertificateData(format!("{:?}", e)))
            },
            Ok(client_cert) => {
                //从PEM数据中解析证书成功
                match self.0.parse_x509() {
                    Err(e) => {
                        //从PEM数据中解析根证书失败，则立即返回验证错误
                        error!("Verify client cert failed, reason: {:?}", e);
                        Err(rustls::Error::InvalidCertificateData(format!("{:?}", e)))
                    },
                    Ok(ca_cert) => {
                        if let Err(e) = client_cert.verify_signature(Some(ca_cert.public_key())) {
                            //客户端证书不是由指定的CA证书颁发，则立即返回验证错误
                            error!("Verify client cert failed, bin: {:?}, reason: {:?}",
                                end_entity.0,
                                e);
                            Err(rustls::Error::InvalidCertificateData(format!("{:?}", e)))
                        } else {
                            //客户端证书是由指定的CA证书颁发
                            Ok(rustls::server::ClientCertVerified::assertion())
                        }
                    },
                }
            }
        }
    }
}

impl ClientCertVerifier {
    /// 使用指定的CA证书构建用于服务端的客户端证书验证器
    pub fn new<P: AsRef<Path>>(ca_cert_path: P) -> Result<Self, Error> {
        Self::with_buf(BufReader::new(File::open(ca_cert_path)?))
    }

    /// 使用指定的CA证书数据构建用于服务端的客户端证书验证器
    pub fn with_buf<B: BufRead>(mut buf: B) -> Result<Self, Error> {
        let mut bytes_vec = certs(&mut buf)?;

        let pem = Pem {
            label: String::new(),
            contents: bytes_vec.remove(0),
        };

        Ok(ClientCertVerifier(pem))
    }
}

///
/// 用于客户端的服务器证书验证器
/// 用于验证对端主机是否是指定的主机
///
pub struct ServerCertVerifier(Pem);

impl rustls::client::ServerCertVerifier for ServerCertVerifier {
    fn verify_server_cert(&self,
                          end_entity: &rustls::Certificate,
                          _intermediates: &[rustls::Certificate],
                          server_name: &rustls::ServerName,
                          scts: &mut dyn Iterator<Item=&[u8]>,
                          _ocsp_response: &[u8],
                          now: SystemTime) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        let mut pem = Pem {
            label: String::new(),
            contents: end_entity.0.clone(),
        };

        match pem.parse_x509() {
            Err(e) => {
                //从PEM数据中解析证书失败，则立即返回验证错误
                error!("Verify server cert failed, bin: {:?}, reason: {:?}",
                    end_entity.0,
                    e);
                Err(rustls::Error::InvalidCertificateData(format!("{:?}", e)))
            },
            Ok(server_cert) => {
                //从PEM数据中解析证书成功
                match server_cert.public_key().parsed() {
                    Err(e) => {
                        //从证书中解析公钥失败，则立即返回验证错误
                        error!("Verify server cert failed, bin: {:?}, reason: {:?}",
                            end_entity.0,
                            e);
                        Err(rustls::Error::InvalidCertificateData(format!("{:?}", e)))
                    },
                    Ok(server_public_key) => {
                        //从证书中解析公钥成功
                        if let PublicKey::Unknown(bin) = server_public_key {
                            match server_name {
                                rustls::ServerName::IpAddress(_) => {
                                    //服务器名称是ip，则立即返回验证错误
                                    error!("Verify server cert failed, server_name: {:?}, reason: require dns name",
                                        server_name);
                                    Err(rustls::Error::InvalidCertificateData(format!("require dns name")))
                                },
                                rustls::ServerName::DnsName(name) => {
                                    if name.as_ref().starts_with(DEFAULT_SEED_HOST_DNS_PREFIX) {
                                        //是种子主机服务端，则需要验证种子主机服务端的合法性
                                        match self.0.parse_x509() {
                                            Err(e) => {
                                                //从PEM数据中解析根证书失败，则立即返回验证错误
                                                error!("Verify seed server cert failed, reason: {:?}", e);
                                                return Err(rustls::Error::InvalidCertificateData(format!("{:?}", e)));
                                            },
                                            Ok(ca_cert) => {
                                                if let Err(e) = server_cert.verify_signature(Some(ca_cert.public_key())) {
                                                    //种子主机服务端证书不是由指定的CA证书颁发，则立即返回验证错误
                                                    error!("Verify seed server cert failed, bin: {:?}, reason: {:?}",
                                                        end_entity.0,
                                                        e);
                                                    return Err(rustls::Error::InvalidCertificateData(format!("{:?}", e)));
                                                }
                                            },
                                        }
                                    }

                                    match host_dns_name_to_bytes(name) {
                                        Err(e) => {
                                            //解析主机DNS名错误，则立即返回错误原因
                                            error!("Verify server cert failed, hex_vec: {:?}, reason: {:?}",
                                                server_name,
                                                e);
                                            Err(e)
                                        },
                                        Ok(buf) => {
                                            //解析主机DNS名成功
                                            if buf.as_slice() != bin {
                                                //公钥匹配失败，则立即返回验证错误
                                                error!("Verify server cert failed, x: {:?}, y: {:?}, reason: public key not match",
                                                    buf,
                                                    bin);
                                                Err(rustls::Error::InvalidCertificateData(format!("public key not match")))
                                            } else {
                                                //公钥匹配成功
                                                Ok(rustls::client::ServerCertVerified::assertion())
                                            }
                                        },
                                    }
                                },
                                _ => unimplemented!(),
                            }
                        } else {
                            //公钥算法不是ED25519，则立即返回验证错误
                            error!("Verify server cert failed, public_key: {:?}, reason: require ed25519",
                                server_public_key);
                            Err(rustls::Error::InvalidCertificateData(format!("require ed25519")))
                        }
                    },
                }
            },
        }
    }
}

impl ServerCertVerifier {
    /// 使用指定的种子主机的CA证书构建用于客户端的服务端证书验证器
    pub fn new<P: AsRef<Path>>(ca_cert_path: P) -> Result<Self, Error> {
        Self::with_buf(BufReader::new(File::open(ca_cert_path)?))
    }

    /// 使用指定的种子主机的CA证书数据构建用于客户端的服务端证书验证器
    pub fn with_buf<B: BufRead>(mut buf: B) -> Result<Self, Error> {
        let mut bytes_vec = certs(&mut buf)?;

        let pem = Pem {
            label: String::new(),
            contents: bytes_vec.remove(0),
        };

        Ok(ServerCertVerifier(pem))
    }
}

// 将主机唯一id转换为主机DNS名
pub(crate) fn host_id_to_host_dns(host_id: &GossipNodeID,
                                  is_seed: bool) -> IOResult<String> {
    let hex_vec = encode(host_id.get_hash()).into_bytes();
    let hex_vec_len = hex_vec.len();
    let mut hostname_vec = Vec::with_capacity(hex_vec.len() * 2);
    for (index, hex) in hex_vec.iter().enumerate() {
        hostname_vec.push(*hex);

        let n = index + 1;
        if n < hex_vec_len && n % 2 == 0 {
            hostname_vec.push(46);
        }
    }

    match String::from_utf8(hostname_vec) {
        Err(e) => {
            //获取对端主机的dns名失败，则立即返回错误原因
            Err(Error::new(ErrorKind::Other,
                           format!("From host id to host dns failed, host_id: {:?}, reason: {:?}",
                                   host_id,
                                   e)))
        },
        Ok(str) => {
            if is_seed {
                //是种子主机
                Ok(DEFAULT_SEED_HOST_DNS_PREFIX.to_string() + str.as_str() + DEFAULT_HOST_DNS_SUFFIX)
            } else {
                //非种子主机
                Ok(str + DEFAULT_HOST_DNS_SUFFIX)
            }
        },
    }
}

// 将主机DNS名转换为二进制
pub(crate) fn host_dns_name_to_bytes(name: &rustls::server::DnsName) -> Result<Vec<u8>, rustls::Error> {
    let hex_vec = name
        .as_ref()
        .strip_prefix(DEFAULT_SEED_HOST_DNS_PREFIX)
        .unwrap_or(name.as_ref())
        .strip_suffix(DEFAULT_HOST_DNS_SUFFIX)
        .unwrap_or(name.as_ref())
        .split(".")
        .collect::<Vec<&str>>();

    if hex_vec.len() != PUBLIC_KEY_LENGTH {
        //长度不对，则立即返回错误原因
        Err(rustls::Error::InvalidCertificateData(format!("invalid vec length")))
    } else {
        let hex_str = hex_vec.join("");
        let mut buf = Vec::with_capacity(PUBLIC_KEY_LENGTH);
        buf.resize(PUBLIC_KEY_LENGTH, 0);
        if let Err(e) = decode_to_slice(&hex_str, buf.as_mut_slice()) {
            //解码Hex字符串失败，则立即返回验证错误
            Err(rustls::Error::InvalidCertificateData(format!("{:?}", e)))
        } else {
            Ok(buf)
        }
    }
}