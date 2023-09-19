extern crate netlink_rs;
extern crate bincode;
extern crate rustc_serialize;
#[macro_use]
extern crate clap;
extern crate config;
extern crate rdkafka;
extern crate futures;
extern crate tokio;
extern crate get_if_addrs;
extern crate chrono;

use chrono::{DateTime, FixedOffset, Utc, NaiveDateTime};
use tokio::time;

use clap::{Arg, App, ArgMatches, AppSettings};
use rustc_serialize::json::{as_pretty_json};
use std::os::raw::{c_uint, c_ushort};
use netlink_rs::socket::{Socket, Payload, Msg, NlMsgHeader, NetlinkAddr};
use netlink_rs::Protocol;
use std::process;
use std::str;
use std::time::Duration;
use std::net::Ipv4Addr;
use futures::stream::StreamExt;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use futures::FutureExt;
use config::{Config, File};

pub const AF_INET: u8 = 2;
pub const AF_INET6: u8 = 3;
pub const IPPROTO_TCP: u8 = 6;
pub const SOCK_DIAG_BY_FAMILY: u16 = 20;
pub const TCPF_ALL: u32 = 0xFFF;
pub const NETLINK_INET_DIAG: i32 = 4;

const SIZE: usize = 96;
const OFFSET: usize = 16;

pub struct SockAddrIn {
    family: u8,
    port: u8,
    addr: InAddr
}

pub struct InAddr {
    s_addr: u32
}

// #[derive(RustcEncodable)]
// #[derive(Debug)]
// pub struct TcpCon {
//     pub tcp_established: i64,
//     pub tcp_syn_sent: i64,
//     pub tcp_syn_recv: i64,
//     pub tcp_fin_wait1: i64,
//     pub tcp_fin_wait2: i64,
//     pub tcp_time_wait: i64,
//     pub tcp_close: i64,
//     pub tcp_close_wait: i64,
//     pub tcp_last_ack: i64,
//     pub tcp_listen: i64,
//     pub tcp_closing: i64,
//     pub tcp_max_states: i64
// }

// impl ::std::default::Default for TcpCon {
//     fn default() -> Self {
//         unsafe { ::std::mem::zeroed() }
//     }
// }


#[derive(RustcEncodable, RustcDecodable)]
#[derive(Debug)]
#[repr(C)]
pub struct NLMsgHeader {
    msg_length: u32,
    nl_type: u16,
    flags: u16,
    seq: u32,
    pid: u32,
}


#[derive(RustcEncodable, RustcDecodable)]
#[derive(Debug)]
#[repr(C)]
pub struct InetDiagSocketID {
    pub sport: c_ushort,
    pub dport: c_ushort,
    pub src: (c_uint, c_uint, c_uint, c_uint),
    pub dst: (c_uint, c_uint, c_uint, c_uint),
    pub if_: c_uint,
    pub cookie: (c_uint, c_uint),
}

impl ::std::default::Default for InetDiagSocketID {
    fn default() -> Self {
        InetDiagSocketID {
            sport: 0,
            dport: 0,
            src: (0, 0, 0, 0),
            dst: (0, 0, 0, 0),
            if_: 0,
            cookie: (0, 0)
        }
    }
}

#[derive(RustcEncodable, RustcDecodable)]
#[derive(Debug)]
#[repr(C)]
pub struct InetDiagV2 {
    pub family: u8,
    pub protocol: u8,
    pub states: u32,
    pub id: InetDiagSocketID,
    pub ext: u8,
    pub _pad: u8,
}

impl ::std::default::Default for InetDiagV2 {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[derive(RustcEncodable, RustcDecodable)]
#[derive(Debug)]
#[repr(C)]
pub struct InetDiagMsg {
    pub family: u8,
    pub state: u8,
    pub timer: u8,
    pub retrans: u8,

    pub id: InetDiagSocketID,

    pub expires: u32,
    pub rqueue: u32,
    pub wqueue: u32,
    pub uid: u32,
    pub inode: u32
}

impl ::std::default::Default for InetDiagMsg {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}


fn parsing(vec: Vec<Msg>, lan_ip: &String, datetime_format: &String) -> Vec<String> {
    let mut result = Vec::new();
    for reply in vec {
        match reply.bytes() {
            Ok(bytes) => {
                if bytes.len() != SIZE {
                    continue;
                }
                let data = &bytes[OFFSET..SIZE];
                match bincode::rustc_serialize::decode::<InetDiagMsg>(data) {
                    Ok(msg) => {
                        // let _src_format = Ipv4Addr::from(msg.id.src.0).to_string();
                        // let _src = if _src_format != "127.0.0.1" && _src_format != "0.0.0.0" {_src_format} else {lan_ip.to_string()};
                        if msg.state != 10 {
                            // 创建一个新的字符串并将其添加到结果向量中
                            let res_str = format!(
                                "{},{},{},{},{},{}",
                                datetime_format.to_string(),
                                Ipv4Addr::from(msg.id.src.0), 
                                msg.id.sport, 
                                Ipv4Addr::from(msg.id.dst.0), 
                                msg.id.dport, 
                                msg.retrans
                            );
                            result.push(res_str);
                        } else {
                            // 创建一个新的字符串并将其添加到结果向量中
                            let res_str = format!("{},{},{}t", datetime_format.to_string(), lan_ip.to_string(), msg.id.sport);
                            result.push(res_str);
                        }
                    },
                    Err(e) => eprintln!("An error occurred while decoding: {}", e),
                }
            },
            Err(e) => eprintln!("An error occurred while getting bytes: {}", e),
        }
    }
    result
}

#[tokio::main]
async fn main() {
    let matches = App::new(crate_name!()).setting(AppSettings::ArgRequiredElseHelp)
        .version(crate_version!())
        .author("amos <heyang.amos@gmail.com>")
        .about("parsing tool for tcp net")
        .arg(Arg::with_name("version")
            .short("V")
            .long("version")
            .help("print version info"))
        .arg(Arg::with_name("network")
            .required(true)
            .takes_value(true)
            .short("n")
            .long("network")
            .help("network card"))
        .arg(Arg::with_name("tracker")
            .short("t")
            .long("tracker")
            .multiple(true)
            .help("prints summary in json")).get_matches();

    let mut lan_ip = String::new();
    let _network_card = matches.value_of("network").unwrap();

    if matches.is_present("version") {
        println!("rnstat version {}", crate_version!())
    }

    if !matches.is_present("tracker") {
        println!("usage: -t or --tracker");
        std::process::exit(0);
    }
    
    match get_if_addrs::get_if_addrs() {
        Ok(if_addrs) => {
            for if_addr in if_addrs {
                if if_addr.name == _network_card { // 只打印名为 "eth0" 的网卡的 IP
                    lan_ip = if_addr.ip().to_string();
                    break;
                }
            }
        }
        Err(err) => {
            eprintln!("error: {}", err);
        }
    }

    let mut settings = Config::default();
    settings
        .merge(File::with_name("Settings.toml"))
        .expect("Unable to load config file");

    let bootstrap_servers = settings
        .get_str("kafka_producer.bootstrap_servers")
        .unwrap();
    let message_timeout_ms = settings
        .get_str("kafka_producer.message_timeout_ms")
        .unwrap();
    let listen_topic = settings
        .get_str("kafka_producer.listen_topic")
        .unwrap();
    let flow_topic = settings
        .get_str("kafka_producer.flow_topic")
        .unwrap();
    let timeout_sec =  settings
        .get("kafka_producer.timeout_sec")
        .unwrap();

    let interval_sec =  settings
        .get("base.interval_sec")
        .unwrap();

    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("message.timeout.ms", &message_timeout_ms)
        .create()
        .expect("Producer creation error");

    let mut interval = time::interval(Duration::from_secs(interval_sec));

    loop {
        interval.tick().await;

        let timestamp = Utc::now().timestamp();
        let naive = NaiveDateTime::from_timestamp(timestamp, 0);
        let datetime: DateTime<FixedOffset> = DateTime::from_utc(naive, FixedOffset::east(8 * 3600));
        let datetime_format = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
        println!("{} netlink package send to kernel.", datetime_format);

        let mut nl_sock = Socket::new(Protocol::INETDiag).unwrap();
        let nl_sock_addr = NetlinkAddr::new(0, 0);
        let payload = InetDiagV2 { family: AF_INET, protocol: IPPROTO_TCP, states: TCPF_ALL, id: InetDiagSocketID { ..Default::default() }, ..Default::default() };
        let gen_bytes = bincode::rustc_serialize::encode(&payload, bincode::SizeLimit::Bounded(56)).unwrap();
        let mut shdr = NlMsgHeader::user_defined(20, 56);
        shdr.data_length(56).seq(178431).pid(0).dump();
        let msg = Msg::new(shdr, Payload::Data(&gen_bytes));
        // let mut contkr = TcpCon { ..Default::default() };
        let _ = nl_sock.send(msg, &nl_sock_addr);

        let timeout = Duration::from_secs(timeout_sec);
        // todo: buffer and batch send
        loop {
            let result = nl_sock.recv();
            match result {
                Ok((addr, vec)) => {
                    if vec.len() != 0 {
                        let messages = parsing(vec, &lan_ip, &datetime_format);
                        let futures: Vec<_> = messages.iter().map(|message| async {
                            let topic = if message.ends_with("t") {&listen_topic } else {&flow_topic};
                            let payload = if message.ends_with("t") {(&message[..message.len() -1])} else {message.as_str()};
                            let delivery_status = producer.send(
                                FutureRecord::to(topic)
                                    .key("")
                                    .payload(payload),
                                timeout,
                            ).await;
                        }).collect();

                        let message_len = futures.len();
                        for future in futures {
                            future.await
                        }
                        println!("send {} message to kafka", message_len)
                    } else {
                        break
                    }
                },
                Err(error) => {
                    // error handling
                    eprintln!("Error: {}", error);
                    break;
                },
            }
        }
    }
}