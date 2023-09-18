extern crate netlink_rs;
extern crate bincode;
extern crate rustc_serialize;
#[macro_use]
extern crate clap;

use clap::{Arg, App, ArgMatches, AppSettings};
use rustc_serialize::json::{as_pretty_json};
use netlink_rs::socket::{Socket, Payload, Msg, NlMsgHeader, NetlinkAddr};
use netlink_rs::Protocol;
use std::process;
use std::str;
use std::net::Ipv4Addr;

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
    pub sport: u16,
    pub dport: u16,
    pub src: (u32, u32, u32, u32),
    pub dst: (u32, u32, u32, u32),
    pub if_: u32,
    pub cookie: (u32, u32),
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
    pub iode: u32
}

impl ::std::default::Default for InetDiagMsg {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

fn parsing(vec: Vec<Msg>) {
    for reply in vec {
        match reply.bytes() {
            Ok(bytes) => {
                if bytes.len() != SIZE {
                    continue;
                }
                let data = &bytes[OFFSET..SIZE];
                match bincode::rustc_serialize::decode::<InetDiagMsg>(data) {
                    Ok(msg) => {
                        println!("{}:{},{}:{}", Ipv4Addr::from(msg.id.src.0), msg.id.sport, Ipv4Addr::from(msg.id.dst.0), msg.id.dport);
                    },
                    Err(e) => eprintln!("An error occurred while decoding: {}", e),
                }
            },
            Err(e) => eprintln!("An error occurred while getting bytes: {}", e),
        }
    }
    if matches.is_present("states") {
        println!("{}", as_pretty_json(contkr))
    }
}

fn main() {
    let matches = App::new(crate_name!()).setting(AppSettings::ArgRequiredElseHelp)
        .version(crate_version!())
        .author("amos <heyang.amos@gmail.com>")
        .about("parsing tool for tcp net")
        .arg(Arg::with_name("version")
            .short("V")
            .long("version")
            .help("print version info"))
        .arg(Arg::with_name("tracker")
            .short("t")
            .long("tracker")
            .multiple(true)
            .help("prints summary in json")).get_matches();

    if matches.is_present("version") {
        println!("rnstat version {}", crate_version!())
    }

    if !matches.is_present("tracker") {
        println!("usage: -t or --tracker");
        std::process::exit(0);
    }
    
    let mut nl_sock = Socket::new(Protocol::INETDiag).unwrap();
    let nl_sock_addr = NetlinkAddr::new(0, 0);
    let payload = InetDiagV2 { family: AF_INET, protocol: IPPROTO_TCP, states: TCPF_ALL, id: InetDiagSocketID { ..Default::default() }, ..Default::default() };
    let gen_bytes = bincode::rustc_serialize::encode(&payload, bincode::SizeLimit::Bounded(56)).unwrap();
    let mut shdr = NlMsgHeader::user_defined(20, 56);
    shdr.data_length(56).seq(178431).pid(0).dump();
    let msg = Msg::new(shdr, Payload::Data(&gen_bytes));
    // let mut contkr = TcpCon { ..Default::default() };
    nl_sock.send(msg, &nl_sock_addr);
    
    let mut settings = Config::default();
    settings
        .merge(File::with_name("Settings"))
        .expect("Unable to load config file");

    let bootstrap_servers = settings
        .get_str("kafka_producer.bootstrap_servers")
        .unwrap();
    let message_timeout_ms = settings
        .get_str("kafka_producer.message_timeout_ms")
        .unwrap();
    let topic = settings
        .get_str("kafka_producer.topic")
        .unwrap();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("message.timeout.ms", &message_timeout_ms)
        .create()
        .expect("Producer creation error");

    loop {
        let result = nl_sock.recv();
        match result {
            Ok((addr, vec)) => {
                if vec.len() != 0 {
                    parsing(vec);
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

        // todo: buffer and batch send
        let record = FutureRecord::to(&topic)
        .payload("Hello, Kafka!");
        match producer.send(record, 0).await {
            Ok(delivery) => {
                println!("Message delivered to {:?}", delivery);
            }
            Err((error, _)) => {
                println!("Failed to deliver message: {}", error);
            }
        }
    }
}