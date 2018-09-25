use std::str;
use std::fs::File;
use std::io::{Error, Write};
use std::process::{Command, Child, Stdio};
use tempfile::{Builder, TempDir};
use std::sync::atomic::{ATOMIC_USIZE_INIT, AtomicUsize, Ordering};

static PORT_OFFSET: AtomicUsize = ATOMIC_USIZE_INIT;

macro_rules! get_redis_config {
    ($($arg:tt)*) => (format!(r#"
        {{
            "stats_port": {},
            "listeners": [
                {{
                    "protocol": "redis",
                    "address": "127.0.0.1:{}",
                    "pools": {{
                        "default": {{
                            "addresses": ["127.0.0.1:{}", "127.0.0.1:{}"]
                        }}
                    }},
                    "routing": {{
                        "type": "fixed"
                    }}
                }}
            ]
        }}
    "#, $($arg)*));

}

pub struct SynchrotronRunner {
    handle: Child,
    port: u16,
    conn_str: String,
    conf_dir: Option<TempDir>,
}

impl SynchrotronRunner {
    pub fn new_redis(listen_port: u16, stats_port: u16, redis1_port: u16, redis2_port: u16) -> Result<SynchrotronRunner, Error> {
        let full_config = get_redis_config!(stats_port, listen_port, redis1_port, redis2_port);

        // Create our configuration file from the data we got.
        let conf_dir = Builder::new()
            .prefix("synchrotron-test-")
            .tempdir()?;

        let file_path = conf_dir.path().join("synchrotron");
        let file_path_w_ext = conf_dir.path().join("synchrotron.json");
        let mut conf_file = File::create(file_path_w_ext)?;
        conf_file.write(full_config.as_bytes())?;

        // Now try and launch Synchrotron.
        let handle = Command::new("../target/debug/synchrotron")
            .env("SYNC_CONFIG", file_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        wait_until(|| check_synchrotron(listen_port));

        Ok(SynchrotronRunner {
            handle: handle,
            port: listen_port,
            conn_str: format!("redis://127.0.0.1:{}", listen_port),
            conf_dir: Some(conf_dir),
        })
    }

    pub fn get_conn_str(&self) -> &str {
        self.conn_str.as_str()
    }
}

impl Drop for SynchrotronRunner {
    fn drop(&mut self) {
        // If it panics, it panics. ¯\_(ツ)_/¯
        self.handle.kill().unwrap();
        self.conf_dir.take().unwrap().close().unwrap();

        println!("Synchrotron ({}) killed!", self.port);
    }
}

pub struct RedisRunner {
    handle: Child,
    port: u16,
    conn_str: String,
}

impl RedisRunner {
    pub fn new(port: u16) -> Result<RedisRunner, Error> {
        // Launch Redis on the specified port.
        let handle = Command::new("/usr/local/bin/redis-server")
            .arg("--port")
            .arg(port.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        // Wait for the instance to be ready.
        wait_until(|| check_redis(port));

        Ok(RedisRunner {
            handle: handle,
            port: port,
            conn_str: format!("redis://127.0.0.1:{}", port),
        })
    }

    pub fn get_conn_str(&self) -> &str {
        self.conn_str.as_str()
    }
}

impl Drop for RedisRunner {
    fn drop(&mut self) {
        // If it panics, it panics. ¯\_(ツ)_/¯
        self.handle.kill().unwrap();

        println!("redis-server ({}) killed!", self.port);
    }
}

fn wait_until<F>(f: F)
    where F: Fn() -> bool
{
    let mut sleep_ms = 50;

    loop {
        let status = f();
        if status {
            return;
        }

        if sleep_ms < 5000 {
            sleep_ms *= 2;
        }
    }
}

fn check_redis(port: u16) -> bool {
    let result = Command::new("redis-cli")
        .args(&["-h", "localhost", "-p", port.to_string().as_str(), "ping"])
        .output()
        .expect("failed to run redis-cli");

    match str::from_utf8(&result.stdout) {
        Ok(output) => match output == "PONG\n" {
            true => {
                println!("redis-server ({}) is running!", port);
                true
            },
            false => {
                println!("redis-server ({}) not running yet.", port);
                false
            },
        },
        _ => {
            println!("redis-server ({}) not running yet.", port);
            false
        },
    }
}

fn check_synchrotron(port: u16) -> bool {
    let result = Command::new("redis-cli")
        .args(&["-h", "localhost", "-p", port.to_string().as_str(), "ping"])
        .output()
        .expect("failed to run redis-cli");

    match str::from_utf8(&result.stdout) {
        Ok(output) => match output == "PONG\n" {
            true => {
                println!("Synchrotron ({}) is running!", port);
                true
            },
            false => {
                println!("Synchrotron ({}) not running yet.", port);
                false
            },
        },
        _ => {
            println!("Synchrotron ({}) not running yet.", port);
            false
        },
    }
}

pub fn get_redis_daemons() -> (SynchrotronRunner, RedisRunner, RedisRunner) {
    let offset = PORT_OFFSET.fetch_add(1, Ordering::SeqCst) as u16;

    let synchrotron_port = 43000 + offset;
    let synchrotron_stats_port = 44000 + offset;
    let redis1_port = 45000 + offset;
    let redis2_port = 46000 + offset;

    let redis1 = RedisRunner::new(redis1_port).unwrap();
    let redis2 = RedisRunner::new(redis2_port).unwrap();
    let synchrotron = SynchrotronRunner::new_redis(synchrotron_port, synchrotron_stats_port, redis1_port, redis2_port).unwrap();

    (synchrotron, redis1, redis2)
}
