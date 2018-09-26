extern crate redis;
extern crate tempfile;

mod daemons;

fn main() {
    println!("This binary does nothing.  Run `cargo test` on this to actually run the tests.")
}

#[cfg(test)]
mod redis_tests {
    use redis::Client as RedisClient;
    use redis::Commands;
    use daemons::get_redis_daemons;

    #[test]
    fn test_set_get() {
        let (sd, _rd1, _rd2) = get_redis_daemons();

        // A simple set and then get.
        let client = RedisClient::open(sd.get_conn_str()).unwrap();
        let conn = client.get_connection().unwrap();
        let _: () = conn.set("my_key", 42).unwrap();
        let value: isize = conn.get("my_key").unwrap();
        assert_eq!(value, 42);
    }

    #[test]
    fn test_mget() {
        let (sd, _rd1, _rd2) = get_redis_daemons();

        // A simple set and then get.
        let client = RedisClient::open(sd.get_conn_str()).unwrap();
        let conn = client.get_connection().unwrap();
        let _: () = conn.set("key_one", 42).unwrap();
        let _: () = conn.set("key_two", 43).unwrap();
        let _: () = conn.set("key_three", 44).unwrap();
        let value: Vec<isize> = conn.get(&["key_one", "key_two", "key_three"]).unwrap();
        assert_eq!(value, vec![42, 43, 44]);
    }

    #[test]
    fn test_null_key() {
        let (sd, _rd1, _rd2) = get_redis_daemons();

        // A simple set and then get.
        let client = RedisClient::open(sd.get_conn_str()).unwrap();
        let conn = client.get_connection().unwrap();

        let _: () = conn.set("", 19).unwrap();
        let value: isize = conn.get("").unwrap();
        assert_eq!(value, 19);

        let _: () = conn.set("", "").unwrap();
        let value: String = conn.get("").unwrap();
        assert_eq!(value, "");

        let _: () = conn.set_multiple(&[("", "x"), ("d", "t")]).unwrap();
        let value: String = conn.get("").unwrap();
        assert_eq!(value, "x");
    }

    #[test]
    fn test_linsert() {
        let (sd, _rd1, _rd2) = get_redis_daemons();

        // A simple set and then get.
        let client = RedisClient::open(sd.get_conn_str()).unwrap();
        let conn = client.get_connection().unwrap();

        let _: () = conn.rpush("mylist", "Hello").unwrap();
        let _: () = conn.rpush("mylist", "World").unwrap();
        let _: () = conn.linsert_before("mylist", "World", "There").unwrap();
        let value: Vec<String> = conn.lrange("mylist", 0, -1).unwrap();
        assert_eq!(value, ["Hello", "There", "World"]);
    }

    #[test]
    fn test_quit_drops_conn() {
        let (sd, _rd1, _rd2) = get_redis_daemons();

        let client = RedisClient::open(sd.get_conn_str()).unwrap();
        let conn = client.get_connection().unwrap();

        let _: () = conn.set("", 19).unwrap();
        let value: isize = conn.get("").unwrap();
        assert_eq!(value, 19);

        // Now end the connection with QUIT.
        let _ = conn.send_packed_command(b"quit\r\n").unwrap();
        let _ = conn.recv_response().unwrap();

        // We still have our sending side open, so we can send the PING command, but trying to
        // receive the command should fail.
        let _ = conn.send_packed_command(b"ping\r\n").unwrap();
        match conn.recv_response().err() {
            Some(e) => assert!(e.is_connection_dropped()),
            None => panic!("call after quit should yield error"),
        }
    }
}
