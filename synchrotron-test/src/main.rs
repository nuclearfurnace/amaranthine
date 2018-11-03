extern crate redis;
extern crate tempfile;

mod daemons;

fn main() {
    println!("This binary does nothing.  Run `cargo test` on this to actually run the tests.")
}

#[cfg(test)]
mod redis_tests {
    use std::thread;
    use std::time::Duration;
    use redis::Client as RedisClient;
    use redis::{Commands, RedisResult, ErrorKind as RedisErrorKind};
    use daemons::get_redis_daemons;

    #[test]
    fn test_set_get() {
        let (sd, _rd1, _rd2) = get_redis_daemons();

        // A simple set and then get.
        let client = RedisClient::open(sd.get_fixed_conn_str()).unwrap();
        let conn = client.get_connection().unwrap();
        let _: () = conn.set("my_key", 42).unwrap();
        let value: isize = conn.get("my_key").unwrap();
        assert_eq!(value, 42);
    }

    #[test]
    fn test_mget() {
        let (sd, _rd1, _rd2) = get_redis_daemons();

        // A simple set and then get.
        let client = RedisClient::open(sd.get_fixed_conn_str()).unwrap();
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
        let client = RedisClient::open(sd.get_fixed_conn_str()).unwrap();
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
        let client = RedisClient::open(sd.get_fixed_conn_str()).unwrap();
        let conn = client.get_connection().unwrap();

        let _: () = conn.rpush("mylist", "Hello").unwrap();
        let _: () = conn.rpush("mylist", "World").unwrap();
        let _: () = conn.linsert_before("mylist", "World", "There").unwrap();
        let value: Vec<String> = conn.lrange("mylist", 0, -1).unwrap();
        assert_eq!(value, ["Hello", "There", "World"]);
    }

    #[test]
    fn test_large_insert_times_out() {
        let (sd, _rd1, _rd2) = get_redis_daemons();

        let client = RedisClient::open(sd.get_fixed_conn_str()).unwrap();
        let conn = client.get_connection().unwrap();

        let mut hash_fields = Vec::new();
        for i in 0..500000 {
            let key = format!("k-{}", i);
            let value = format!("v-{}", i);

            hash_fields.push((key, value));
        }

        let result: RedisResult<()> = conn.hset_multiple("large-hash", &hash_fields);
        match result {
            Ok(_) => panic!("should have been error after request timing out"),
            Err(inner_err) => assert_eq!(inner_err.kind(), RedisErrorKind::ResponseError),
        }
    }

    #[test]
    fn test_quit_drops_conn() {
        let (sd, _rd1, _rd2) = get_redis_daemons();

        let client = RedisClient::open(sd.get_fixed_conn_str()).unwrap();
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
            Some(e) => {
                println!("quit conn error: {:?}", e);
                assert!(e.is_connection_dropped() || e.kind() == RedisErrorKind::ResponseError)
            },
            None => panic!("call after quit should yield error"),
        }
    }

    #[test]
    fn test_traffic_shadowing() {
        let (sd, rd1, rd2) = get_redis_daemons();

        let client = RedisClient::open(sd.get_shadow_conn_str()).unwrap();
        let conn = client.get_connection().unwrap();

        // Set values directly on both Redis servers so we can distinguish between nodes when
        // we eventually go through Synchrotron.
        let r1client = RedisClient::open(rd1.get_conn_str()).unwrap();
        let r1conn = r1client.get_connection().unwrap();

        let _: () = r1conn.set("two", 1).unwrap();
        let value1: isize = r1conn.get("two").unwrap();
        assert_eq!(value1, 1);

        let r2client = RedisClient::open(rd2.get_conn_str()).unwrap();
        let r2conn = r2client.get_connection().unwrap();

        let _: () = r2conn.set("two", 2).unwrap();
        let value2: isize = r2conn.get("two").unwrap();
        assert_eq!(value2, 2);

        // Now set the value through Synchrotron.
        let _: () = conn.set("two", 3).unwrap();

        // Wait for a hot second just to make sure the shadow pool is hit.
        thread::sleep(Duration::from_millis(50));

        // Both pools should have the same value now.
        let value3: isize = r1conn.get("two").unwrap();
        assert_eq!(value3, 3);

        let value4: isize = r2conn.get("two").unwrap();
        assert_eq!(value4, 3);

        // Do it through Synchrotron one more time to show the shadow backend isn't locked up or
        // anything.
        let _: () = conn.set("two", 4).unwrap();

        // Wait for a hot second just to make sure the shadow pool is hit.
        thread::sleep(Duration::from_millis(50));

        // Both pools should have the same value now.
        let value3: isize = r1conn.get("two").unwrap();
        assert_eq!(value3, 4);

        let value4: isize = r2conn.get("two").unwrap();
        assert_eq!(value4, 4);

    }

    #[test]
    fn test_backend_cooloff() {
        let (sd, rd1, rd2) = get_redis_daemons();

        let client = RedisClient::open(sd.get_fixed_conn_str()).unwrap();
        let conn = client.get_connection().unwrap();

        // Set values directly on both Redis servers so we can distinguish between nodes when
        // we eventually go through Synchrotron.
        let r1client = RedisClient::open(rd1.get_conn_str()).unwrap();
        let r1conn = r1client.get_connection().unwrap();

        let _: () = r1conn.set("two", 1).unwrap();
        let value1: isize = r1conn.get("two").unwrap();
        assert_eq!(value1, 1);

        let r2client = RedisClient::open(rd2.get_conn_str()).unwrap();
        let r2conn = r2client.get_connection().unwrap();

        let _: () = r2conn.set("two", 2).unwrap();
        let value2: isize = r2conn.get("two").unwrap();
        assert_eq!(value2, 2);

        // Now grab the value through Synchrotron so we have our baseline value.
        let baseline: isize = conn.get("two").unwrap();

        // Now kill whichever server was the one that the key routed to.
        if baseline == 1 {
            drop(rd1);
        } else {
            drop(rd2);
        }

        // Wait for a hot second just to make sure things are dead.  250ms should do it.
        thread::sleep(Duration::from_millis(250));

        // Now, try to ask Synchrotron for the value, five times.  Should be all errors.
        for _ in 0..5 {
            let iclient = RedisClient::open(sd.get_fixed_conn_str()).unwrap();
            let iconn = iclient.get_connection().unwrap();
            let result: RedisResult<isize> = iconn.get("two");
            match result {
                Ok(_) => panic!("should have been error after killing redis node"),
                Err(inner_err) => assert_eq!(inner_err.kind(), RedisErrorKind::ResponseError),
            }
        }

        // Next one should work as it switches over to the new server.
        let failover: isize = conn.get("two").unwrap();
        assert!(failover != baseline);

        // Now wait for the cooloff to expire.
        thread::sleep(Duration::from_millis(2500));

        // And make sure we get errors again.  We can't easily restart the killed Redis
        // daemon again but we'll know the cooloff happened if we get an error because
        // it means it tried the downed server again.
        let result: RedisResult<isize> = conn.get("two");
        assert!(result.is_err());
    }
}
