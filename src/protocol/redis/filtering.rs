// Copyright (c) 2018 Nuclear Furnace
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
use phf::phf_set;

static VALID_COMMANDS: phf::Set<&'static str> = phf_set! {
    "DEL",
    "DUMP",
    "EXISTS",
    "EXPIRE",
    "EXPIREAT",
    "PERSIST",
    "PEXPIRE",
    "PEXPIREAT",
    "PTTL",
    "RESTORE",
    "SORT",
    "TTL",
    "TYPE",
    "APPEND",
    "BITCOUNT",
    "BITPOS",
    "DECR",
    "DECRBY",
    "GET",
    "GETBIT",
    "GETRANGE",
    "GETSET",
    "INCR",
    "INCRBY",
    "INCRBYFLOAT",
    "MGET",
    "MSET",
    "PSETEX",
    "SET",
    "SETBIT",
    "SETEX",
    "SETNX",
    "SETRANGE",
    "STRLEN",
    "HDEL",
    "HEXISTS",
    "HGET",
    "HGETALL",
    "HINCRBY",
    "HINCRBYFLOAT",
    "HKEYS",
    "HLEN",
    "HMGET",
    "HMSET",
    "HSET",
    "HSETNX",
    "HVALS",
    "HSCAN",
    "LINDEX",
    "LINSERT",
    "LLEN",
    "LPOP",
    "LPUSH",
    "LPUSHX",
    "LRANGE",
    "LREM",
    "LSET",
    "LTRIM",
    "RPOP",
    "RPOPLPUSH",
    "RPUSH",
    "RPUSHX",
    "SADD",
    "SCARD",
    "SDIFF",
    "SDIFFSTORE",
    "SINTER",
    "SINTERSTORE",
    "SISMEMBER",
    "SMEMBERS",
    "SMOVE",
    "SPOP",
    "SRANDMEMBER",
    "SREM",
    "SUNION",
    "SUNIONSTORE",
    "SSCAN",
    "ZADD",
    "ZCARD",
    "ZCOUNT",
    "ZINCRBY",
    "ZINTERSTORE",
    "ZLEXCOUNT",
    "ZRANGE",
    "ZRANGEBYLEX",
    "ZRANGEBYSCORE",
    "ZRANK",
    "ZREM",
    "ZREMRANGEBYLEX",
    "ZREMRANGEBYRANK",
    "ZREMRANGEBYSCORE",
    "ZREVRANGE",
    "ZREVRANGEBYSCORE",
    "ZREVRANK",
    "ZSCORE",
    "ZUNIONSTORE",
    "ZSCAN",
    "PFADD",
    "PFCOUNT",
    "PFMERGE",
    "EVAL",
    "EVALSHA",
    "PING",
    "QUIT",
};

pub fn check_command_validity(cmd: &[u8]) -> bool {
    // This is goofy but redis only supports commands with ASCII characters, so we munge
    // these bytes to make sure that, if they were lowercase ASCII, they now become
    // uppercase ASCII... and we do it by hand instead of using str::to_uppercase because
    // this is 2x as fast. Really feels stupid to pay a constant perf penalty if we don't
    // really have to.  Could probably unroll this to work on 8-byte chunks, 4-byte chunks,
    // etc, but that'd require full on pointers and this is good enough for now, I think.
    let mut c = cmd.to_owned();
    let m = c.as_mut_slice();

    let count = m.len();
    let mut offset = 0;

    while offset < count {
        // This is just a neat invariant of ASCII where lower/uppercase letters are only
        // separated by 64 so we can mask it out to force uppercase.
        m[offset] = m[offset] & 0b11011111;
        offset += 1;
    }

    let as_str = unsafe { std::str::from_utf8_unchecked(m) };
    VALID_COMMANDS.contains(as_str)
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;

    #[test]
    fn ensure_valid_vs_invalid() {
        let valid_cmd_1 = "PFCOUNT";
        let valid_cmd_2 = "hmset";
        let invalid_cmd_1 = "INFO";
        let invalid_cmd_2 = "rename";

        assert!(check_command_validity(valid_cmd_1.as_bytes()));
        assert!(check_command_validity(valid_cmd_2.as_bytes()));
        assert!(!check_command_validity(invalid_cmd_1.as_bytes()));
        assert!(!check_command_validity(invalid_cmd_2.as_bytes()));
    }

    #[bench]
    fn bench_valid_lookup(b: &mut Bencher) {
        let valid_cmd = "PFCOUNT".as_bytes();
        b.iter(|| check_command_validity(valid_cmd));
    }

    #[bench]
    fn bench_invalid_lookup(b: &mut Bencher) {
        let invalid_cmd = "INFO".as_bytes();
        b.iter(|| check_command_validity(invalid_cmd));
    }
}
