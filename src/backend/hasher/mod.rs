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
mod fnv64a;
mod md5;
pub use self::{fnv64a::Fnv64aHasher, md5::MD5Hasher};
use errors::CreationError;

/// Basic hashing capabilities.
///
/// The hash output is a 64-bit integer so that it can be used with mapping hashed keys to specific
/// backend servers by index.
pub trait KeyHasher {
    fn hash(&self, buf: &[u8]) -> u64;
}

pub fn configure_hasher(hash_type: &str) -> Result<Box<KeyHasher + Send + Sync>, CreationError> {
    match hash_type {
        "md5" => Ok(Box::new(MD5Hasher::new())),
        "fnv1a_64" => Ok(Box::new(Fnv64aHasher::new())),
        s => Err(CreationError::InvalidResource(format!("unknown hash type {}", s))),
    }
}
