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
use serde::de::{Deserialize, Deserializer, Error};
use std::{fmt, net::SocketAddr};

#[derive(Debug, Clone)]
pub struct BackendAddress {
    pub address: SocketAddr,
    pub identifier: String,
}

impl fmt::Display for BackendAddress {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result { write!(f, "{}({})", self.address, self.identifier) }
}

impl<'de> Deserialize<'de> for BackendAddress {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let mut parts = s.split(" ");

        let address = parts
            .next()
            .ok_or(D::Error::custom("missing address"))?
            .parse::<SocketAddr>()
            .map_err(D::Error::custom)?;
        let identifier = parts
            .next()
            .map(|s| s.to_string())
            .unwrap_or_else(|| address.to_string().clone());

        if parts.next() != None {
            return Err(D::Error::custom("unexpected element"));
        }

        Ok(BackendAddress { address, identifier })
    }
}
