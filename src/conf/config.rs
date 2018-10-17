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
use config::{Config, ConfigError, File};
use std::{collections::HashMap, env, net::SocketAddr};

#[derive(Deserialize, Default, Clone, Debug)]
pub struct Configuration {
    pub stats_port: u16,
    pub logging: LoggingConfiguration,
    pub listeners: Vec<ListenerConfiguration>,
}

#[derive(Deserialize, Default, Clone, Debug)]
pub struct LoggingConfiguration {
    pub level: String,
}

#[derive(Deserialize, Default, Clone, Debug)]
pub struct ListenerConfiguration {
    pub protocol: String,
    pub address: String,
    pub reload_timeout_ms: Option<u64>,
    pub pools: HashMap<String, PoolConfiguration>,
    pub routing: HashMap<String, String>,
}

#[derive(Deserialize, Default, Clone, Debug)]
pub struct PoolConfiguration {
    pub addresses: Vec<SocketAddr>,
    pub options: Option<HashMap<String, String>>,
}

impl Configuration {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();

        // TODO: the hierarchy stuff doesn't work IIRC.  re-examine that and figure out if my
        // memory is just shit or if it actually doesn't work.

        // Set some defaults.
        s.set_default("logging.level", "info")?;
        // how tf do we make this work?
        // s.set_default("listeners", Vec::<ListenerConfiguration>::new())?;
        s.set_default("stats_port", 16161)?;

        // Now load in any configuration files we can find.
        s.merge(File::with_name("config/synchrotron").required(false))?;

        let env = env::var("ENV").unwrap_or_else(|_| "dev".into());
        s.merge(File::with_name(&format!("config/synchrotron.{}", env)).required(false))?;
        s.merge(File::with_name("config/synchrotron.local").required(false))?;

        let conf_override = env::var("SYNC_CONFIG").ok();
        if let Some(path) = conf_override {
            s.merge(File::with_name(path.as_str()).required(false))?;
        }

        s.try_into()
    }
}
