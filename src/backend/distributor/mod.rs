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
mod modulo;
mod random;
pub use self::{modulo::ModuloDistributor, random::RandomDistributor};
use errors::CreationError;

/// A placeholder for backends.  This lets us avoid holding references to the actual backends.
pub struct BackendDescriptor {
    pub idx: usize,
    pub identifier: String,
    pub healthy: bool,
}

/// Distributes items amongst a set of backends.
pub trait Distributor {
    fn update(&mut self, backends: Vec<BackendDescriptor>);

    /// Chooses a backend based on the given point.
    fn choose(&self, point: u64) -> usize;
}

pub fn configure_distributor(dist_type: &str) -> Result<Box<Distributor + Send + Sync>, CreationError> {
    match dist_type {
        "random" => Ok(Box::new(RandomDistributor::new())),
        "modulo" => Ok(Box::new(ModuloDistributor::new())),
        s => {
            Err(CreationError::InvalidResource(format!(
                "unknown distributor type {}",
                s
            )))
        },
    }
}
