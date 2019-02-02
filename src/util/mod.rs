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
use futures::{future::Future, stream::Stream};

mod batch;
pub use self::batch::Batch;

mod helpers;
pub use self::helpers::ProcessFuture;

mod container;
pub use self::container::IntegerMappedVec;

impl<T: ?Sized> StreamExt for T where T: Stream {}

/// An extension trait for `Stream`s that provides necessary combinators specific to synchrotron.
pub trait StreamExt: Stream {
    /// Converts this stream into a batched stream.
    ///
    /// Items from the underlying stream will be batched, up to `capacity`, and returned as a
    /// `Vec<T>`.
    ///
    /// Unlike standard combinators that perform grouping, batches are collected opportunistically.
    /// The combinator will take up to `capacity` items from the underlying stream in a single
    /// `poll`, but will return the currently batched items -- if any have been batched since the
    /// last batch was emitted -- either when capacity is reached or the underlying stream reports
    /// that it is no longer ready.
    ///
    /// If the underlying stream signals that it is not ready, and no items have been batched, then
    /// the stream will emit nothing.
    fn batch(self, capacity: usize) -> batch::Batch<Self>
    where
        Self: Sized,
        Self::Item: Sizable,
    {
        batch::Batch::new(self, capacity)
    }
}

pub fn typeless<F>(f: F) -> impl Future<Item = (), Error = ()>
where
    F: Future,
{
    f.map(|_| ()).map_err(|_| ())
}

pub trait Sizable {
    fn size(&self) -> usize;
}
