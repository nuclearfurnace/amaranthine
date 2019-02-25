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
use std::{
    iter::{Flatten, Iterator},
    vec::IntoIter as VecIntoIter,
};

type KeyValuePair<T> = (usize, Vec<T>);
type FlattenedIterator<T> = Flatten<VecIntoIter<Vec<T>>>;

pub struct IntegerMappedVec<T> {
    items: Vec<Vec<KeyValuePair<T>>>,
    size: u32,
    mask: usize,
    count: usize,
}

impl<V> IntegerMappedVec<V> {
    pub fn new() -> Self {
        let mut vec = IntegerMappedVec {
            items: Vec::new(),
            size: 0,
            count: 0,
            mask: 0,
        };
        vec.expand();
        vec
    }

    pub fn push(&mut self, key: usize, value: V) {
        let idx = self.calculate_index(key);
        let vals = &mut self.items[idx];
        match vals.iter().position(|x| x.0 == key) {
            Some(vidx) => {
                let ivals = &mut vals[vidx].1;
                ivals.push(value);
            },
            None => {
                let mut ivals = Vec::new();
                ivals.push(value);
                vals.push((key, ivals));
                self.count += 1;
            },
        }

        if (self.count & 4) == 4 {
            self.ensure_load_rate();
        }
    }

    #[inline]
    fn calculate_index(&self, key: usize) -> usize {
        let a = 11_400_714_819_323_198_549usize;
        let hash = a.wrapping_mul(key);
        (hash & self.mask) as usize
    }

    #[inline]
    fn limit(&self) -> usize { 2usize.pow(self.size) as usize }

    fn expand(&mut self) {
        self.size += 1;
        let new_limit = self.limit();
        self.mask = (new_limit as usize) - 1;

        let mut vec = Vec::new();
        vec.append(&mut self.items);

        for _ in 0..new_limit {
            // pretty sure we can optimize this to allocate the minimum required up front
            self.items.push(Vec::with_capacity(0));
        }

        vec.drain(0..).for_each(|mut values| {
            values.drain(0..).for_each(|kv| {
                let idx = self.calculate_index(kv.0);
                let vals = &mut self.items[idx];
                vals.push(kv);
            });
        });
    }

    fn ensure_load_rate(&mut self) {
        while ((self.count * 100) / self.items.len()) > 70 {
            self.expand();
        }
    }
}

pub struct IntoIter<T> {
    inner: FlattenedIterator<KeyValuePair<T>>,
}

impl<T> IntoIter<T> {
    pub fn new(items: Vec<Vec<KeyValuePair<T>>>) -> IntoIter<T> {
        IntoIter {
            inner: items.into_iter().flatten(),
        }
    }
}

impl<T> Iterator for IntoIter<T> {
    type Item = (usize, Vec<T>);

    #[inline]
    fn next(&mut self) -> Option<(usize, Vec<T>)> { self.inner.next() }
}

impl<T> IntoIterator for IntegerMappedVec<T> {
    type IntoIter = IntoIter<T>;
    type Item = (usize, Vec<T>);

    fn into_iter(self) -> Self::IntoIter { IntoIter::new(self.items) }
}
