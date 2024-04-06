/* * keyvi - A key value store.
 *
 * Copyright 2017   Narek Gharibyan <narekgharibyan@gmail.com>
 *                  Subu <subu@cliqz.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 *  dictionary.rs
 *
 *  Created on: September 4, 2017
 *  Author: Narek Gharibyan <narekgharibyan@gmail.com>
 *          Subu <subu@cliqz.com>
 */

use std::ffi::CString;
use std::io;

use bindings::*;
use keyvi_match::KeyviMatch;
use keyvi_match_iterator::KeyviMatchIterator;
use keyvi_string::KeyviString;

pub enum LoadingStrategyTypes {
    DEFAULT_OS,        // no special treatment, use whatever the OS/Boost has as default
    LAZY,              // load data as needed with some read-ahead
    POPULATE, // immediately load everything in memory (blocks until everything is fully read)
    POPULATE_KEY_PART, // populate only the key part, load value part lazy
    POPULATE_LAZY, // load data lazy but ask the OS to read ahead if possible (does not block)
    LAZY_NO_READAHEAD, // disable any read-ahead (for cases when index > x * main memory)
    LAZY_NO_READAHEAD_VALUE_PART, // disable read-ahead only for the value part
    POPULATE_KEY_PART_NO_READAHEAD_VALUE_PART, // populate the key part, but disable read ahead value part
}

pub struct Dictionary {
    dict: *mut root::keyvi_dictionary,
}

unsafe impl Send for Dictionary {}

unsafe impl Sync for Dictionary {}

impl Dictionary {
    pub fn new(filename: &str) -> io::Result<Dictionary> {
        let fn_c = CString::new(filename)?;
        let ptr = unsafe { root::keyvi_create_dictionary(fn_c.as_ptr()) };
        if ptr.is_null() {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "could not load file",
            ))
        } else {
            Ok(Dictionary { dict: ptr })
        }
    }
    pub fn new_with_strategy(
        filename: &str,
        loading_strategy: LoadingStrategyTypes,
    ) -> io::Result<Dictionary> {
        let fn_c = CString::new(filename)?;
        let ptr = unsafe { root::keyvi_create_dictionary_with_strategy(fn_c.as_ptr()) };
        if ptr.is_null() {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "could not load file",
            ))
        } else {
            Ok(Dictionary { dict: ptr })
        }
    }

    pub fn statistics(&self) -> String {
        let c_buf: *mut ::std::os::raw::c_char =
            unsafe { root::keyvi_dictionary_get_statistics(self.dict) };
        KeyviString::new(c_buf).to_owned()
    }

    pub fn size(&self) -> usize {
        unsafe { root::keyvi_dictionary_get_size(self.dict) }
    }

    pub fn get(&self, key: &str) -> KeyviMatch {
        let match_ptr = unsafe {
            root::keyvi_dictionary_get(
                self.dict,
                key.as_ptr() as *const ::std::os::raw::c_char,
                key.len() as usize,
            )
        };
        KeyviMatch::new(match_ptr)
    }

    pub fn get_all_items(&self) -> KeyviMatchIterator {
        let ptr = unsafe { root::keyvi_dictionary_get_all_items(self.dict) };
        KeyviMatchIterator::new(ptr)
    }

    pub fn get_prefix_completions(&self, key: &str, cutoff: usize) -> KeyviMatchIterator {
        let ptr = unsafe {
            root::keyvi_dictionary_get_prefix_completions(
                self.dict,
                key.as_ptr() as *const ::std::os::raw::c_char,
                key.len() as usize,
                cutoff,
            )
        };
        KeyviMatchIterator::new(ptr)
    }

    pub fn get_fuzzy(&self, key: &str, max_edit_distance: usize) -> KeyviMatchIterator {
        let ptr = unsafe {
            root::keyvi_dictionary_get_fuzzy(
                self.dict,
                key.as_ptr() as *const ::std::os::raw::c_char,
                key.len() as usize,
                max_edit_distance,
            )
        };
        KeyviMatchIterator::new(ptr)
    }

    pub fn get_multi_word_completions(&self, key: &str, cutoff: usize) -> KeyviMatchIterator {
        let ptr = unsafe {
            root::keyvi_dictionary_get_multi_word_completions(
                self.dict,
                key.as_ptr() as *const ::std::os::raw::c_char,
                key.len() as usize,
                cutoff,
            )
        };
        KeyviMatchIterator::new(ptr)
    }
}

impl Drop for Dictionary {
    fn drop(&mut self) {
        unsafe {
            root::keyvi_dictionary_destroy(self.dict);
        }
    }
}
