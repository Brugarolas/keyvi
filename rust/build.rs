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
 *  build.rs
 *
 *  Created on: September 4, 2017
 *  Author: Narek Gharibyan <narekgharibyan@gmail.com>
 *          Subu <subu@cliqz.com>
 */
extern crate bindgen;
extern crate cmake;

use std::env;
use std::path::PathBuf;

fn main() {
    let dst = cmake::Config::new("keyvi_core/")
        .build_target("keyvi_c")
        .build();
    let keyvi_c_path = dst.join("build").display().to_string();

    // trigger rebuild
    println!("cargo:rerun-if-changed=keyvi_core/");
    println!("cargo:rerun-if-env-changed=STATIC_ZLIB_PATH");
    println!("cargo:rerun-if-env-changed=STATIC_SNAPPY_PATH");

    // link dependencies
    println!("cargo:rustc-link-lib=static=keyvi_c");
    println!("cargo:rustc-link-search=native={}", keyvi_c_path);

    // using nightly-only feature static-bundle would make this unnecessary
    if let Ok(zlib_path) = env::var("STATIC_ZLIB_PATH") {
        println!("cargo:rustc-link-lib=static=z");
        println!("cargo:rustc-link-search=native={}", zlib_path);
    } else {
        println!("cargo:rustc-link-lib=dylib=z");
    }

    if let Ok(snappy_path) = env::var("STATIC_SNAPPY_PATH") {
        println!("cargo:rustc-link-lib=static=snappy");
        println!("cargo:rustc-link-search=native={}", snappy_path);
    } else {
        println!("cargo:rustc-link-lib=dylib=snappy");
    }

    let target = std::env::var("TARGET").unwrap_or_default();
    if target == "x86_64-apple-darwin" || target == "aarch64-apple-darwin" {
        println!("cargo:rustc-link-lib=dylib=c++");
    } else {
        println!("cargo:rustc-link-lib=dylib=stdc++");
    }

    let bindings = bindgen::Builder::default()
        // The input header we would like to generate bindings for.
        .header("keyvi_core/keyvi/include/keyvi/c_api/c_api.h")
        .clang_arg("-x")
        .clang_arg("c++")
        .enable_cxx_namespaces()
        .layout_tests(true)
        .allowlist_function("keyvi_bytes_destroy")
        .allowlist_function("keyvi_string_destroy")
        .allowlist_function("keyvi_create_dictionary")
        .allowlist_function("keyvi_create_dictionary_with_strategy")
        .allowlist_function("keyvi_dictionary_destroy")
        .allowlist_function("keyvi_dictionary_get")
        .allowlist_function("keyvi_dictionary_get_all_items")
        .allowlist_function("keyvi_dictionary_get_fuzzy")
        .allowlist_function("keyvi_dictionary_get_multi_word_completions")
        .allowlist_function("keyvi_dictionary_get_prefix_completions")
        .allowlist_function("keyvi_dictionary_get_size")
        .allowlist_function("keyvi_dictionary_get_statistics")
        .allowlist_function("keyvi_match_destroy")
        .allowlist_function("keyvi_match_get_matched_string")
        .allowlist_function("keyvi_match_get_msgpacked_value")
        .allowlist_function("keyvi_match_get_score")
        .allowlist_function("keyvi_match_get_value_as_string")
        .allowlist_function("keyvi_match_is_empty")
        .allowlist_function("keyvi_match_iterator_dereference")
        .allowlist_function("keyvi_match_iterator_destroy")
        .allowlist_function("keyvi_match_iterator_empty")
        .allowlist_function("keyvi_match_iterator_increment")
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    println!("Saving to bindings..");
    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
