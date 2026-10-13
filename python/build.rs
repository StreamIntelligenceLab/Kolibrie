/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Links the extension module against the host Python on macOS.
//!
//! A cdylib that Python loads must leave its Python symbols unresolved at link
//! time and pick them up from the interpreter that loads it. `-undefined
//! dynamic_lookup` is how macOS expresses that; other platforms need nothing
//! here.

fn main() {
    if cfg!(target_os = "macos") {
        println!("cargo:rustc-link-arg=-undefined");
        println!("cargo:rustc-link-arg=dynamic_lookup");
    }
}

