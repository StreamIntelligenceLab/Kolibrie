/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::Arc;
use std::fmt::{self, Debug};
use crate::sparql_database::SparqlDatabase;

#[derive(Clone)]
pub struct ClonableFn(Arc<dyn Fn(Vec<&str>) -> String + Send + Sync>);

impl ClonableFn {
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(Vec<&str>) -> String + Send + Sync + 'static,
    {
        ClonableFn(Arc::new(f))
    }

    pub fn call(&self, args: Vec<&str>) -> String {
        (self.0)(args)
    }
}

// Implement Debug for ClonableFn
impl Debug for ClonableFn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClonableFn(<function>)")
    }
}

// Basic HTTP server function
pub fn run_server() {
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();
    let mut database = SparqlDatabase::new();

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream, &mut database);
    }
}

pub fn handle_connection(mut stream: TcpStream, database: &mut SparqlDatabase) {
    let mut buffer = [0; 1024];
    stream.read_exact(&mut buffer).unwrap();

    let request = String::from_utf8_lossy(&buffer[..]);
    let response = database.handle_http_request(&request);

    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
        response.len(),
        response
    );
    stream.write_all(response.as_bytes()).unwrap();
    stream.flush().unwrap();
}

pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}

// Custom function to get the number of CPUs without using the num_cpus crate
pub fn get_num_cpus() -> usize {
    // For Unix-like systems
    #[cfg(unix)]
    {
        unsafe {
            let cpus = libc::sysconf(libc::_SC_NPROCESSORS_ONLN);
            if cpus > 0 {
                cpus as usize
            } else {
                1 // Fallback to 1 if sysconf fails
            }
        }
    }

    // For Windows systems
    #[cfg(windows)]
    {
        use winapi::um::sysinfoapi::{GetSystemInfo, SYSTEM_INFO};
        unsafe {
            let mut sysinfo: SYSTEM_INFO = std::mem::zeroed();
            GetSystemInfo(&mut sysinfo);
            sysinfo.dwNumberOfProcessors as usize
        }
    }

    // Fallback for other platforms
    #[cfg(not(any(unix, windows)))]
    {
        1
    }
}
