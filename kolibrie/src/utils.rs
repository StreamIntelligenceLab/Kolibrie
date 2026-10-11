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
/// A user-defined function stored so the database can be cloned.
///
/// Wraps the closure in an `Arc` because [`SparqlDatabase`] derives `Clone` and a
/// bare `Box<dyn Fn>` is not cloneable. Cloning shares the same function rather
/// than duplicating it.
pub struct ClonableFn(Arc<dyn Fn(Vec<&str>) -> String + Send + Sync>);

impl ClonableFn {
    /// Wraps `f` for storage in a database.
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(Vec<&str>) -> String + Send + Sync + 'static,
    {
        ClonableFn(Arc::new(f))
    }

    /// Invokes the function with lexical arguments, as they appear in the query.
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

/// A minimal demonstration HTTP server on `127.0.0.1:7878`.
///
/// For trying the engine out, not for serving traffic: it handles one connection
/// at a time, reads a fixed 1 KiB buffer, and panics on any I/O error. The real
/// server is the `kolibrie-http-server` binary.
pub fn run_server() {
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();
    let mut database = SparqlDatabase::new();

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream, &mut database);
    }
}

/// Answers one request on `stream` from `database`.
///
/// # Panics
///
/// On any read or write error, and if the request is shorter than the 1 KiB it
/// insists on reading. See [`run_server`].
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

/// Seconds since the Unix epoch.
///
/// # Panics
///
/// If the system clock is before the epoch.
pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}

/// Number of logical CPUs, queried directly rather than through a dependency.
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
