/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v.2.0.If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use shared::triple::Triple;
use std::path::PathBuf;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, BufReader, Write, BufRead};

/// Write-Ahead Log for crash recovery
#[derive(Debug)]
pub struct WriteAheadLog {
    file: Option<BufWriter<File>>,
    path: PathBuf,
    sync_on_write: bool,
}

impl WriteAheadLog {
    pub fn new(path: PathBuf, sync_on_write: bool) -> Result<Self, String> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| format!("Failed to open WAL: {}", e))?;
        
        Ok(Self {
            file: Some(BufWriter::new(file)),
            path,
            sync_on_write,
        })
    }

    /// Log a triple insert operation
    pub fn log_insert(&mut self, triple: &Triple) -> Result<(), String> {
        if let Some(ref mut file) = self.file {
            let entry = format!("INSERT {} {} {}\n", triple.subject, triple.predicate, triple.object);
            file.write_all(entry.as_bytes())
                .map_err(|e| format!("WAL write failed: {}", e))?;
            
            if self.sync_on_write {
                file.flush().map_err(|e| format!("WAL flush failed: {}", e))?;
            }
        }
        Ok(())
    }

    /// Log a triple delete operation
    pub fn log_delete(&mut self, triple: &Triple) -> Result<(), String> {
        if let Some(ref mut file) = self.file {
            let entry = format!("DELETE {} {} {}\n", triple.subject, triple.predicate, triple.object);
            file.write_all(entry.as_bytes())
                .map_err(|e| format!("WAL write failed: {}", e))?;
            
            if self.sync_on_write {
                file.flush().map_err(|e| format!("WAL flush failed: {}", e))?;
            }
        }
        Ok(())
    }

    /// Flush WAL to disk
    pub fn flush(&mut self) -> Result<(), String> {
        if let Some(ref mut file) = self.file {
            file.flush().map_err(|e| format!("WAL flush failed: {}", e))?;
        }
        Ok(())
    }

    /// Recover operations from WAL
    pub fn recover(&self) -> Result<Vec<(WalOperation, Triple)>, String> {
        let file = File::open(&self.path)
            .map_err(|e| format!("Failed to open WAL for recovery: {}", e))?;
        let reader = BufReader::new(file);
        let mut operations = Vec::new();

        for line in reader.lines() {
            let line = line.map_err(|e| format!("WAL read error: {}", e))?;
            let parts: Vec<&str> = line.split_whitespace().collect();
            
            if parts.len() != 4 {
                continue;
            }

            let op = match parts[0] {
                "INSERT" => WalOperation::Insert,
                "DELETE" => WalOperation::Delete,
                _ => continue,
            };

            let triple = Triple {
                subject: parts[1].parse().unwrap_or(0),
                predicate: parts[2].parse().unwrap_or(0),
                object: parts[3].parse().unwrap_or(0),
            };

            operations.push((op, triple));
        }

        Ok(operations)
    }

    /// Clear WAL after successful flush
    pub fn clear(&mut self) -> Result<(), String> {
        if let Some(ref mut file) = self.file {
            file.flush().map_err(|e| format!("WAL flush failed: {}", e))?;
        }
        
        // Truncate the file
        std::fs::write(&self.path, "")
            .map_err(|e| format!("WAL clear failed: {}", e))?;
        
        // Reopen
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| format!("Failed to reopen WAL: {}", e))?;
        
        self.file = Some(BufWriter::new(file));
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalOperation {
    Insert,
    Delete,
}