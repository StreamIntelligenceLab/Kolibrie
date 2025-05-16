/*
 * Copyright © 2024 ladroid
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

fn main() {
    // Use pyo3 build config
    pyo3_build_config::add_extension_module_link_args();

    if cfg!(target_os = "linux") {
        // Get Python version
        let python_version = get_python_config("import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')");
        
        // Get configuration for Linux
        let lib_path = format!("/usr/lib/python{}/config-{}-x86_64-linux-gnu", python_version, python_version);
        
        // Add Python library configuration
        println!("cargo:rustc-link-lib=python{}", python_version);
        println!("cargo:rustc-link-search={}", lib_path);
        println!("cargo:rustc-link-search=/usr/lib/x86_64-linux-gnu");
        
        // Add rpath to ensure library can be found at runtime
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_path);
    } else if cfg!(target_os = "macos") {
        // Get Python prefix
        let python_prefix = get_command_output("python3-config", &["--prefix"]);
        
        // Get Python version
        let python_version = get_python_config("import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')");

        // Check for the framework path specifically at the prefix location
        let is_framework = python_prefix.contains("Python.framework") || 
                           std::path::Path::new(&format!("{}/Python.framework", python_prefix)).exists();

        if is_framework {
            // Extract framework base directory
            let framework_dir: String;
            if python_prefix.contains("Python.framework") {
                // Python prefix already points to framework location
                framework_dir = python_prefix.clone();
            } else {
                framework_dir = format!("{}/Frameworks/Python.framework/Versions/{}", python_prefix, python_version);
            }
            
            // Add framework search path (look for the directory containing the framework)
            let framework_parent = std::path::Path::new(&framework_dir).parent().unwrap().parent().unwrap().parent().unwrap();
            println!("cargo:rustc-link-search=framework={}", framework_parent.display());
            
            // Link to Python framework
            println!("cargo:rustc-link-lib=framework=Python");
            
            // Add runtime path to ensure framework can be found
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", framework_dir);
        } else {
            
            // Get library configuration
            let libdir = get_python_config("import sysconfig; print(sysconfig.get_config_var('LIBDIR'))");
            let libpl = get_python_config("import sysconfig; print(sysconfig.get_config_var('LIBPL'))");
            let ldlibrary = get_python_config("import sysconfig; print(sysconfig.get_config_var('LDLIBRARY'))");

            // Special case: If the library name contains "framework", this is actually a framework
            // but not in the expected location
            if ldlibrary.contains("Python.framework") {
                
                // Extract the framework path from the library name
                let framework_parent = if libdir.contains("Python.framework") {
                    // Try to extract the parent directory
                    std::path::Path::new(&libdir).parent().unwrap().parent().unwrap().parent().unwrap()
                } else {
                    // Default to common locations
                    std::path::Path::new("/Library/Frameworks")
                };
                
                // Add framework search path
                println!("cargo:rustc-link-search=framework={}", framework_parent.display());
                
                // Link to Python framework
                println!("cargo:rustc-link-lib=framework=Python");
                
                // Add runtime path
                println!("cargo:rustc-link-arg=-Wl,-rpath,{}", libdir);
                println!("cargo:rustc-link-arg=-Wl,-rpath,{}", libpl);
            } else {
                
                // Check all possible paths
                let lib_paths = [
                    format!("{}/{}", libdir, ldlibrary),
                    format!("{}/{}", libpl, ldlibrary),
                    format!("/opt/homebrew/lib/{}", ldlibrary),
                    format!("/usr/local/lib/{}", ldlibrary),
                ];
                
                // First, try to find the library
                let mut found_path = false;
                for path in &lib_paths {
                    if std::path::Path::new(path).exists() {
                        let dir = std::path::Path::new(path).parent().unwrap().to_str().unwrap();
                        println!("cargo:rustc-link-search={}", dir);
                        found_path = true;
                        break;
                    }
                }
                
                // If not found, add all possible search paths
                if !found_path {
                    println!("cargo:rustc-link-search={}", libdir);
                    println!("cargo:rustc-link-search={}", libpl);
                    println!("cargo:rustc-link-search=/usr/local/lib");
                    
                    // Only add homebrew lib if it exists
                    if std::path::Path::new("/opt/homebrew/lib").exists() {
                        println!("cargo:rustc-link-search=/opt/homebrew/lib");
                    }
                }
                
                // Handle different library naming formats
                let lib_name = if ldlibrary.starts_with("lib") && ldlibrary.ends_with(".dylib") {
                    ldlibrary[3..ldlibrary.len() - 6].to_string()
                } else if ldlibrary.starts_with("lib") && ldlibrary.ends_with(".so") {
                    ldlibrary[3..ldlibrary.len() - 3].to_string()
                } else {
                    // For non-standard library names, try python3.x
                    format!("python{}", python_version)
                };
                println!("cargo:rustc-link-lib={}", lib_name);
                
                // Add rpaths for runtime
                for dir in [&libdir, &libpl, "/usr/local/lib"] {
                    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", dir);
                }
                
                // Only add homebrew lib path if it exists
                if std::path::Path::new("/opt/homebrew/lib").exists() {
                    println!("cargo:rustc-link-arg=-Wl,-rpath,/opt/homebrew/lib");
                }
            }
        }
        
        // Set environment variables
        println!("cargo:rustc-env=PYTHONPATH={}", get_python_config("import sys; print(':'.join(sys.path))"));
    }
}

fn get_python_config(script: &str) -> String {
    match std::process::Command::new("python3").args(["-c", script]).output() {
        Ok(output) if output.status.success() => String::from_utf8_lossy(&output.stdout).trim().to_string(),
        _ => String::new(),
    }
}

fn get_command_output(command: &str, args: &[&str]) -> String {
    match std::process::Command::new(command).args(args).output() {
        Ok(output) if output.status.success() => String::from_utf8_lossy(&output.stdout).trim().to_string(),
        _ => String::new(),
    }
}