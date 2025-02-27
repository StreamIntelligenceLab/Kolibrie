fn main() {
    // Use pyo3 build config
    pyo3_build_config::add_extension_module_link_args();

    if cfg!(target_os = "linux") {
        // Add Python library configuration
        println!("cargo:rustc-link-lib=python3.10");
        println!("cargo:rustc-link-search=/usr/lib/python3.10/config-3.10-x86_64-linux-gnu");
        println!("cargo:rustc-link-search=/usr/lib/x86_64-linux-gnu");
        
        // Add rpath to ensure library can be found at runtime
        println!("cargo:rustc-link-arg=-Wl,-rpath,/usr/lib/python3.10/config-3.10-x86_64-linux-gnu");
    } else if cfg!(target_os = "macos") {
        // Get Python prefix on macOS
        let output = std::process::Command::new("python3-config")
            .arg("--prefix")
            .output()
            .expect("Failed to execute python3-config");
        let python_prefix = std::str::from_utf8(&output.stdout).unwrap().trim();
        
        // Get Python version
        let output = std::process::Command::new("python3")
            .args(["-c", "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"])
            .output()
            .expect("Failed to get Python version");
        let python_version = std::str::from_utf8(&output.stdout).unwrap().trim();
        
        // Add macOS-specific configuration
        let framework_dir = format!("{}/Python.framework/Versions/{}", python_prefix, python_version);
        
        // Check if this is a framework build
        if std::path::Path::new(&framework_dir).exists() {
            println!("cargo:rustc-link-search=framework={}/Python.framework/Versions/{}", python_prefix, python_version);
            println!("cargo:rustc-link-search={}/lib", framework_dir);
            println!("cargo:rustc-link-lib=python{}", python_version);
        } else {
            // Add all potential Python lib locations for macOS
            println!("cargo:rustc-link-search=/usr/local/opt/python/Frameworks/Python.framework/Versions/Current/lib");
            println!("cargo:rustc-link-search=/opt/homebrew/opt/python/Frameworks/Python.framework/Versions/Current/lib");
            println!("cargo:rustc-link-search=/opt/homebrew/lib");
            println!("cargo:rustc-link-search=/usr/local/lib");
            
            // Try to find the Python library directly
            for path in [
                format!("/opt/homebrew/opt/python@{}/lib", python_version),
                format!("/opt/homebrew/lib/python{}", python_version),
                format!("/usr/local/opt/python@{}/lib", python_version),
                format!("/Library/Frameworks/Python.framework/Versions/{}/lib", python_version),
            ] {
                println!("cargo:rustc-link-search={}", path);
            }
            
            // Try different naming conventions for the Python library
            println!("cargo:rustc-link-lib=python{}", python_version);
            println!("cargo:rustc-link-lib=python{}.dylib", python_version);
            
            // Explicitly include the Python library
            println!("cargo:rustc-link-arg=-lpython{}", python_version);
        }
        
        // Add rpath to ensure library can be found at runtime
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}/lib", framework_dir);
        println!("cargo:rustc-link-arg=-Wl,-rpath,/opt/homebrew/lib");
        println!("cargo:rustc-link-arg=-Wl,-rpath,/usr/local/lib");
    }
}