fn main() {
    if cfg!(target_os = "linux") {
        // Add Python library configuration
        println!("cargo:rustc-link-lib=python3.10");
        println!("cargo:rustc-link-search=/usr/lib/python3.10/config-3.10-x86_64-linux-gnu");
        println!("cargo:rustc-link-search=/usr/lib/x86_64-linux-gnu");
        
        // Add rpath to ensure library can be found at runtime
        println!("cargo:rustc-link-arg=-Wl,-rpath,/usr/lib/python3.10/config-3.10-x86_64-linux-gnu");
        
        // Use pyo3 build config
        pyo3_build_config::add_extension_module_link_args();
    }
}