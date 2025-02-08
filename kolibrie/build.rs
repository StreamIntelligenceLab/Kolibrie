use std::env;
use std::path::Path;
use std::process::Command;

fn main() {
     if env::var("CARGO_FEATURE_CUDA").is_ok() {
        // Determine the root of the crate
        let project_root = Path::new(env!("CARGO_MANIFEST_DIR"));
        
        // Dynamically set the path to the CUDA source directory
        let cuda_dir = project_root.join("src").join("cuda");

        if !cuda_dir.exists() {
            panic!(
                "Expected CUDA directory at {:?}, but it does not exist.",
                cuda_dir
            );
        }

        // Determine the target platform
        let target_os = env::var("CARGO_CFG_TARGET_OS").expect("Failed to get target OS");

        match target_os.as_str() {
            "windows" | "linux" => build_with_cmake(&cuda_dir, target_os),
            _ => panic!("Unsupported target OS: {}", target_os),
        }
    }
}

fn build_with_cmake(cuda_dir: &Path, target_os: String) {
    let output_dir = cuda_dir.join("output");

    // Step 1: Configure CMake
    let mut cmake_configure = Command::new("cmake");
    cmake_configure
        .current_dir(&cuda_dir)
        .arg("-DCMAKE_BUILD_TYPE=Release")
        .arg(format!("-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={}", output_dir.to_str().unwrap()))
        .arg(".");

    if target_os == "windows" {
        cmake_configure.arg("-G").arg("NMake Makefiles");
    }

    let status = cmake_configure.status().expect("Failed to configure CMake");
    if !status.success() {
        panic!("CMake configuration failed");
    }

    // Step 2: Build using CMake
    let mut cmake_build = Command::new("cmake");
    cmake_build.current_dir(&cuda_dir).arg("--build").arg(".");
    if !cmake_build.status().expect("Failed to build with CMake").success() {
        panic!("CMake build failed");
    }

    // Step 3: Link the generated library
    println!(
        "cargo:rustc-link-search=native={}",
        output_dir.to_str().unwrap()
    );
    println!("cargo:rustc-link-lib=cudajoin");

    // Step 4: Ensure Cargo rebuilds if these files change
    println!(
        "cargo:rerun-if-changed={}",
        cuda_dir.join("CMakeLists.txt").to_str().unwrap()
    );
    println!(
        "cargo:rerun-if-changed={}",
        cuda_dir.join("cuda_join.cu").to_str().unwrap()
    );
}
