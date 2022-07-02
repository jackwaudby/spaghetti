fn main() {
    cxx_build::bridge("src/cpp.rs")
        .file("src/shared_spin_mutex.cc")
        .flag_if_supported("-std=c++14")
        .compile("cxx-demo");
}
