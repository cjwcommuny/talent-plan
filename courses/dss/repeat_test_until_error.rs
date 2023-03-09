use std::env::args;
use std::process::Command;

fn main() {
    let mut args = args();
    let test_name = args.nth(1).unwrap();
    let max_trial: usize = args.nth(2).unwrap_or("200".to_string()).parse().unwrap();
    for index in 1..max_trial + 1 {
        println!("begin test {}", index);
        let output = Command::new("cargo")
            .env("RUST_LOG", "raft=trace")
            .args(["test", "--package", "raft", "--", "--test", &test_name])
            .output()
            .expect("failed to execute process");

        if !output.status.success() {
            println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
            println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
            break;
        }
    }
}
