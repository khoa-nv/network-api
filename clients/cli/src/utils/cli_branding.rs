use colored::Colorize;

const LOGO_NAME: &str = r#"
███╗   ██╗███████╗██╗  ██╗██╗   ██╗███████╗
████╗  ██║██╔════╝╚██╗██╔╝██║   ██║██╔════╝
██╔██╗ ██║█████╗   ╚███╔╝ ██║   ██║███████╗
██║╚██╗██║██╔══╝   ██╔██╗ ██║   ██║╚════██║
██║ ╚████║███████╗██╔╝ ██╗╚██████╔╝███████║
╚═╝  ╚═══╝╚══════╝╚═╝  ╚═╝ ╚═════╝ ╚══════╝
"#;

const VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn print_banner() {
    println!("{}", LOGO_NAME.bright_cyan());
    println!("Nexus Network CLI v{}\n", VERSION);
    println!("Welcome to the Nexus Network! This CLI allows you to contribute to the network by running a prover node.");
    println!("For more information, visit: https://nexus.xyz/network\n");
}

pub fn print_logo() {
    println!("{}", LOGO_NAME.bright_cyan());
}
