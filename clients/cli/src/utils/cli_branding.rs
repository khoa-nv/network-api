use colored::Colorize;

pub fn print_banner() {
    println!("{}", LOGO_NAME.bright_cyan());
    println!(
        "{} {} {}\n",
        "Welcome to the".bright_white(),
        "Nexus Network CLI".bright_cyan().bold(),
        "v0.5.5".bright_white()
    );
    println!(
        "{}",
        "The Nexus network is a massively-parallelized proof network for executing and proving the \x1b]8;;https://docs.nexus.org\x1b\\Nexus zkVM\x1b]8;;\x1b\\.\n\n"
            .bright_white()
    );
}
