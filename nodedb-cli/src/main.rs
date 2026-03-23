mod args;
mod batch;
mod connect;
mod error;
mod format;
mod history;
mod metacommand;
mod tui;

use std::io;

use clap::Parser;
use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

use args::CliArgs;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = CliArgs::parse();
    let client = connect::build_client(&args)?;

    // Non-interactive: -e flag.
    if let Some(ref sql) = args.execute {
        batch::run(&client, sql, args.format).await?;
        return Ok(());
    }

    // Non-interactive: piped stdin.
    if !atty_is_stdin() {
        let sql = io::read_to_string(io::stdin())?;
        batch::run(&client, &sql, args.format).await?;
        return Ok(());
    }

    // Interactive TUI.
    crossterm::terminal::enable_raw_mode()?;
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = tui::app::App::new(client, args.format, args.host.clone(), args.port);
    let result = app.run(&mut terminal).await;

    // Restore terminal.
    crossterm::terminal::disable_raw_mode()?;
    crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen)?;

    result?;
    Ok(())
}

/// Check if stdin is a terminal (not piped).
fn atty_is_stdin() -> bool {
    std::io::IsTerminal::is_terminal(&io::stdin())
}
