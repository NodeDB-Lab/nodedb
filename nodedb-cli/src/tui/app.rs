//! TUI application state and event loop.

use std::io::Stdout;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

use nodedb_client::NativeClient;

use crate::args::OutputFormat;
use crate::format;
use crate::history::History;
use crate::metacommand;

use super::input::InputState;
use super::render;

/// Main TUI application.
pub struct App {
    client: NativeClient,
    pub input: InputState,
    pub result_output: Option<String>,
    pub error_message: Option<String>,
    pub output_format: OutputFormat,
    pub last_query_time: Option<Duration>,
    pub host: String,
    pub port: u16,
    pub scroll_offset: u16,
    pub show_timing: bool,
    history: History,
    running: bool,
}

impl App {
    pub fn new(client: NativeClient, format: OutputFormat, host: String, port: u16) -> Self {
        Self {
            client,
            input: InputState::new(),
            result_output: None,
            error_message: None,
            output_format: format,
            last_query_time: None,
            host,
            port,
            scroll_offset: 0,
            show_timing: true,
            history: History::load(),
            running: true,
        }
    }

    /// Run the TUI event loop.
    pub async fn run(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    ) -> crate::error::CliResult<()> {
        while self.running {
            terminal.draw(|frame| render::render(frame, self))?;

            // Poll for events with a 100ms timeout (for responsive UI).
            if event::poll(Duration::from_millis(100))?
                && let Event::Key(key) = event::read()?
            {
                self.handle_key(key).await;
            }
        }

        self.history.save();
        Ok(())
    }

    async fn handle_key(&mut self, key: KeyEvent) {
        match (key.modifiers, key.code) {
            // Quit.
            (KeyModifiers::CONTROL, KeyCode::Char('c')) => {
                if self.input.is_empty() {
                    self.running = false;
                } else {
                    // Clear input on first Ctrl+C.
                    self.input.take();
                }
            }
            (KeyModifiers::CONTROL, KeyCode::Char('d')) => {
                if self.input.is_empty() {
                    self.running = false;
                }
            }

            // Execute: Enter when input ends with ';' or starts with '\'.
            (KeyModifiers::NONE | KeyModifiers::SHIFT, KeyCode::Enter) => {
                if self.input.buffer().trim_start().starts_with('\\')
                    || self.input.ends_with_semicolon()
                {
                    self.execute_input().await;
                } else {
                    self.input.newline();
                }
            }

            // Force execute (Ctrl+Enter).
            (KeyModifiers::CONTROL, KeyCode::Enter) => {
                if !self.input.is_empty() {
                    self.execute_input().await;
                }
            }

            // Line editing.
            (KeyModifiers::NONE, KeyCode::Backspace) => self.input.backspace(),
            (KeyModifiers::NONE, KeyCode::Delete) => self.input.delete(),
            (KeyModifiers::NONE, KeyCode::Left) => self.input.move_left(),
            (KeyModifiers::NONE, KeyCode::Right) => self.input.move_right(),
            (KeyModifiers::NONE, KeyCode::Home) => self.input.home(),
            (KeyModifiers::NONE, KeyCode::End) => self.input.end(),
            (KeyModifiers::CONTROL, KeyCode::Char('a')) => self.input.home(),
            (KeyModifiers::CONTROL, KeyCode::Char('e')) => self.input.end(),
            (KeyModifiers::CONTROL, KeyCode::Char('k')) => self.input.kill_to_end(),
            (KeyModifiers::CONTROL, KeyCode::Char('w')) => self.input.delete_word(),
            (KeyModifiers::CONTROL, KeyCode::Char('u')) => {
                self.input.take();
            }

            // History navigation (when input is empty or already browsing).
            (KeyModifiers::NONE, KeyCode::Up) => {
                self.input.history_up(&self.history);
            }
            (KeyModifiers::NONE, KeyCode::Down) => {
                self.input.history_down(&self.history);
            }

            // Scroll results.
            (KeyModifiers::NONE, KeyCode::PageUp) => {
                self.scroll_offset = self.scroll_offset.saturating_sub(10);
            }
            (KeyModifiers::NONE, KeyCode::PageDown) => {
                self.scroll_offset = self.scroll_offset.saturating_add(10);
            }

            // Character input.
            (KeyModifiers::NONE | KeyModifiers::SHIFT, KeyCode::Char(c)) => {
                self.input.insert(c);
            }
            (KeyModifiers::NONE, KeyCode::Tab) => {
                self.input.insert(' ');
                self.input.insert(' ');
            }

            _ => {}
        }
    }

    async fn execute_input(&mut self) {
        let input = self.input.take();
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return;
        }

        self.history.add(trimmed);
        self.error_message = None;
        self.scroll_offset = 0;

        // Metacommand?
        if trimmed.starts_with('\\') {
            self.handle_metacommand(trimmed).await;
            return;
        }

        // SQL execution.
        let sql = trimmed.trim_end_matches(';');
        let start = Instant::now();
        match self.client.query(sql).await {
            Ok(result) => {
                self.last_query_time = Some(start.elapsed());
                let output = format::format_result(&result, self.output_format);
                self.result_output = Some(output);
            }
            Err(e) => {
                self.last_query_time = Some(start.elapsed());
                self.error_message = Some(format!("ERROR: {e}"));
                self.result_output = None;
            }
        }
    }

    async fn handle_metacommand(&mut self, input: &str) {
        match metacommand::parse(input) {
            metacommand::MetaAction::Sql(sql) => {
                let start = Instant::now();
                match self.client.query(&sql).await {
                    Ok(result) => {
                        self.last_query_time = Some(start.elapsed());
                        let output = format::format_result(&result, self.output_format);
                        self.result_output = Some(output);
                        self.error_message = None;
                    }
                    Err(e) => {
                        self.last_query_time = Some(start.elapsed());
                        self.error_message = Some(format!("ERROR: {e}"));
                        self.result_output = None;
                    }
                }
            }
            metacommand::MetaAction::SetFormat(f) => match f.as_str() {
                "table" | "t" => self.output_format = OutputFormat::Table,
                "json" | "j" => self.output_format = OutputFormat::Json,
                "csv" | "c" => self.output_format = OutputFormat::Csv,
                _ => {
                    self.error_message =
                        Some(format!("Unknown format '{f}'. Use: table, json, csv"));
                }
            },
            metacommand::MetaAction::ToggleTiming => {
                self.show_timing = !self.show_timing;
                self.result_output = Some(format!(
                    "Timing is {}.",
                    if self.show_timing { "on" } else { "off" }
                ));
            }
            metacommand::MetaAction::Help => {
                self.result_output = Some(metacommand::help_text().to_string());
                self.error_message = None;
            }
            metacommand::MetaAction::Quit => {
                self.running = false;
            }
            metacommand::MetaAction::Unknown(cmd) => {
                self.error_message = Some(format!("Unknown command: {cmd}. Type \\? for help."));
            }
        }
    }
}
