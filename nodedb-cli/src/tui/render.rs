//! TUI rendering with ratatui.
//!
//! Layout: status bar (top) + results panel (middle) + input area (bottom).

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

use super::app::App;

/// Render the full TUI layout.
pub fn render(frame: &mut Frame, app: &App) {
    let area = frame.area();

    let input_height = input_line_count(app) + 2; // +2 for borders
    let layout = Layout::vertical([
        Constraint::Length(1),                   // status bar
        Constraint::Min(3),                      // results
        Constraint::Length(input_height as u16), // input
    ])
    .split(area);

    render_status_bar(frame, layout[0], app);
    render_results(frame, layout[1], app);
    render_input(frame, layout[2], app);
}

fn render_status_bar(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    let status_style = Style::default().bg(Color::DarkGray).fg(Color::White);

    let format_str = match app.output_format {
        crate::args::OutputFormat::Table => "table",
        crate::args::OutputFormat::Json => "json",
        crate::args::OutputFormat::Csv => "csv",
    };

    let timing_str = if let Some(d) = app.last_query_time {
        format!(" {:.1}ms", d.as_secs_f64() * 1000.0)
    } else {
        String::new()
    };

    let left = format!(" NodeDB CLI │ {}:{}", app.host, app.port);
    let right = format!("{}{} ", format_str, timing_str);
    let padding = area
        .width
        .saturating_sub(left.len() as u16 + right.len() as u16);

    let line = Line::from(vec![
        Span::styled(left, status_style.add_modifier(Modifier::BOLD)),
        Span::styled(" ".repeat(padding as usize), status_style),
        Span::styled(right, status_style),
    ]);

    frame.render_widget(Paragraph::new(line), area);
}

fn render_results(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));

    let content = if let Some(ref err) = app.error_message {
        Paragraph::new(err.as_str())
            .style(Style::default().fg(Color::Red))
            .block(block)
            .wrap(Wrap { trim: false })
    } else if let Some(ref output) = app.result_output {
        Paragraph::new(output.as_str())
            .block(block)
            .wrap(Wrap { trim: false })
            .scroll((app.scroll_offset, 0))
    } else {
        let welcome = "Type SQL and press Enter (with ;) to execute.\n\
                        Use \\? for metacommand help, \\q to quit.";
        Paragraph::new(welcome)
            .style(Style::default().fg(Color::DarkGray))
            .block(block)
    };

    frame.render_widget(content, area);
}

fn render_input(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    let block = Block::default()
        .title(" ndb> ")
        .title_style(
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        )
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Green));

    let input_text = app.input.buffer();
    let paragraph = Paragraph::new(input_text).block(block);
    frame.render_widget(paragraph, area);

    // Position the cursor.
    let cursor_x = cursor_col(app.input.buffer(), app.input.cursor());
    let cursor_y = cursor_row(app.input.buffer(), app.input.cursor());
    frame.set_cursor_position((area.x + 1 + cursor_x as u16, area.y + 1 + cursor_y as u16));
}

fn input_line_count(app: &App) -> usize {
    let lines = app.input.buffer().lines().count().max(1);
    lines.min(6) // Cap at 6 lines
}

fn cursor_col(buffer: &str, byte_pos: usize) -> usize {
    let before = &buffer[..byte_pos];
    match before.rfind('\n') {
        Some(nl) => before.len() - nl - 1,
        None => before.len(),
    }
}

fn cursor_row(buffer: &str, byte_pos: usize) -> usize {
    buffer[..byte_pos].matches('\n').count()
}
