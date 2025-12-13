use anyhow::Result;
use colored::Colorize;
use comfy_table::{
    modifiers::{UTF8_ROUND_CORNERS, UTF8_SOLID_INNER_BORDERS},
    presets::UTF8_FULL,
    Cell, Color, Table,
};
use nats3_types::{LoadJob, LoadJobStatus, StoreJob, StoreJobStatus};
use serde::Serialize;

use crate::config::OutputFormat;

pub fn print_load_jobs(jobs: Vec<LoadJob>, format: &OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Table => print_load_jobs_table(jobs),
        OutputFormat::Json => print_json(&jobs),
    }
}

pub fn print_store_jobs(jobs: Vec<StoreJob>, format: &OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Table => print_store_jobs_table(jobs),
        OutputFormat::Json => print_json(&jobs),
    }
}

pub fn print_load_job(job: LoadJob, format: &OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Table => {
            println!("{}", "Load job created successfully!".green());
            print_load_jobs_table(vec![job])
        }
        OutputFormat::Json => print_json(&job),
    }
}

pub fn print_store_job(job: StoreJob, format: &OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Table => {
            println!("{}", "Store job created successfully!".green());
            print_store_jobs_table(vec![job])
        }
        OutputFormat::Json => print_json(&job),
    }
}

fn print_load_jobs_table(jobs: Vec<LoadJob>) -> Result<()> {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .apply_modifier(UTF8_SOLID_INNER_BORDERS)
        .set_header(vec![
            Cell::new("ID").fg(Color::Blue),
            Cell::new("STATUS").fg(Color::Blue),
            Cell::new("BUCKET").fg(Color::Blue),
            Cell::new("READ STREAM").fg(Color::Blue),
            Cell::new("WRITE STREAM").fg(Color::Blue),
        ]);

    for job in jobs {
        let status_cell = match job.status {
            LoadJobStatus::Created => Cell::new(job.status.to_string()).fg(Color::Grey),
            LoadJobStatus::Running => Cell::new(job.status.to_string()).fg(Color::Yellow),
            LoadJobStatus::Success => Cell::new(job.status.to_string()).fg(Color::Green),
            LoadJobStatus::Failure => Cell::new(job.status.to_string()).fg(Color::Red),
        };

        table.add_row(vec![
            Cell::new(&job.id),
            status_cell,
            Cell::new(&job.bucket),
            Cell::new(job.prefix.unwrap_or("".to_string())),
            Cell::new(&job.read_stream),
            Cell::new(job.read_consumer.unwrap_or("".to_string())),
            Cell::new(&job.read_subject),
            Cell::new(&job.write_subject),
        ]);
    }

    println!("{table}");
    Ok(())
}

fn print_store_jobs_table(jobs: Vec<StoreJob>) -> Result<()> {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .apply_modifier(UTF8_SOLID_INNER_BORDERS)
        .set_header(vec![
            Cell::new("ID").fg(Color::Blue),
            Cell::new("NAME").fg(Color::Blue),
            Cell::new("STATUS").fg(Color::Blue),
            Cell::new("BUCKET").fg(Color::Blue),
            Cell::new("STREAM").fg(Color::Blue),
            Cell::new("SUBJECT").fg(Color::Blue),
        ]);

    for job in jobs {
        let status_cell = match job.status {
            StoreJobStatus::Created => Cell::new(job.status.to_string()).fg(Color::Grey),
            StoreJobStatus::Running => Cell::new(job.status.to_string()).fg(Color::Yellow),
            StoreJobStatus::Success => Cell::new(job.status.to_string()).fg(Color::Green),
            StoreJobStatus::Failure => Cell::new(job.status.to_string()).fg(Color::Red),
        };

        table.add_row(vec![
            Cell::new(&job.id),
            Cell::new(&job.name),
            status_cell,
            Cell::new(&job.bucket),
            Cell::new(job.prefix.unwrap_or("".to_string())),
            Cell::new(&job.stream),
            Cell::new(job.consumer.unwrap_or("".to_string())),
            Cell::new(&job.subject),
        ]);
    }

    println!("{table}");
    Ok(())
}

fn print_json<T: Serialize>(data: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(data)?);
    Ok(())
}
