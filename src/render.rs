use anyhow::Result;
use charton::prelude::*;
use chrono::DateTime;
use polars::prelude::{DataFrame, DataType};

use crate::data::{DeploymentSummary, OverviewBucket, PreparedData};
use crate::util::{escape_html, format_count, format_date};

pub fn overview_chart(data: &PreparedData, bucket: OverviewBucket) -> Result<LayeredChart> {
    overview_chart_from_table(
        data.overview_table(bucket)?,
        bucket,
        &format!("Deployment × {} heatmap", bucket.display_name()),
    )
}

pub fn overview_chart_from_table(
    table: &DataFrame,
    bucket: OverviewBucket,
    title: &str,
) -> Result<LayeredChart> {
    let chart = Chart::build(table)?
        .mark_rect()?
        .encode((x("bucket_label"), y("deployment"), color("event_count")))?
        .configure_rect(|rect| rect.with_stroke("#f7f1e7").with_stroke_width(0.2))
        .with_size(1860, 920)
        .with_title(title)
        .with_x_label(bucket.axis_label())
        .with_y_label("Deployment")
        .configure_theme(|_| {
            base_theme()
                .with_color_map(ColorMap::YlOrRd)
                .with_tick_label_size(11.0)
                .with_x_tick_label_angle(-42.0)
                .with_axis_reserve_buffer(10.0)
                .with_tick_min_spacing(80.0)
                .with_panel_defense_ratio(0.28)
                .with_title_size(22.0)
                .with_legend_title_size(13.0)
                .with_legend_label_size(11.0)
                .with_left_margin(0.13)
                .with_right_margin(0.05)
                .with_bottom_margin(0.16)
                .with_top_margin(0.11)
        });

    Ok(chart)
}

pub fn detail_chart(data: &PreparedData, deployment: &str) -> Result<LayeredChart> {
    detail_chart_from_table(
        data.detail_table(deployment)?,
        deployment,
        data.deployment_summary(deployment)?,
    )
}

pub fn detail_chart_from_table(
    table: &DataFrame,
    _deployment: &str,
    summary: &DeploymentSummary,
) -> Result<LayeredChart> {
    let chart = Chart::build(table)?
        .mark_point()?
        .encode((
            x("timestamp"),
            y("hour_of_day"),
            color("media_type"),
            shape("media_type"),
        ))?
        .configure_point(|point| {
            point
                .with_size(2.7)
                .with_opacity(0.76)
                .with_stroke("#fffaf2")
                .with_stroke_width(0.25)
        })
        .with_size(1640, 560)
        .with_title(format!("Deployment detail: {}", summary.deployment))
        .with_x_label("Timestamp")
        .with_y_label("Hour of day")
        .with_y_domain(0.0, 24.0)
        .configure_theme(|_| {
            base_theme()
                .with_palette(ColorPalette::Dark2)
                .with_x_tick_label_angle(-40.0)
                .with_tick_label_size(11.0)
                .with_tick_min_spacing(90.0)
                .with_title_size(22.0)
                .with_right_margin(0.08)
                .with_left_margin(0.08)
                .with_bottom_margin(0.18)
                .with_top_margin(0.12)
                .with_grid_color("#e3d7c7")
        });

    Ok(chart)
}

pub fn hour_heatmap_chart(data: &PreparedData) -> Result<LayeredChart> {
    hour_heatmap_chart_from_table(&data.hour_heatmap, "Hour-of-day activity by deployment")
}

pub fn hour_heatmap_chart_from_table(table: &DataFrame, title: &str) -> Result<LayeredChart> {
    let chart = Chart::build(table)?
        .mark_rect()?
        .encode((x("hour_label"), y("deployment"), color("event_count")))?
        .configure_rect(|rect| rect.with_stroke("#f7f1e7").with_stroke_width(0.28))
        .with_size(1160, 920)
        .with_title(title)
        .with_x_label("Hour of day")
        .with_y_label("Deployment")
        .configure_theme(|_| {
            base_theme()
                .with_color_map(ColorMap::GnBu)
                .with_tick_label_size(11.0)
                .with_tick_min_spacing(40.0)
                .with_left_margin(0.14)
                .with_bottom_margin(0.12)
                .with_top_margin(0.11)
                .with_right_margin(0.08)
        });

    Ok(chart)
}

pub fn overview_svg(data: &PreparedData, bucket: OverviewBucket) -> Result<String> {
    let svg = overview_chart(data, bucket)?.to_svg()?;
    let svg = replace_zero_fill_with_white(&svg, HEATMAP_ZERO_FILL_YL_OR_RD);
    let svg = thin_rotated_bottom_axis_labels(&svg, target_overview_label_count(bucket));
    annotate_overview_svg(&svg, data.overview_table(bucket)?)
}

pub fn detail_svg(data: &PreparedData, deployment: &str) -> Result<String> {
    let svg = detail_chart(data, deployment)?.to_svg()?;
    let svg = thin_rotated_bottom_axis_labels(&svg, target_detail_label_count());
    annotate_detail_svg(&svg, data.detail_table(deployment)?)
}

pub fn hour_heatmap_svg(data: &PreparedData) -> Result<String> {
    let svg = hour_heatmap_chart(data)?.to_svg()?;
    let svg = replace_zero_fill_with_white(&svg, HEATMAP_ZERO_FILL_GN_BU);
    annotate_hour_heatmap_svg(&svg, &data.hour_heatmap)
}

pub fn detail_caption(data: &PreparedData, deployment: &str) -> Result<String> {
    let summary = data.deployment_summary(deployment)?;
    Ok(detail_caption_from_summary(summary))
}

pub fn resolve_deployment<'a>(
    data: &'a PreparedData,
    requested: Option<&'a str>,
) -> Result<&'a str> {
    match requested {
        Some(value) => {
            data.deployment_summary(value)?;
            Ok(value)
        }
        None => Ok(data.default_deployment()),
    }
}

pub fn detail_caption_from_summary(summary: &DeploymentSummary) -> String {
    format!(
        "{} events from {} through {}. Media types: {}.",
        format_count(summary.event_count),
        format_date(summary.first_seen),
        format_date(summary.last_seen),
        summary.media_breakdown()
    )
}

fn base_theme() -> Theme {
    Theme::default()
        .with_background_color("#fffaf2")
        .with_grid_color("#e7dccd")
        .with_grid_width(0.8)
        .with_title_family(display_font())
        .with_label_family(sans_font())
        .with_tick_label_family(sans_font())
        .with_legend_label_family(sans_font())
        .with_title_color("#12211f")
        .with_label_color("#26403d")
        .with_tick_label_color("#26403d")
        .with_legend_label_color("#26403d")
        .with_palette(ColorPalette::Dark2)
        .with_show_axes(true)
        .with_axis_width(1.0)
        .with_tick_length(5.0)
        .with_label_size(14.0)
        .with_title_size(20.0)
}

fn target_overview_label_count(bucket: OverviewBucket) -> usize {
    match bucket {
        OverviewBucket::Day => 14,
        OverviewBucket::Week => 12,
        OverviewBucket::Month => 8,
    }
}

fn target_detail_label_count() -> usize {
    12
}

const HEATMAP_ZERO_FILL_YL_OR_RD: &str = "rgba(255,255,204,1.000)";
const HEATMAP_ZERO_FILL_GN_BU: &str = "rgba(247,252,240,1.000)";
const HEATMAP_ZERO_FILL_WHITE: &str = "rgba(255,255,255,1.000)";
const PLOT_CLIP_PATH: &str = r#"clip-path="url(#plot-clip-area)""#;

fn thin_rotated_bottom_axis_labels(svg: &str, target_labels: usize) -> String {
    let lines = svg.lines().collect::<Vec<_>>();
    let label_indices = lines
        .iter()
        .enumerate()
        .filter_map(|(index, line)| is_rotated_bottom_tick_label(line).then_some(index))
        .collect::<Vec<_>>();

    if label_indices.len() <= target_labels.max(2) {
        return svg.to_string();
    }

    let stride = ((label_indices.len() as f64) / (target_labels as f64)).ceil() as usize;
    let last_label_position = label_indices.len().saturating_sub(1);

    let mut skip = vec![false; lines.len()];
    for (position, &line_index) in label_indices.iter().enumerate() {
        let keep = position == 0 || position == last_label_position || position % stride == 0;
        if !keep {
            skip[line_index] = true;
            if line_index > 0 && lines[line_index - 1].trim_start().starts_with("<line ") {
                skip[line_index - 1] = true;
            }
        }
    }

    let mut out = String::with_capacity(svg.len());
    for (index, line) in lines.iter().enumerate() {
        if !skip[index] {
            out.push_str(line);
            out.push('\n');
        }
    }

    if !svg.ends_with('\n') {
        out.pop();
    }

    out
}

fn annotate_overview_svg(svg: &str, table: &DataFrame) -> Result<String> {
    let deployments = table.column("deployment")?.str()?;
    let bucket_labels = table.column("bucket_label")?.str()?;
    let event_counts = table.column("event_count")?.i64()?;
    let mut titles = Vec::with_capacity(table.height());

    for index in 0..table.height() {
        let deployment = deployments.get(index).unwrap_or("");
        let bucket_label = bucket_labels.get(index).unwrap_or("");
        let event_count = event_counts.get(index).unwrap_or_default().max(0) as usize;
        titles.push(format!(
            "Deployment: {deployment}\nBucket: {bucket_label}\nEvents: {}",
            format_count(event_count)
        ));
    }

    Ok(inject_plot_titles(svg, &titles, &["rect"]))
}

fn annotate_detail_svg(svg: &str, table: &DataFrame) -> Result<String> {
    let timestamps = table.column("timestamp")?.cast(&DataType::Int64)?;
    let timestamp_values = timestamps.i64()?;
    let hours = table.column("hour_of_day")?.f64()?;
    let media_types = table.column("media_type")?.str()?;
    let mut titles = Vec::with_capacity(table.height());

    for index in 0..table.height() {
        let timestamp_ns = timestamp_values.get(index).unwrap_or_default();
        let hour_of_day = hours.get(index).unwrap_or_default();
        let media_type = media_types.get(index).unwrap_or("unknown");
        titles.push(format!(
            "Timestamp: {}\nHour: {}\nMedia type: {media_type}",
            format_timestamp_ns(timestamp_ns),
            format_hour_label(hour_of_day)
        ));
    }

    Ok(inject_plot_titles(svg, &titles, &["circle", "rect"]))
}

fn annotate_hour_heatmap_svg(svg: &str, table: &DataFrame) -> Result<String> {
    let deployments = table.column("deployment")?.str()?;
    let hour_labels = table.column("hour_label")?.str()?;
    let event_counts = table.column("event_count")?.i64()?;
    let mut titles = Vec::with_capacity(table.height());

    for index in 0..table.height() {
        let deployment = deployments.get(index).unwrap_or("");
        let hour_label = hour_labels.get(index).unwrap_or("");
        let event_count = event_counts.get(index).unwrap_or_default().max(0) as usize;
        titles.push(format!(
            "Deployment: {deployment}\nHour: {hour_label}\nEvents: {}",
            format_count(event_count)
        ));
    }

    Ok(inject_plot_titles(svg, &titles, &["rect"]))
}

fn inject_plot_titles(svg: &str, titles: &[String], allowed_tags: &[&str]) -> String {
    let mut output = String::with_capacity(svg.len() + titles.len() * 72);
    let mut cursor = 0usize;
    let mut last_copied = 0usize;
    let mut title_index = 0usize;

    while let Some(relative_clip_index) = svg[cursor..].find(PLOT_CLIP_PATH) {
        let clip_index = cursor + relative_clip_index;
        let Some(tag_start) = svg[..clip_index].rfind('<') else {
            break;
        };
        let Some(tag_end_relative) = svg[clip_index..].find("/>") else {
            break;
        };
        let tag_end = clip_index + tag_end_relative;
        let tag_body = &svg[tag_start + 1..tag_end];
        let tag_name = tag_body
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_matches('/');

        if !allowed_tags.contains(&tag_name) {
            cursor = tag_end + 2;
            continue;
        }

        output.push_str(&svg[last_copied..tag_start]);

        if let Some(title) = titles.get(title_index) {
            output.push_str(&svg[tag_start..tag_end]);
            output.push('>');
            output.push_str("<title>");
            output.push_str(&escape_html(title));
            output.push_str("</title></");
            output.push_str(tag_name);
            output.push('>');
            title_index += 1;
        } else {
            output.push_str(&svg[tag_start..tag_end + 2]);
        }

        last_copied = tag_end + 2;
        cursor = tag_end + 2;
    }

    output.push_str(&svg[last_copied..]);
    output
}

fn format_timestamp_ns(timestamp_ns: i64) -> String {
    let seconds = timestamp_ns.div_euclid(1_000_000_000);
    let nanos = timestamp_ns.rem_euclid(1_000_000_000) as u32;

    DateTime::from_timestamp(seconds, nanos)
        .map(|value| value.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| timestamp_ns.to_string())
}

fn format_hour_label(hour_of_day: f64) -> String {
    let total_minutes = (hour_of_day * 60.0).round() as i64;
    let hours = total_minutes.div_euclid(60);
    let minutes = total_minutes.rem_euclid(60);
    format!("{hours:02}:{minutes:02}")
}

fn is_rotated_bottom_tick_label(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with("<text ")
        && trimmed.contains("dominant-baseline=\"hanging\"")
        && trimmed.contains("transform=\"rotate(")
}

fn replace_zero_fill_with_white(svg: &str, zero_fill_color: &str) -> String {
    svg.replace(zero_fill_color, HEATMAP_ZERO_FILL_WHITE)
}

fn display_font() -> &'static str {
    "'Iowan Old Style', 'Palatino Linotype', 'Book Antiqua', Georgia, serif"
}

fn sans_font() -> &'static str {
    "'IBM Plex Sans', 'Avenir Next', 'Segoe UI Variable', 'Segoe UI', 'Noto Sans', sans-serif"
}
