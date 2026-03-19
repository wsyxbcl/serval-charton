use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

use crate::data::{OverviewBucket, PreparedData};
use crate::render;
use crate::util::{escape_html, format_count, format_date, format_timestamp, page_styles};

pub fn export_report(
    data: &PreparedData,
    out_path: &Path,
    bucket: OverviewBucket,
    focus_deployment: &str,
    top_details: usize,
) -> Result<()> {
    let html = build_report_html(data, bucket, focus_deployment, top_details)?;

    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create report directory {}", parent.display()))?;
    }

    fs::write(out_path, html)
        .with_context(|| format!("failed to write report to {}", out_path.display()))?;

    Ok(())
}

fn build_report_html(
    data: &PreparedData,
    bucket: OverviewBucket,
    focus_deployment: &str,
    top_details: usize,
) -> Result<String> {
    let overview_svg = render::overview_svg(data, bucket)?;
    let hour_svg = render::hour_heatmap_svg(data)?;
    let detail_svg = render::detail_svg(data, focus_deployment)?;
    let detail_caption = render::detail_caption(data, focus_deployment)?;

    let mut details_html = String::new();
    for deployment in data.deployments.iter().take(top_details.max(1)) {
        let svg = render::detail_svg(data, &deployment.deployment)?;
        let caption = render::detail_caption(data, &deployment.deployment)?;
        write!(
            details_html,
            r#"
                <section class="panel">
                    <h3>{}</h3>
                    <p>Detailed event view for one deployment from the prepared event table.</p>
                    <div class="chart-frame">{}</div>
                    <div class="caption">{}</div>
                </section>
            "#,
            escape_html(&deployment.deployment),
            svg,
            escape_html(&caption),
        )?;
    }

    let mut table_rows = String::new();
    for deployment in &data.deployments {
        write!(
            table_rows,
            r#"
                <tr>
                    <td class="mono">{}</td>
                    <td>{}</td>
                    <td>{}</td>
                    <td>{}</td>
                    <td>{}</td>
                </tr>
            "#,
            escape_html(&deployment.deployment),
            format_count(deployment.event_count),
            escape_html(&format_timestamp(deployment.first_seen)),
            escape_html(&format_timestamp(deployment.last_seen)),
            escape_html(&deployment.media_breakdown()),
        )?;
    }

    Ok(format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Timestamp exploration report</title>
    <style>{}</style>
</head>
<body>
    <main>
        <section class="hero">
            <div class="eyebrow">Polars + Charton report</div>
            <h1>Media timestamp distributions by deployment</h1>
            <p>This report reuses one prepared set of Polars tables across a static overview heatmap, a focus deployment event plot, and an hour-of-day comparison view. The overview bucket in this export is <strong>{}</strong>.</p>
            <div class="metrics">
                <div class="metric">
                    <span class="label">CSV source</span>
                    <span class="value mono">{}</span>
                </div>
                <div class="metric">
                    <span class="label">Rows</span>
                    <span class="value">{}</span>
                </div>
                <div class="metric">
                    <span class="label">Deployments</span>
                    <span class="value">{}</span>
                </div>
                <div class="metric">
                    <span class="label">Range</span>
                    <span class="value">{}</span>
                </div>
            </div>
        </section>

        <div class="stack">
            <section class="panel">
                <h2>Overview heatmap</h2>
                <p>Counts are aggregated to {} buckets and laid out across deployments in descending event volume order. The SVG export thins X-axis labels after Charton renders so long ranges stay readable without dropping any heatmap cells.</p>
                <div class="chart-frame">{}</div>
            </section>

            <div class="grid two">
                <section class="panel">
                    <h2>Focus deployment</h2>
                    <p>The event plot uses exact timestamps on the X axis and hour-of-day on the Y axis, with media type mapped to both color and shape.</p>
                    <div class="chart-frame">{}</div>
                    <div class="caption">{}</div>
                </section>

                <section class="panel">
                    <h2>Hour-of-day comparison</h2>
                    <p>This optional heatmap compresses the full time range into a daily activity fingerprint for each deployment.</p>
                    <div class="chart-frame">{}</div>
                </section>
            </div>

            <section class="panel">
                <h2>Deployment inventory</h2>
                <p>The same prepared summaries are also exposed here in tabular form for export and review.</p>
                <div class="chart-frame">
                    <table>
                        <thead>
                            <tr>
                                <th>Deployment</th>
                                <th>Events</th>
                                <th>First seen</th>
                                <th>Last seen</th>
                                <th>Media types</th>
                            </tr>
                        </thead>
                        <tbody>{}</tbody>
                    </table>
                </div>
            </section>

            <section class="panel">
                <h2>Detail gallery</h2>
                <p>These additional detail plots are rendered from the same cached per-deployment tables used by the browser app.</p>
                <div class="stack">{}</div>
            </section>
        </div>
    </main>
</body>
</html>
"#,
        page_styles(),
        bucket.display_name(),
        escape_html(&data.csv_path.display().to_string()),
        format_count(data.events.height()),
        format_count(data.deployments.len()),
        escape_html(&format!(
            "{} to {}",
            format_date(data.min_timestamp),
            format_date(data.max_timestamp)
        )),
        bucket.display_name(),
        overview_svg,
        detail_svg,
        escape_html(&detail_caption),
        hour_svg,
        table_rows,
        details_html,
    ))
}
