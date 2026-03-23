use std::collections::{BTreeMap, HashMap};
#[cfg(not(target_arch = "wasm32"))]
use std::fs::File;
use std::io::{Cursor, Read};
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, Timelike};
#[cfg(not(target_arch = "wasm32"))]
use clap::ValueEnum;
use csv::StringRecord;
use polars::prelude::*;

pub const DEFAULT_CSV_PATH: &str =
    "/home/wsyxbcl/scripts/datetime_plot_demo/data/tags_mazev11_xmp-s-m_20260312103320.csv";

#[cfg_attr(not(target_arch = "wasm32"), derive(ValueEnum))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum OverviewBucket {
    Day,
    Week,
    Month,
}

impl OverviewBucket {
    pub const ALL: [OverviewBucket; 3] = [
        OverviewBucket::Day,
        OverviewBucket::Week,
        OverviewBucket::Month,
    ];

    pub fn slug(self) -> &'static str {
        match self {
            OverviewBucket::Day => "day",
            OverviewBucket::Week => "week",
            OverviewBucket::Month => "month",
        }
    }

    pub fn axis_label(self) -> &'static str {
        match self {
            OverviewBucket::Day => "Day",
            OverviewBucket::Week => "Week start",
            OverviewBucket::Month => "Month start",
        }
    }

    pub fn display_name(self) -> &'static str {
        match self {
            OverviewBucket::Day => "daily",
            OverviewBucket::Week => "weekly",
            OverviewBucket::Month => "monthly",
        }
    }
}

#[derive(Clone, Debug)]
pub struct DeploymentSummary {
    pub deployment: String,
    pub order: usize,
    pub event_count: usize,
    pub first_seen: NaiveDateTime,
    pub last_seen: NaiveDateTime,
    pub media_counts: BTreeMap<String, usize>,
}

impl DeploymentSummary {
    pub fn media_breakdown(&self) -> String {
        self.media_counts
            .iter()
            .map(|(kind, count)| format!("{kind} {count}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[derive(Clone)]
pub struct PreparedData {
    pub csv_path: PathBuf,
    pub min_timestamp: NaiveDateTime,
    pub max_timestamp: NaiveDateTime,
    pub deployments: Vec<DeploymentSummary>,
    pub events: DataFrame,
    pub detail_tables: BTreeMap<String, DataFrame>,
    pub overview_tables: BTreeMap<OverviewBucket, DataFrame>,
    pub hour_heatmap: DataFrame,
}

impl PreparedData {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load(csv_path: &Path) -> Result<Self> {
        let file = File::open(csv_path)
            .with_context(|| format!("failed to open CSV at {}", csv_path.display()))?;
        Self::from_reader(file, csv_path.to_path_buf())
    }

    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    pub fn from_csv_text(csv_content: &str) -> Result<Self> {
        Self::from_reader(
            Cursor::new(csv_content.as_bytes()),
            PathBuf::from("uploaded.csv"),
        )
    }

    fn from_reader<R>(reader: R, csv_path: PathBuf) -> Result<Self>
    where
        R: Read,
    {
        let mut reader = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .from_reader(reader);

        let headers = reader
            .headers()
            .context("failed to read CSV headers")?
            .clone();

        let indices = ColumnIndices::from_headers(&headers)?;
        let mut rows = Vec::new();

        for (row_offset, record) in reader.records().enumerate() {
            let record =
                record.with_context(|| format!("failed to read CSV row {}", row_offset + 2))?;
            rows.push(EventRow::from_record(&record, &indices, row_offset + 2)?);
        }

        if rows.is_empty() {
            bail!("the input CSV does not contain any rows");
        }

        let min_timestamp = rows
            .iter()
            .map(|row| row.timestamp)
            .min()
            .expect("rows checked non-empty");
        let max_timestamp = rows
            .iter()
            .map(|row| row.timestamp)
            .max()
            .expect("rows checked non-empty");

        let deployments = summarize_rows(&rows);

        let order_map = deployments
            .iter()
            .map(|deployment| (deployment.deployment.clone(), deployment.order))
            .collect::<HashMap<_, _>>();

        rows.sort_by(|left, right| {
            order_map[&left.deployment]
                .cmp(&order_map[&right.deployment])
                .then_with(|| left.timestamp.cmp(&right.timestamp))
                .then_with(|| left.media_type.cmp(&right.media_type))
        });

        let events = build_events_table(&rows, &order_map)?;
        let detail_tables = build_detail_tables(&rows, &deployments)?;
        let hour_heatmap = build_hour_heatmap(&rows, &deployments, &order_map)?;

        let mut overview_tables = BTreeMap::new();
        for bucket in OverviewBucket::ALL {
            overview_tables.insert(
                bucket,
                build_overview_table(
                    bucket,
                    &rows,
                    &deployments,
                    &order_map,
                    min_timestamp,
                    max_timestamp,
                )?,
            );
        }

        Ok(Self {
            csv_path,
            min_timestamp,
            max_timestamp,
            deployments,
            events,
            detail_tables,
            overview_tables,
            hour_heatmap,
        })
    }

    pub fn default_deployment(&self) -> &str {
        &self.deployments[0].deployment
    }

    pub fn deployment_summary(&self, deployment: &str) -> Result<&DeploymentSummary> {
        self.deployments
            .iter()
            .find(|item| item.deployment == deployment)
            .ok_or_else(|| anyhow!("unknown deployment: {deployment}"))
    }

    pub fn detail_table(&self, deployment: &str) -> Result<&DataFrame> {
        self.detail_tables
            .get(deployment)
            .ok_or_else(|| anyhow!("unknown deployment: {deployment}"))
    }

    pub fn overview_table(&self, bucket: OverviewBucket) -> Result<&DataFrame> {
        self.overview_tables
            .get(&bucket)
            .ok_or_else(|| anyhow!("missing overview table for {}", bucket.slug()))
    }
}

#[derive(Clone, Debug)]
struct EventRow {
    deployment: String,
    timestamp: NaiveDateTime,
    media_type: String,
}

impl EventRow {
    fn from_record(
        record: &StringRecord,
        indices: &ColumnIndices,
        row_number: usize,
    ) -> Result<Self> {
        let deployment = record
            .get(indices.deployment)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("missing deployment at row {row_number}"))?
            .to_string();

        let timestamp_str = record
            .get(indices.datetime)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("missing datetime at row {row_number}"))?;

        let timestamp = NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S")
            .with_context(|| format!("invalid datetime '{timestamp_str}' at row {row_number}"))?;

        let media_type = record
            .get(indices.path)
            .map(extract_media_type)
            .unwrap_or_else(|| "unknown".to_string());

        Ok(Self {
            deployment,
            timestamp,
            media_type,
        })
    }
}

fn summarize_rows(rows: &[EventRow]) -> Vec<DeploymentSummary> {
    let mut stats = HashMap::<String, DeploymentAccumulator>::new();
    for row in rows {
        let entry = stats
            .entry(row.deployment.clone())
            .or_insert_with(|| DeploymentAccumulator::new(row.timestamp));
        entry.record(row.timestamp, &row.media_type);
    }

    let mut deployments = stats
        .into_iter()
        .map(|(deployment, stat)| DeploymentSummary {
            deployment,
            order: 0,
            event_count: stat.event_count,
            first_seen: stat.first_seen,
            last_seen: stat.last_seen,
            media_counts: stat.media_counts,
        })
        .collect::<Vec<_>>();

    deployments.sort_by(|left, right| {
        right
            .event_count
            .cmp(&left.event_count)
            .then_with(|| left.deployment.cmp(&right.deployment))
    });

    for (order, deployment) in deployments.iter_mut().enumerate() {
        deployment.order = order;
    }

    deployments
}

#[derive(Clone, Copy, Debug)]
struct ColumnIndices {
    path: usize,
    deployment: usize,
    datetime: usize,
}

impl ColumnIndices {
    fn from_headers(headers: &StringRecord) -> Result<Self> {
        let path = headers
            .iter()
            .position(|header| header == "path")
            .context("missing required column 'path'")?;
        let deployment = headers
            .iter()
            .position(|header| header == "deployment")
            .context("missing required column 'deployment'")?;
        let datetime = headers
            .iter()
            .position(|header| header == "datetime")
            .context("missing required column 'datetime'")?;

        Ok(Self {
            path,
            deployment,
            datetime,
        })
    }
}

#[derive(Clone, Debug)]
struct DeploymentAccumulator {
    event_count: usize,
    first_seen: NaiveDateTime,
    last_seen: NaiveDateTime,
    media_counts: BTreeMap<String, usize>,
}

impl DeploymentAccumulator {
    fn new(timestamp: NaiveDateTime) -> Self {
        Self {
            event_count: 0,
            first_seen: timestamp,
            last_seen: timestamp,
            media_counts: BTreeMap::new(),
        }
    }

    fn record(&mut self, timestamp: NaiveDateTime, media_type: &str) {
        self.event_count += 1;
        self.first_seen = self.first_seen.min(timestamp);
        self.last_seen = self.last_seen.max(timestamp);
        *self.media_counts.entry(media_type.to_string()).or_insert(0) += 1;
    }
}

fn build_events_table(rows: &[EventRow], order_map: &HashMap<String, usize>) -> Result<DataFrame> {
    let mut deployments = Vec::with_capacity(rows.len());
    let mut deployment_order = Vec::with_capacity(rows.len());
    let mut timestamps = Vec::with_capacity(rows.len());
    let mut hours = Vec::with_capacity(rows.len());
    let mut event_index = Vec::with_capacity(rows.len());
    let mut media_types = Vec::with_capacity(rows.len());

    let mut current_deployment = None::<&str>;
    let mut index_within = 0_i32;

    for row in rows {
        if current_deployment != Some(row.deployment.as_str()) {
            current_deployment = Some(row.deployment.as_str());
            index_within = 0;
        }

        deployments.push(row.deployment.clone());
        deployment_order.push(order_map[&row.deployment] as i32);
        timestamps.push(timestamp_to_ns(row.timestamp)?);
        hours.push(hour_of_day(row.timestamp));
        event_index.push(index_within);
        media_types.push(row.media_type.clone());
        index_within += 1;
    }

    build_event_dataframe(
        deployments,
        deployment_order,
        timestamps,
        hours,
        event_index,
        media_types,
    )
}

fn build_detail_tables(
    rows: &[EventRow],
    deployments: &[DeploymentSummary],
) -> Result<BTreeMap<String, DataFrame>> {
    let mut grouped = BTreeMap::<String, Vec<&EventRow>>::new();
    for row in rows {
        grouped.entry(row.deployment.clone()).or_default().push(row);
    }

    let mut tables = BTreeMap::new();
    for deployment in deployments {
        let group = grouped.get(&deployment.deployment).ok_or_else(|| {
            anyhow!(
                "missing event rows for deployment {}",
                deployment.deployment
            )
        })?;

        let mut deployments_col = Vec::with_capacity(group.len());
        let mut deployment_order = Vec::with_capacity(group.len());
        let mut timestamps = Vec::with_capacity(group.len());
        let mut hours = Vec::with_capacity(group.len());
        let mut event_index = Vec::with_capacity(group.len());
        let mut media_types = Vec::with_capacity(group.len());

        for (index, row) in group.iter().enumerate() {
            deployments_col.push(row.deployment.clone());
            deployment_order.push(deployment.order as i32);
            timestamps.push(timestamp_to_ns(row.timestamp)?);
            hours.push(hour_of_day(row.timestamp));
            event_index.push(index as i32);
            media_types.push(row.media_type.clone());
        }

        tables.insert(
            deployment.deployment.clone(),
            build_event_dataframe(
                deployments_col,
                deployment_order,
                timestamps,
                hours,
                event_index,
                media_types,
            )?,
        );
    }

    Ok(tables)
}

fn build_event_dataframe(
    deployments: Vec<String>,
    deployment_order: Vec<i32>,
    timestamps: Vec<i64>,
    hours: Vec<f64>,
    event_index: Vec<i32>,
    media_types: Vec<String>,
) -> Result<DataFrame> {
    let timestamp_series = Series::new("timestamp".into(), timestamps)
        .cast(&DataType::Datetime(TimeUnit::Nanoseconds, None))
        .context("failed to cast timestamps into Polars Datetime")?;

    DataFrame::new(vec![
        Series::new("deployment".into(), deployments).into(),
        Series::new("deployment_order".into(), deployment_order).into(),
        timestamp_series.into(),
        Series::new("hour_of_day".into(), hours).into(),
        Series::new("event_index".into(), event_index).into(),
        Series::new("media_type".into(), media_types).into(),
    ])
    .context("failed to build events dataframe")
}

fn build_overview_table(
    bucket: OverviewBucket,
    rows: &[EventRow],
    deployments: &[DeploymentSummary],
    order_map: &HashMap<String, usize>,
    min_timestamp: NaiveDateTime,
    max_timestamp: NaiveDateTime,
) -> Result<DataFrame> {
    let bucket_starts = enumerate_buckets(bucket, min_timestamp, max_timestamp);
    let mut counts = HashMap::<(usize, i64), i64>::new();

    for row in rows {
        let deployment_order = order_map[&row.deployment];
        let bucket_start = bucket_floor(bucket, row.timestamp);
        let bucket_ns = timestamp_to_ns(bucket_start)?;
        *counts.entry((deployment_order, bucket_ns)).or_insert(0) += 1;
    }

    let total_cells = deployments.len() * bucket_starts.len();
    let mut deployment_col = Vec::with_capacity(total_cells);
    let mut order_col = Vec::with_capacity(total_cells);
    let mut bucket_ns_col = Vec::with_capacity(total_cells);
    let mut bucket_label_col = Vec::with_capacity(total_cells);
    let mut count_col = Vec::with_capacity(total_cells);

    for deployment in deployments {
        for bucket_start in &bucket_starts {
            let bucket_ns = timestamp_to_ns(*bucket_start)?;
            deployment_col.push(deployment.deployment.clone());
            order_col.push(deployment.order as i32);
            bucket_ns_col.push(bucket_ns);
            bucket_label_col.push(format_bucket_label(bucket, *bucket_start));
            count_col.push(*counts.get(&(deployment.order, bucket_ns)).unwrap_or(&0));
        }
    }

    let bucket_series = Series::new("bucket_start".into(), bucket_ns_col)
        .cast(&DataType::Datetime(TimeUnit::Nanoseconds, None))
        .context("failed to cast overview bucket timestamps")?;

    DataFrame::new(vec![
        Series::new("deployment".into(), deployment_col).into(),
        Series::new("deployment_order".into(), order_col).into(),
        bucket_series.into(),
        Series::new("bucket_label".into(), bucket_label_col).into(),
        Series::new("event_count".into(), count_col).into(),
    ])
    .context("failed to build overview dataframe")
}

fn build_hour_heatmap(
    rows: &[EventRow],
    deployments: &[DeploymentSummary],
    order_map: &HashMap<String, usize>,
) -> Result<DataFrame> {
    let mut counts = HashMap::<(usize, u32), i64>::new();
    for row in rows {
        let deployment_order = order_map[&row.deployment];
        *counts
            .entry((deployment_order, row.timestamp.hour()))
            .or_insert(0) += 1;
    }

    let total_cells = deployments.len() * 24;
    let mut deployment_col = Vec::with_capacity(total_cells);
    let mut order_col = Vec::with_capacity(total_cells);
    let mut hour_value_col = Vec::with_capacity(total_cells);
    let mut hour_label_col = Vec::with_capacity(total_cells);
    let mut count_col = Vec::with_capacity(total_cells);

    for deployment in deployments {
        for hour in 0..24_u32 {
            deployment_col.push(deployment.deployment.clone());
            order_col.push(deployment.order as i32);
            hour_value_col.push(hour as i32);
            hour_label_col.push(format!("{hour:02}:00"));
            count_col.push(*counts.get(&(deployment.order, hour)).unwrap_or(&0));
        }
    }

    DataFrame::new(vec![
        Series::new("deployment".into(), deployment_col).into(),
        Series::new("deployment_order".into(), order_col).into(),
        Series::new("hour".into(), hour_value_col).into(),
        Series::new("hour_label".into(), hour_label_col).into(),
        Series::new("event_count".into(), count_col).into(),
    ])
    .context("failed to build hour heatmap dataframe")
}

fn extract_media_type(path: &str) -> String {
    let trimmed = path.strip_suffix(".xmp").unwrap_or(path);
    let extension = trimmed
        .rsplit_once('.')
        .map(|(_, ext)| ext)
        .unwrap_or("unknown");

    match extension.to_ascii_lowercase().as_str() {
        "jpg" | "jpeg" => "jpeg".to_string(),
        "mp4" => "mp4".to_string(),
        "png" => "png".to_string(),
        "mov" => "mov".to_string(),
        other => other.to_string(),
    }
}

fn bucket_floor(bucket: OverviewBucket, timestamp: NaiveDateTime) -> NaiveDateTime {
    let date = timestamp.date();

    match bucket {
        OverviewBucket::Day => date.and_hms_opt(0, 0, 0).expect("valid midnight"),
        OverviewBucket::Week => {
            let days_from_monday = date.weekday().num_days_from_monday() as i64;
            (date - Duration::days(days_from_monday))
                .and_hms_opt(0, 0, 0)
                .expect("valid monday midnight")
        }
        OverviewBucket::Month => NaiveDate::from_ymd_opt(date.year(), date.month(), 1)
            .expect("valid first day of month")
            .and_hms_opt(0, 0, 0)
            .expect("valid month midnight"),
    }
}

fn enumerate_buckets(
    bucket: OverviewBucket,
    min_timestamp: NaiveDateTime,
    max_timestamp: NaiveDateTime,
) -> Vec<NaiveDateTime> {
    let mut current = bucket_floor(bucket, min_timestamp);
    let end = bucket_floor(bucket, max_timestamp);
    let mut buckets = Vec::new();

    while current <= end {
        buckets.push(current);
        current = next_bucket(bucket, current);
    }

    buckets
}

fn next_bucket(bucket: OverviewBucket, current: NaiveDateTime) -> NaiveDateTime {
    match bucket {
        OverviewBucket::Day => current + Duration::days(1),
        OverviewBucket::Week => current + Duration::weeks(1),
        OverviewBucket::Month => {
            let year = current.date().year();
            let month = current.date().month();
            let (next_year, next_month) = if month == 12 {
                (year + 1, 1)
            } else {
                (year, month + 1)
            };

            NaiveDate::from_ymd_opt(next_year, next_month, 1)
                .expect("valid first day for next month")
                .and_hms_opt(0, 0, 0)
                .expect("valid next month midnight")
        }
    }
}

fn format_bucket_label(bucket: OverviewBucket, value: NaiveDateTime) -> String {
    match bucket {
        OverviewBucket::Day | OverviewBucket::Week => value.format("%Y-%m-%d").to_string(),
        OverviewBucket::Month => value.format("%Y-%m").to_string(),
    }
}

fn timestamp_to_ns(timestamp: NaiveDateTime) -> Result<i64> {
    timestamp
        .and_utc()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("timestamp is out of nanosecond range: {timestamp}"))
}

fn hour_of_day(timestamp: NaiveDateTime) -> f64 {
    timestamp.hour() as f64 + timestamp.minute() as f64 / 60.0 + timestamp.second() as f64 / 3600.0
}
