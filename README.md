# Datetime Plot Demo

Rust exploration project for static media timestamp distributions using `polars` for prepared data tables and `charton` for rendering.

## What it builds

- Overview deployment × time heatmap
- Single-deployment detail event plot
- Hour-of-day heatmap
- Pure-Rust static file export
- Pure-Rust report-style HTML export
- Browser-side WASM explorer

Both export modes reuse the same prepared tables built once from the source CSV.

## Default dataset

The CLI defaults to:

`/home/wsyxbcl/scripts/datetime_plot_demo/data/tags_mazev11_xmp-s-m_20260312103320.csv`

## Commands

Minimal static export:

```bash
cargo run -- export-static --overview-bucket week --format svg
```

Static export with all deployment details:

```bash
cargo run -- export-static --all-details --format png
```

Report export:

```bash
cargo run -- export-report --overview-bucket month --top-details 6
```

## Structure

- `src/data.rs`: CSV parsing and reusable Polars table preparation
- `src/render.rs`: Charton chart construction plus lightweight SVG post-processing
- `src/report.rs`: report-style HTML export
- `src/web_app.rs`: local HTTP wrapper that serves the embedded WASM explorer
- `src/main.rs`: CLI entry point
- `web/`: `wasm-bindgen` browser app that reuses the prepared-table and SVG rendering code

## Notes

- Overview buckets support `day`, `week`, and `month`.
- SVG-based outputs thin crowded rotated date labels after Charton renders, so both the overview heatmap and detail event plot stay readable in `export-static --format svg` and the HTML report.
- The detail plot uses days on the X axis and minute-of-day on the Y axis, while hover still shows the exact timestamp and file path.
- Heatmap cells with `0` events are forced to white in SVG output so empty regions stand out immediately.
- The detail plot groups media into `image` and `video` from the CSV `media_type` column when present, with a fallback to `path` inference for older CSVs.
- The current tool intentionally stays on a pure-Rust path: static export, report export, and a local `serve-wasm` preview app.

## Experimental WASM Demo

The `web/` subproject compiles Charton + Polars + the existing data preparation/rendering modules to WebAssembly. It lets a user upload a CSV in the browser and rerender the overview, detail, and hour heatmap entirely inside Rust/WASM.

Rebuild the WASM package:

```bash
cd web
wasm-pack build --release --target web --out-dir pkg
```

Then preview through the native local server:

```bash
cargo run -- serve-wasm
```

Then open `http://127.0.0.1:8787/`.

Notes:

- The browser demo expects `path`, `deployment`, and `datetime`, with datetimes in `yyyy-mm-dd hh:mm:ss` format. A `media_type` column is preferred and used for image/video classification.
- The "Load bundled demo CSV" button served by `cargo run -- serve-wasm` now uses the main dataset under `data/`.
- The current WASM package was built successfully on this machine after trimming the browser-side `polars` feature set and keeping `dtype-categorical` enabled for Charton compatibility.
- The optimized `web/pkg/datetime_plot_demo_web_bg.wasm` is currently about `8.7 MB`, so this is a viable experiment path but still heavier than the static native CLI.
