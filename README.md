# Deployment Timestamp Visualizer

Media timeline explorer built with `polars`, `charton`, and `wasm-bindgen`.

## Overview

This project ships as a local WASM web app by default. Running the binary starts a local server and opens an interactive browser interface for exploring media timestamp distributions by deployment.

Core views:

- Deployment-over-time overview heatmap
- Single-deployment media detail plot
- Hour-of-day heatmap
- Deployment inventory with `trap_info` template export

## Screenshot

![Deployment Timestamp Visualizer](docs/screenshot.png)

## Default Run Mode

Start the local WASM server:

```bash
cargo run
```

Or explicitly:

```bash
cargo run -- serve-wasm --bind 127.0.0.1:8787
```

## Build

Rebuild the browser bundle:

```bash
cd web
wasm-pack build --release --target web --out-dir pkg
```

Build the native binary:

```bash
cd ..
cargo build --release
```