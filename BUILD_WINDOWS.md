# Windows Build Guide

This guide builds the packaged desktop-style launcher for the Charton + Polars + WASM timestamp explorer.

The final executable is:

`target\release\datetime_plot_demo.exe`

It serves the embedded web UI on `127.0.0.1` and opens it in the browser when requested.

## What The Binary Does

- Starts a local Rust HTTP server
- Serves the embedded `web/index.html` page
- Serves the generated `web/pkg/*.wasm` and `web/pkg/*.js` assets
- Lets the user upload a local CSV in the browser
- Runs the data preparation and Charton rendering in Rust/WASM

No Python runtime is required for end users.

## Prerequisites

Install these first:

1. Rust stable toolchain with MSVC target
2. LLVM/Clang for Windows, added to `PATH`
3. `wasm-pack`

Recommended install sequence:

### 1. Install Rust

Use `rustup-init.exe` from:

`https://rustup.rs/`

Choose the default stable MSVC toolchain.

After installation, open a new terminal and verify:

```powershell
rustc --version
cargo --version
```

### 2. Install LLVM/Clang

Download LLVM for Windows from:

`https://github.com/llvm/llvm-project/releases`

During installation, enable the option to add LLVM to `PATH`.

Verify:

```powershell
clang --version
```

### 3. Install wasm-pack

```powershell
cargo install wasm-pack
```

Verify:

```powershell
wasm-pack --version
```

## Build Steps

Open PowerShell in the project root.

### 1. Build the WASM frontend bundle

This generates `web/pkg`, which the native executable embeds at compile time.

```powershell
cd web
wasm-pack build --release --target web --out-dir pkg
cd ..
```

If you change anything in `web/index.html`, `web/src/lib.rs`, or shared Rust rendering/data code used by the web module, rerun this step before rebuilding the native executable.

### 2. Build the native release executable

```powershell
cargo build --release
```

The executable will be:

`target\release\datetime_plot_demo.exe`

## Run The App

From the project root:

```powershell
target\release\datetime_plot_demo.exe serve-wasm --open
```

If automatic browser opening does not work, run:

```powershell
target\release\datetime_plot_demo.exe serve-wasm
```

Then open this URL manually:

`http://127.0.0.1:8787/`

## CSV Requirements

The uploaded CSV must contain:

- `deployment`
- `datetime`

The `datetime` format must be:

`yyyy-mm-dd hh:mm:ss`

If a `path` column is present, the detail chart can still separate media types consistently with the native exporter.

## Notes

- The native executable embeds the current `web/index.html`, `web/demo.csv`, and `web/pkg/*` files at compile time.
- Rebuild `web/pkg` first, then rebuild the executable.
- The browser page is local-only; there is no remote backend.
- Large datasets may still make the browser-side WASM app feel heavy. That affects runtime performance, not installation.
- If `wasm-pack` fails specifically during `wasm-opt`, check whether `web/pkg` was already produced. In many cases the bundle is still usable, just larger.

## Minimal Rebuild Command Set

```powershell
cd web
wasm-pack build --release --target web --out-dir pkg
cd ..
cargo build --release
target\release\datetime_plot_demo.exe serve-wasm --open
```
