# Windows Build Guide

This guide builds the packaged desktop-style launcher for the Charton + Polars + WASM timestamp explorer.

The final executable is:

`target\release\datetime_plot_demo.exe`

It serves the embedded web UI on `127.0.0.1` and can open it in the browser when requested.

Important: the `web` subdirectory is a separate WASM/frontend crate. Building inside `web` does not produce the Windows desktop executable.

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

If you run `cargo build --release` inside `web`, the output will be a frontend crate artifact such as:

`web\target\release\datetime_plot_demo_web.dll`

That is expected, but it is not the desktop executable.

If you change anything in `web/index.html`, `web/src/lib.rs`, or shared Rust rendering/data code used by the web module, rerun this step before rebuilding the native executable.

### 2. Build the native release executable

```powershell
cargo build --release
```

The executable will be:

`target\release\datetime_plot_demo.exe`

This command must be run from the project root, not from `web`.

## Run The App

From the project root:

```powershell
target\release\datetime_plot_demo.exe
```

That starts the local WASM server directly.

To change the bind address manually, run:

```powershell
target\release\datetime_plot_demo.exe serve-wasm
```

To force browser auto-open:

```powershell
target\release\datetime_plot_demo.exe serve-wasm --open
```

Then open this URL manually if needed:

`http://127.0.0.1:8787/`

## Create A Distributable Package

The web assets are embedded into `datetime_plot_demo.exe` at compile time, so you do not need to ship `web\index.html` or `web\pkg\*` separately.

Recommended package contents:

- `datetime_plot_demo.exe`
- `start_datetime_plot_demo.bat` for double-click startup
- optional: `datetime_plot_demo.pdb` for debugging symbols

Example packaging commands from the project root:

```powershell
$dist = "dist\datetime_plot_demo-windows-x86_64"
New-Item -ItemType Directory -Force -Path $dist | Out-Null
Copy-Item "target\release\datetime_plot_demo.exe" "$dist\"
Set-Content -Path "$dist\start_datetime_plot_demo.bat" -Value "@echo off`r`ncd /d `"%~dp0`"`r`nstart `"datetime_plot_demo server`" `"%~dp0datetime_plot_demo.exe`"`r`ntimeout /t 2 /nobreak >nul`r`nstart `"`" http://127.0.0.1:8787/ >nul 2>&1`r`n"
Compress-Archive -Path "$dist\*" -DestinationPath "dist\datetime_plot_demo-windows-x86_64.zip" -Force
```

The resulting zip can be unpacked on another Windows machine and run directly:

```powershell
datetime_plot_demo.exe
```

Or by double-clicking:

`start_datetime_plot_demo.bat`

## Notes

- The native executable embeds the current `web/index.html` and `web/pkg/*` at compile time.
- The `Deployment inventory` panel can export the `trap_info` template workbook (`trap_info_template.xlsx`) directly in the browser, with deployment name and start/end time prefilled.
- Rebuild `web/pkg` first, then rebuild the executable.
- `cargo build --release` from `web` builds the frontend crate only; use the project root to build `datetime_plot_demo.exe`.
- The browser page is local-only; there is no remote backend.
- Large datasets may still make the browser-side WASM app feel heavy. That affects runtime performance, not installation.
- If `wasm-pack` fails specifically during `wasm-opt`, check whether `web/pkg` was already produced. In many cases the bundle is still usable, just larger.

## Minimal Rebuild Command Set

```powershell
cd web
wasm-pack build --release --target web --out-dir pkg
cd ..
cargo build --release
target\release\datetime_plot_demo.exe
```
