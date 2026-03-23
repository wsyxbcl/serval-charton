use std::borrow::Cow;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::Command;

use anyhow::{Context, Result};

const INDEX_HTML: &[u8] = include_bytes!("../web/index.html");
const DEMO_CSV: &[u8] = include_bytes!("../web/demo.csv");
const PKG_WASM: &[u8] = include_bytes!("../web/pkg/datetime_plot_demo_web_bg.wasm");
const PKG_JS: &[u8] = include_bytes!("../web/pkg/datetime_plot_demo_web.js");
const PKG_WASM_D_TS: &[u8] = include_bytes!("../web/pkg/datetime_plot_demo_web_bg.wasm.d.ts");
const PKG_D_TS: &[u8] = include_bytes!("../web/pkg/datetime_plot_demo_web.d.ts");
const PKG_PACKAGE_JSON: &[u8] = include_bytes!("../web/pkg/package.json");

pub fn serve(bind: SocketAddr, open_browser: bool) -> Result<()> {
    let listener = TcpListener::bind(bind)
        .with_context(|| format!("failed to bind local web server on {bind}"))?;
    let local_addr = listener
        .local_addr()
        .context("failed to determine local web server address")?;
    let url = format!("http://{local_addr}/");

    println!("WASM explorer available at {url}");
    println!("Press Ctrl-C to stop the local server.");

    if open_browser {
        if let Err(error) = try_open_browser(&url) {
            eprintln!("warning: failed to open browser automatically: {error}");
        }
    }

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                if let Err(error) = handle_connection(stream) {
                    eprintln!("request handling error: {error:#}");
                }
            }
            Err(error) => {
                eprintln!("incoming connection error: {error}");
            }
        }
    }

    Ok(())
}

fn handle_connection(mut stream: TcpStream) -> Result<()> {
    let mut request_line = String::new();
    {
        let mut reader = BufReader::new(&mut stream);
        reader
            .read_line(&mut request_line)
            .context("failed to read request line")?;
    }

    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let raw_path = parts.next().unwrap_or("/");
    let path = raw_path.split('?').next().unwrap_or("/");

    let response = match method {
        "GET" | "HEAD" => route(path),
        _ => Response::method_not_allowed(),
    };

    write_response(&mut stream, method == "HEAD", response)
}

fn route(path: &str) -> Response<'_> {
    match path {
        "/" | "/index.html" => Response::ok("text/html; charset=utf-8", INDEX_HTML),
        "/demo.csv" => Response::ok("text/csv; charset=utf-8", DEMO_CSV),
        "/pkg/datetime_plot_demo_web_bg.wasm" => Response::ok("application/wasm", PKG_WASM),
        "/pkg/datetime_plot_demo_web.js" => Response::ok("text/javascript; charset=utf-8", PKG_JS),
        "/pkg/datetime_plot_demo_web_bg.wasm.d.ts" => {
            Response::ok("text/plain; charset=utf-8", PKG_WASM_D_TS)
        }
        "/pkg/datetime_plot_demo_web.d.ts" => Response::ok("text/plain; charset=utf-8", PKG_D_TS),
        "/pkg/package.json" => Response::ok("application/json; charset=utf-8", PKG_PACKAGE_JSON),
        _ => Response::not_found(),
    }
}

fn write_response(stream: &mut TcpStream, head_only: bool, response: Response<'_>) -> Result<()> {
    let status_text = match response.status_code {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "OK",
    };

    write!(
        stream,
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        response.status_code,
        status_text,
        response.content_type,
        response.body.len()
    )
    .context("failed to write response headers")?;

    if !head_only {
        stream
            .write_all(&response.body)
            .context("failed to write response body")?;
    }

    stream.flush().context("failed to flush response")
}

fn try_open_browser(url: &str) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        Command::new("open")
            .arg(url)
            .status()
            .context("failed to spawn macOS browser opener")?;
        return Ok(());
    }

    #[cfg(target_os = "windows")]
    {
        Command::new("cmd")
            .args(["/C", "start", "", url])
            .status()
            .context("failed to spawn Windows browser opener")?;
        return Ok(());
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    {
        Command::new("xdg-open")
            .arg(url)
            .status()
            .context("failed to spawn Linux browser opener")?;
        return Ok(());
    }

    #[allow(unreachable_code)]
    Ok(())
}

struct Response<'a> {
    status_code: u16,
    content_type: &'a str,
    body: Cow<'a, [u8]>,
}

impl<'a> Response<'a> {
    fn ok(content_type: &'a str, body: &'a [u8]) -> Self {
        Self {
            status_code: 200,
            content_type,
            body: Cow::Borrowed(body),
        }
    }

    fn not_found() -> Self {
        Self {
            status_code: 404,
            content_type: "text/plain; charset=utf-8",
            body: Cow::Borrowed(b"404 not found"),
        }
    }

    fn method_not_allowed() -> Self {
        Self {
            status_code: 405,
            content_type: "text/plain; charset=utf-8",
            body: Cow::Borrowed(b"405 method not allowed"),
        }
    }
}
