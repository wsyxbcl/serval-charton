use chrono::NaiveDateTime;

pub fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

pub fn format_timestamp(value: NaiveDateTime) -> String {
    value.format("%Y-%m-%d %H:%M:%S").to_string()
}

pub fn format_date(value: NaiveDateTime) -> String {
    value.format("%Y-%m-%d").to_string()
}

pub fn format_count(value: usize) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);

    for (index, ch) in digits.chars().rev().enumerate() {
        if index > 0 && index % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }

    out.chars().rev().collect()
}

pub fn slugify(value: &str) -> String {
    let mut slug = String::with_capacity(value.len());

    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
        } else if !slug.ends_with('_') {
            slug.push('_');
        }
    }

    slug.trim_matches('_').to_string()
}

pub fn page_styles() -> &'static str {
    r#"
        :root {
            --bg: #f5efe5;
            --bg-deep: #e9e0cf;
            --card: rgba(255, 250, 242, 0.86);
            --card-strong: rgba(255, 247, 236, 0.96);
            --line: rgba(31, 41, 36, 0.12);
            --ink: #172321;
            --muted: #56635f;
            --accent: #0f766e;
            --accent-2: #c2410c;
            --accent-3: #1d4ed8;
            --shadow: 0 18px 46px rgba(29, 28, 22, 0.09);
            --radius: 22px;
            --radius-tight: 16px;
            --font-sans: "IBM Plex Sans", "Avenir Next", "Segoe UI Variable", "Segoe UI", "Noto Sans", sans-serif;
            --font-display: "Iowan Old Style", "Palatino Linotype", "Book Antiqua", Georgia, serif;
        }

        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            color: var(--ink);
            font-family: var(--font-sans);
            background:
                radial-gradient(circle at top right, rgba(15, 118, 110, 0.16), transparent 28%),
                radial-gradient(circle at left 18%, rgba(194, 65, 12, 0.13), transparent 22%),
                linear-gradient(180deg, #faf5ed 0%, var(--bg) 42%, var(--bg-deep) 100%);
        }

        main {
            width: min(1420px, calc(100% - 40px));
            margin: 32px auto 72px;
        }

        .hero {
            background: linear-gradient(135deg, rgba(255, 248, 239, 0.96), rgba(244, 237, 226, 0.9));
            border: 1px solid var(--line);
            border-radius: calc(var(--radius) + 4px);
            box-shadow: var(--shadow);
            padding: 28px 30px;
            position: relative;
            overflow: hidden;
        }

        .hero::after {
            content: "";
            position: absolute;
            inset: auto -80px -80px auto;
            width: 200px;
            height: 200px;
            border-radius: 50%;
            background: radial-gradient(circle, rgba(15, 118, 110, 0.16), transparent 68%);
        }

        .eyebrow {
            display: inline-flex;
            align-items: center;
            gap: 10px;
            color: var(--accent);
            font-size: 0.82rem;
            font-weight: 700;
            letter-spacing: 0.12em;
            text-transform: uppercase;
        }

        .hero h1 {
            margin: 12px 0 10px;
            font-family: var(--font-display);
            font-size: clamp(2rem, 3.5vw, 3.35rem);
            line-height: 1.02;
            font-weight: 700;
        }

        .hero p {
            margin: 0;
            max-width: 70ch;
            color: var(--muted);
            line-height: 1.6;
        }

        .metrics {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 12px;
            margin-top: 22px;
        }

        .metric {
            padding: 14px 16px;
            border-radius: 18px;
            background: rgba(255, 255, 255, 0.56);
            border: 1px solid rgba(31, 41, 36, 0.08);
        }

        .metric .label {
            display: block;
            color: var(--muted);
            font-size: 0.78rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin-bottom: 7px;
        }

        .metric .value {
            font-size: 1.2rem;
            font-weight: 700;
        }

        .stack {
            display: grid;
            gap: 18px;
            margin-top: 20px;
        }

        .grid {
            display: grid;
            gap: 18px;
        }

        .grid.two {
            grid-template-columns: repeat(auto-fit, minmax(380px, 1fr));
        }

        .panel {
            background: var(--card);
            border: 1px solid var(--line);
            border-radius: var(--radius);
            box-shadow: var(--shadow);
            padding: 18px;
            backdrop-filter: blur(10px);
        }

        .panel h2,
        .panel h3 {
            margin: 0 0 8px;
            font-family: var(--font-display);
            font-size: 1.5rem;
        }

        .panel p {
            margin: 0 0 14px;
            color: var(--muted);
            line-height: 1.55;
        }

        .chart-frame {
            background: var(--card-strong);
            border: 1px solid rgba(31, 41, 36, 0.08);
            border-radius: 18px;
            padding: 10px;
            overflow: auto;
        }

        .chart-frame img,
        .chart-frame svg,
        .chart-frame object {
            display: block;
            max-width: 100%;
            height: auto;
        }

        .caption {
            margin-top: 10px;
            color: var(--muted);
            font-size: 0.92rem;
        }

        .chips {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
        }

        .chip {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 9px 12px;
            border-radius: 999px;
            border: 1px solid rgba(15, 118, 110, 0.18);
            background: rgba(15, 118, 110, 0.08);
            color: var(--ink);
            font-size: 0.94rem;
            text-decoration: none;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.94rem;
        }

        th,
        td {
            padding: 10px 12px;
            border-bottom: 1px solid rgba(31, 41, 36, 0.08);
            text-align: left;
            vertical-align: top;
        }

        th {
            color: var(--muted);
            font-size: 0.78rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }

        code,
        .mono {
            font-family: "Iosevka Term", "SFMono-Regular", Consolas, "Liberation Mono", monospace;
        }

        .toolbar {
            display: flex;
            flex-wrap: wrap;
            gap: 12px;
            align-items: end;
        }

        .control {
            display: grid;
            gap: 6px;
            min-width: 220px;
        }

        .control label {
            color: var(--muted);
            font-size: 0.84rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }

        select,
        button {
            appearance: none;
            border: 1px solid rgba(23, 35, 33, 0.14);
            border-radius: 14px;
            background: rgba(255, 255, 255, 0.88);
            color: var(--ink);
            font: inherit;
            padding: 11px 13px;
        }

        button {
            cursor: pointer;
            background: linear-gradient(135deg, rgba(15, 118, 110, 0.18), rgba(29, 78, 216, 0.12));
        }

        .footnote {
            margin-top: 12px;
            color: var(--muted);
            font-size: 0.9rem;
        }

        @media (max-width: 800px) {
            main {
                width: min(100%, calc(100% - 22px));
                margin-top: 18px;
            }

            .hero,
            .panel {
                padding: 16px;
            }
        }
    "#
}
