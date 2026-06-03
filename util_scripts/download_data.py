"""Download a file from a data repository (Zenodo, Figshare, etc.).

Why this exists: naive ``curl``/``wget`` calls fail on these repositories for
two unrelated reasons:

1. Zenodo file links carry a ``?download=1`` query string. When the URL is not
   quoted on the command line the shell tries to interpret ``?`` and ``=`` as
   glob/assignment syntax, so the download silently breaks. Driving the request
   from Python sidesteps the shell entirely, so the query string is preserved.

2. Figshare ``ndownloader`` links are commonly copied in the form
   ``https://figshare.com/ndownloader/files/<id>`` (or
   ``.../articles/<id>/versions/<n>``). That host never serves the file -- it
   returns ``202 Accepted`` with an empty body indefinitely, which looks like a
   redirect/bot-prevention hang. The actual download host is
   ``ndownloader.figshare.com``; rewriting the host there yields a ``302`` to
   the final S3 object. A browser-like ``User-Agent`` is also sent.

Usage:
    python download_data.py <url> [-o OUTPUT_DIR] [-n FILENAME]

Examples:
    python download_data.py \\
        "https://zenodo.org/records/19038479/files/convergence_summary.zip?download=1"
    python download_data.py \\
        "https://figshare.com/ndownloader/articles/31141138/versions/1" -n dataset.zip
"""

import argparse
import os
import re
import sys
from urllib.parse import unquote, urlsplit

from pathlib import Path
import requests

# A realistic browser User-Agent is required: Figshare's ndownloader endpoints
# reject requests from default library agents as suspected bots.
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 " "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
}

CHUNK_SIZE = 1 << 20  # 1 MiB


def normalize_url(url):
    """Fix repository URLs that won't download as-is.

    Figshare ndownloader links pasted with the ``figshare.com`` host hang on a
    202; the working host is ``ndownloader.figshare.com``. Rewrite it so both
    ``/files/<id>`` and ``/articles/<id>/versions/<n>`` paths resolve.
    """
    parts = urlsplit(url)
    if parts.netloc == "figshare.com" and parts.path.startswith("/ndownloader/"):
        new_path = parts.path[len("/ndownloader") :]  # drop leading /ndownloader
        fixed = parts._replace(netloc="ndownloader.figshare.com", path=new_path)
        return fixed.geturl()
    return url


def resolve_filename(response, url, override=None):
    """Determine the on-disk filename for a download.

    Preference order: explicit override, the server's Content-Disposition
    header, then the last path segment of the final (post-redirect) URL.
    Figshare article links have no meaningful path segment, so the header is
    what yields a sensible name in that case.
    """
    if override:
        return override

    disposition = response.headers.get("Content-Disposition", "")
    # Match filename*=UTF-8''name (RFC 5987) first, then plain filename="name".
    match = re.search(r"filename\*=(?:UTF-8'')?\"?([^\";]+)", disposition, re.IGNORECASE)
    if not match:
        match = re.search(r'filename="?([^";]+)"?', disposition, re.IGNORECASE)
    if match:
        return unquote(match.group(1)).strip()

    path_name = os.path.basename(urlsplit(response.url or url).path)
    if path_name:
        return unquote(path_name)

    # Last resort for opaque endpoints (e.g. Figshare article downloads).
    return "download.bin"


BASE_DIR = "/home/gpwolfe/colabfit/vast_ingest"


def download(url, output_dir=None, filename=None):
    # Preferred: an explicit dataset directory under BASE_DIR holding all of a
    # record's files under their real (server-resolved) names. Legacy fallback:
    # derive a per-file directory from the override filename's stem.
    if output_dir:
        output_dir = os.path.join(BASE_DIR, output_dir)
    elif filename:
        output_dir = os.path.join(BASE_DIR, Path(filename).stem)
    else:
        output_dir = BASE_DIR
    os.makedirs(output_dir, exist_ok=True)

    fixed_url = normalize_url(url)
    if fixed_url != url:
        print(f"Rewrote Figshare host: {url}\n              -> {fixed_url}")
        url = fixed_url

    try:
        # stream=True defers the body; allow_redirects (default True) walks the
        # Figshare -> S3 redirect chain. The query string is sent verbatim, so
        # Zenodo's ?download=1 survives.
        response = requests.get(url, headers=BROWSER_HEADERS, stream=True, allow_redirects=True, timeout=60)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to start download: {e}", file=sys.stderr)
        sys.exit(1)

    name = resolve_filename(response, url, filename)
    target_path = os.path.join(output_dir, name)

    total = response.headers.get("Content-Length")
    total = int(total) if total and total.isdigit() else None

    print(f"Downloading {url}")
    if response.url != url:
        print(f"  -> resolved to {response.url}")
    print(f"  -> {target_path}" + (f" ({total / 1e6:.1f} MB)" if total else ""))

    downloaded = 0
    last_report = 0
    try:
        with open(target_path, "wb") as fh:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if not chunk:
                    continue
                fh.write(chunk)
                downloaded += len(chunk)
                # Throttle progress: redraw at most every ~32 MiB so large
                # downloads don't flood the terminal with a line per chunk.
                if downloaded - last_report >= 32 * CHUNK_SIZE:
                    last_report = downloaded
                    if total:
                        pct = downloaded / total * 100
                        print(f"\r  {downloaded / 1e6:8.1f} MB ({pct:5.1f}%)", end="", flush=True)
                    else:
                        print(f"\r  {downloaded / 1e6:8.1f} MB", end="", flush=True)
            print()
    except (requests.RequestException, OSError) as e:
        print(f"\nDownload failed: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Saved: {target_path} ({downloaded / 1e6:.1f} MB)")
    return target_path


def main():
    parser = argparse.ArgumentParser(description="Download a file from Zenodo, Figshare, or similar repositories.")
    parser.add_argument("url", help="Direct download URL (quote it in the shell).")
    parser.add_argument(
        "-o",
        "--output-dir",
        default=None,
        help="Dataset directory name under vast_ingest to save files into (real filenames preserved).",
    )
    parser.add_argument(
        "-n",
        "--name",
        default=None,
        help="Override the output filename (useful for opaque Figshare links).",
    )
    args = parser.parse_args()
    download(args.url, args.output_dir, args.name)


if __name__ == "__main__":
    main()
