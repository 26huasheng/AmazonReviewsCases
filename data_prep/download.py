from __future__ import annotations

import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from .sources import category_source_paths, category_source_urls, validate_category_source

SOURCE_KINDS = ("rating", "metadata", "reviews")


def _headers() -> dict[str, str]:
    token = os.environ.get("HF_TOKEN", "").strip()
    return {"Authorization": f"Bearer {token}"} if token else {}


def probe_url(url: str, timeout: int = 30) -> dict[str, Any]:
    response = requests.head(url, headers=_headers(), allow_redirects=True, timeout=timeout)
    if response.status_code in {403, 405}:
        response.close()
        response = requests.get(
            url, headers={**_headers(), "Range": "bytes=0-0"}, stream=True, timeout=timeout
        )
    try:
        response.raise_for_status()
        size = response.headers.get("Content-Range", "").rsplit("/", 1)[-1]
        if not size.isdigit():
            size = response.headers.get("Content-Length")
        return {
            "url": url,
            "reachable": True,
            "size_bytes": int(size) if size and str(size).isdigit() else None,
            "accept_ranges": response.headers.get("Accept-Ranges", "").lower() == "bytes",
        }
    finally:
        response.close()


def download_file(url: str, destination: Path, retries: int = 4,
                  progress: bool = True) -> dict[str, Any]:
    """Download through a .part file, resuming only when the server confirms HTTP 206."""
    destination = destination.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    part = destination.with_name(destination.name + ".part")
    last_error: Exception | None = None
    started = time.monotonic()
    for attempt in range(1, retries + 1):
        existing = part.stat().st_size if part.exists() else 0
        headers = _headers()
        if existing:
            headers["Range"] = f"bytes={existing}-"
        try:
            with requests.get(url, headers=headers, stream=True,
                              timeout=(30, 120), allow_redirects=True) as response:
                response.raise_for_status()
                append = existing > 0 and response.status_code == 206
                if existing and not append:
                    existing = 0
                length = response.headers.get("Content-Length")
                total = existing + int(length) if length and length.isdigit() else None
                mode = "ab" if append else "wb"
                downloaded = existing
                tick = time.monotonic()
                with part.open(mode) as handle:
                    for chunk in response.iter_content(chunk_size=4 << 20):
                        if not chunk:
                            continue
                        handle.write(chunk)
                        downloaded += len(chunk)
                        now = time.monotonic()
                        if progress and now - tick >= 2:
                            elapsed = max(now - started, 0.001)
                            speed = downloaded / elapsed
                            pct = f"{downloaded / total:6.1%}" if total else "   ?  "
                            eta = f" ETA {(total - downloaded) / speed:,.0f}s" if total and speed else ""
                            print(
                                f"\r{destination.name}: {pct} {downloaded / 1e9:.2f} GB "
                                f"{speed / 1e6:.1f} MB/s{eta}",
                                end="",
                                flush=True,
                            )
                            tick = now
                if total is not None and downloaded != total:
                    raise OSError(f"download size mismatch: got {downloaded}, expected {total}")
                os.replace(part, destination)
                if progress:
                    print()
                return {
                    "url": url,
                    "path": str(destination),
                    "size_bytes": downloaded,
                    "attempts": attempt,
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                }
        except (requests.RequestException, OSError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(min(2 ** attempt, 15))
    raise RuntimeError(f"download failed after {retries} attempts: {last_error}")


def prepare_category_sources(data_root: Path, category: str) -> dict[str, dict[str, str]]:
    """Reuse valid local files; download only missing rating, metadata, and full reviews."""
    paths = category_source_paths(data_root, category)
    urls = category_source_urls(category)
    results: dict[str, dict[str, str]] = {}
    missing: dict[str, Path] = {}
    for kind in SOURCE_KINDS:
        path = paths[kind]
        valid, reason = validate_category_source(kind, path)
        if valid:
            results[kind] = {"status": "existing / verified", "path": str(path)}
        else:
            print(f"{kind}: [{reason}] {path}")
            missing[kind] = path
    if not missing:
        return results
    probes = {kind: probe_url(urls[kind]) for kind in missing}
    required = sum(int(value.get("size_bytes") or 0) for value in probes.values())
    data_root.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(data_root).free
    if required and free < int(required * 1.1):
        raise RuntimeError(f"insufficient disk: need approximately {required}, free {free}")
    for kind, value in probes.items():
        print(f"{kind}: reachable, remote size={value.get('size_bytes')}")
    for kind, path in missing.items():
        download_file(urls[kind], path)
        valid, reason = validate_category_source(kind, path)
        if not valid:
            raise RuntimeError(f"downloaded {kind} failed validation: {reason}")
        results[kind] = {"status": "downloaded", "path": str(path)}
    return results
