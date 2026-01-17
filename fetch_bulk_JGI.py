#!/usr/bin/env python3
"""
Bulk download files from JGI/Phytozome using copied browser API search URLs.

Key features:
- Reads a CSV of (name, search_url). Header is OPTIONAL.
- Filters files by --include / --exclude glob(s) on file_name.
- Optionally keeps ONLY the newest detected Phytozome version per query (--latest-only).
  Robust version detection: recursively scans ALL string fields in each file object
  and looks for "PhytozomeV##" patterns (so it works even if version is only in a
  weird key like "directory/path").
- Restores PURGED files and polls until READY.
- Downloads selected files as zip and unzips.

Usage:
  export JGI_TOKEN="...your session token..."
  python fetch_bulk_jgi.py queries.csv --outdir downloads \
    --include "*.gff3.gz" --include "*.gff.gz" --latest-only
"""

import argparse
import csv
import fnmatch
import json
import os
import re
import sys
import time
import zipfile
from pathlib import Path
from typing import Dict, List, Tuple, Any
import urllib.request

RESTORE_URL = "https://files.jgi.doe.gov/request_archived_files/"
DOWNLOAD_URL = "https://files-download.jgi.doe.gov/download_files/"


# ----------------------------
# HTTP helpers
# ----------------------------
def http_get_json(url: str, headers: Dict[str, str] | None = None) -> dict:
    req = urllib.request.Request(url, headers=headers or {}, method="GET")
    with urllib.request.urlopen(req) as resp:
        data = resp.read().decode("utf-8")
    return json.loads(data)


def http_post_json(url: str, payload: dict, headers: Dict[str, str]) -> dict:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req) as resp:
        data = resp.read().decode("utf-8")
    return json.loads(data)


def http_post_download_zip(url: str, payload: dict, headers: Dict[str, str], out_zip: Path) -> None:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req) as resp:
        with open(out_zip, "wb") as f:
            while True:
                chunk = resp.read(1024 * 1024)  # 1MB
                if not chunk:
                    break
                f.write(chunk)


# ----------------------------
# CSV reader (header optional)
# ----------------------------
def read_queries_csv(csv_path: Path) -> List[Tuple[str, str]]:
    """
    Accepts either:
      - header CSV with columns name,search_url (or name,url)
      - headerless CSV where each row is: name,search_url
    """
    rows: List[Tuple[str, str]] = []
    with open(csv_path, newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except csv.Error:
            dialect = csv.excel

        reader = csv.reader(f, dialect)
        first = next(reader, None)
        if first is None:
            raise ValueError("CSV is empty.")

        first_lower = [c.strip().lower() for c in first]
        has_header = (
            ("name" in first_lower and ("search_url" in first_lower or "url" in first_lower))
            or (len(first_lower) >= 2 and first_lower[0] == "name" and first_lower[1] in {"search_url", "url"})
        )

        if has_header:
            f.seek(0)
            dr = csv.DictReader(f, dialect=dialect)
            for r in dr:
                name = (r.get("name") or "").strip()
                url = (r.get("search_url") or r.get("url") or "").strip()
                if name and url:
                    rows.append((name, url))
        else:
            def add_row(r: List[str]):
                if len(r) < 2:
                    return
                name = (r[0] or "").strip()
                url = (r[1] or "").strip()
                if name and url:
                    rows.append((name, url))

            add_row(first)
            for r in reader:
                add_row(r)

    if not rows:
        raise ValueError("No valid rows found. Provide rows as: name,search_url (header optional).")
    return rows


# ----------------------------
# Filtering + version detection
# ----------------------------
def matches_any_glob(filename: str, globs: List[str]) -> bool:
    return any(fnmatch.fnmatch(filename, g) for g in globs)


# Detect Phytozome version from strings like:
#   Phytozome/PhytozomeV13/...
#   phytozomev13
PHYTOZOME_VER_PATTERNS = [
    re.compile(r"phytozomev\s*([0-9]{1,2})", re.IGNORECASE),
    re.compile(r"phytozome\s*[_/-]*\s*v\s*([0-9]{1,2})", re.IGNORECASE),
    re.compile(r"phytozome\s*([0-9]{1,2})", re.IGNORECASE),
]


def extract_phytozome_version(text: str) -> int | None:
    s = text or ""
    for pat in PHYTOZOME_VER_PATTERNS:
        m = pat.search(s)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                return None
    return None


def collect_strings(obj: Any, out: List[str], max_strings: int = 2000) -> None:
    """
    Recursively collect string values from nested dict/list structures.
    Safeguards against huge objects via max_strings.
    """
    if len(out) >= max_strings:
        return
    if isinstance(obj, str):
        out.append(obj)
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            if len(out) >= max_strings:
                break
            # include keys too (sometimes version is encoded in keys)
            if isinstance(k, str):
                out.append(k)
            collect_strings(v, out, max_strings=max_strings)
        return
    if isinstance(obj, list):
        for v in obj:
            if len(out) >= max_strings:
                break
            collect_strings(v, out, max_strings=max_strings)
        return
    # ignore other types


def build_version_haystack(file_obj: dict) -> str:
    """
    Build a robust haystack for version detection:
    - Always include file_name
    - Recursively collect strings from the entire file object
    - Prefer strings containing 'phytozome' (to avoid unrelated numbers)
    """
    strings: List[str] = []
    fname = file_obj.get("file_name") or ""
    if fname:
        strings.append(fname)

    collect_strings(file_obj, strings)

    # keep only strings that mention phytozome (reduces false matches)
    phytozome_strings = [s for s in strings if isinstance(s, str) and "phytozome" in s.lower()]

    # If nothing mentions phytozome, fall back to the full string list (still includes fname)
    chosen = phytozome_strings if phytozome_strings else strings

    # Join, but cap length to avoid pathological payloads
    haystack = " ".join(chosen)
    return haystack[:200000]


def parse_search(
    search_json: dict,
    include_globs: List[str],
    exclude_globs: List[str],
) -> Tuple[List[dict], Dict[str, List[str]], Dict[str, List[str]]]:
    manifest_rows: List[dict] = []
    selected_ids: Dict[str, List[str]] = {}
    selected_purged: Dict[str, List[str]] = {}

    organisms = search_json.get("organisms", []) or []
    for org in organisms:
        dataset_id = org.get("id")
        if not dataset_id:
            continue

        files = org.get("files", []) or []
        for f in files:
            fid = f.get("_id")
            fname = f.get("file_name") or ""
            status = (f.get("file_status") or "").upper()

            if not fid or not fname:
                continue

            # include/exclude selection (based on file_name)
            selected = True
            if include_globs:
                selected = matches_any_glob(fname, include_globs)
            if selected and exclude_globs:
                if matches_any_glob(fname, exclude_globs):
                    selected = False

            # detect version robustly from the whole file object
            haystack = build_version_haystack(f)
            pver = extract_phytozome_version(haystack)

            manifest_rows.append(
                {
                    "dataset_id": dataset_id,
                    "file_id": fid,
                    "file_name": fname,
                    "file_status": status,
                    "phytozome_version": pver if pver is not None else "",
                    "selected": selected,
                }
            )

            if not selected:
                continue

            selected_ids.setdefault(dataset_id, []).append(fid)
            if status == "PURGED":
                selected_purged.setdefault(dataset_id, []).append(fid)

    # Deduplicate IDs while preserving order
    def dedup(xs: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for x in xs:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    selected_ids = {k: dedup(v) for k, v in selected_ids.items()}
    selected_purged = {k: dedup(v) for k, v in selected_purged.items()}
    return manifest_rows, selected_ids, selected_purged


def apply_latest_only(manifest_rows: List[dict]) -> None:
    """
    Keep only the newest detected Phytozome version among currently selected files.

    Strict behavior:
    - If we detect at least one version among selected files,
      DESELECT all selected rows that have:
        - a different version, OR
        - no detectable version
    """
    selected_versions = [
        int(r["phytozome_version"])
        for r in manifest_rows
        if r.get("selected") and str(r.get("phytozome_version", "")).strip() != ""
    ]
    if not selected_versions:
        return

    max_ver = max(selected_versions)

    for r in manifest_rows:
        if not r.get("selected"):
            continue
        v = str(r.get("phytozome_version", "")).strip()
        if v == "":
            r["selected"] = False
            continue
        if int(v) != max_ver:
            r["selected"] = False


def rebuild_selected_maps(manifest_rows: List[dict]) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    selected_ids: Dict[str, List[str]] = {}
    selected_purged: Dict[str, List[str]] = {}

    for r in manifest_rows:
        if not r.get("selected"):
            continue
        ds = r["dataset_id"]
        fid = r["file_id"]
        selected_ids.setdefault(ds, []).append(fid)
        if r.get("file_status") == "PURGED":
            selected_purged.setdefault(ds, []).append(fid)

    def dedup(xs: List[str]) -> List[str]:
        seen = set()
        out = []
        for x in xs:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    selected_ids = {k: dedup(v) for k, v in selected_ids.items()}
    selected_purged = {k: dedup(v) for k, v in selected_purged.items()}
    return selected_ids, selected_purged


def write_manifest_tsv(manifest_rows: List[dict], out_path: Path) -> None:
    cols = ["dataset_id", "file_id", "file_name", "file_status", "phytozome_version", "selected"]
    with open(out_path, "w") as f:
        f.write("\t".join(cols) + "\n")
        for r in manifest_rows:
            f.write("\t".join(str(r.get(c, "")) for c in cols) + "\n")


# ----------------------------
# Restore + download
# ----------------------------
def ensure_restored(
    token: str,
    purged_by_dataset: Dict[str, List[str]],
    poll_seconds: int,
    max_wait_seconds: int,
) -> None:
    if not purged_by_dataset:
        print("  No PURGED files selected. Skipping restore.")
        return

    headers = {
        "accept": "application/json",
        "Authorization": token,
        "Content-Type": "application/json",
    }

    restore_payload = {
        "ids": {ds: {"file_ids": fids} for ds, fids in purged_by_dataset.items()},
        "send_mail": False,
        "api_version": "2",
    }

    n_purged = sum(len(v) for v in purged_by_dataset.values())
    print(f"  Requesting restore for {n_purged} PURGED file(s)...")
    resp = http_post_json(RESTORE_URL, restore_payload, headers=headers)

    status_url = resp.get("request_status_url") or resp.get("requestStatusUrl")
    if not status_url:
        raise RuntimeError(f"Restore response missing status URL. Response keys: {list(resp.keys())}")

    print(f"  Polling restore status: {status_url}")
    waited = 0
    while True:
        status_resp = http_get_json(status_url, headers={"Authorization": token, "accept": "application/json"})
        status = (status_resp.get("status") or "").upper()
        print(f"   - {time.strftime('%Y-%m-%d %H:%M:%S')} status={status}")

        if status == "READY":
            print("  Restore READY.")
            return
        if status == "EXPIRED":
            raise RuntimeError("Restore request EXPIRED (re-run restore).")

        time.sleep(poll_seconds)
        waited += poll_seconds
        if waited >= max_wait_seconds:
            raise TimeoutError(f"Timed out waiting for restore after {max_wait_seconds} seconds.")


def download_selected(token: str, selected_ids_by_dataset: Dict[str, List[str]], out_zip: Path) -> None:
    headers = {
        "accept": "application/json",
        "Authorization": token,
        "Content-Type": "application/json",
    }

    download_payload = {
        "ids": {ds: fids for ds, fids in selected_ids_by_dataset.items()},
        "api_version": "2",
    }

    print(f"  Downloading zip -> {out_zip}")
    http_post_download_zip(DOWNLOAD_URL, download_payload, headers=headers, out_zip=out_zip)


def unzip(zip_path: Path, out_dir: Path) -> None:
    print(f"  Unzipping into: {out_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(out_dir)


# ----------------------------
# Main
# ----------------------------
def main():
    ap = argparse.ArgumentParser(description="Bulk fetch JGI/Phytozome files from API search URLs (CSV).")
    ap.add_argument("csv", type=str, help="CSV rows: name,search_url (header optional)")
    ap.add_argument("--outdir", type=str, default="jgi_downloads", help="Output directory")
    ap.add_argument("--poll-seconds", type=int, default=600, help="Seconds between restore-status polls")
    ap.add_argument("--max-wait-seconds", type=int, default=6 * 3600, help="Max seconds to wait for restore")
    ap.add_argument("--keep-zip", action="store_true", help="Keep downloaded zip files")
    ap.add_argument(
        "--include",
        action="append",
        default=[],
        help='Glob to include by file_name (repeatable). Example: --include "*.gff3.gz"',
    )
    ap.add_argument(
        "--exclude",
        action="append",
        default=[],
        help='Glob to exclude by file_name (repeatable). Example: --exclude "*softmasked*"',
    )
    ap.add_argument(
        "--latest-only",
        action="store_true",
        help="After include/exclude filtering, keep only the newest detected Phytozome version (robust scan).",
    )
    args = ap.parse_args()

    token = os.environ.get("JGI_TOKEN", "").strip()
    if not token:
        print("ERROR: Please set JGI_TOKEN in your environment.", file=sys.stderr)
        print('Example: export JGI_TOKEN="PASTE_TOKEN_HERE"', file=sys.stderr)
        sys.exit(2)

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    queries = read_queries_csv(Path(args.csv))
    print(f"Loaded {len(queries)} querie(s) from {args.csv}")

    if args.include:
        print(f"Include globs: {args.include}")
    else:
        print("Include globs: (none) -> will download ALL files returned by each query")
    if args.exclude:
        print(f"Exclude globs: {args.exclude}")
    if args.latest_only:
        print("Latest-only mode: ON (newest detected Phytozome version per query)")

    for name, url in queries:
        print(f"\n==> {name}")
        qdir = outdir / name
        qdir.mkdir(parents=True, exist_ok=True)

        # 1) fetch search JSON
        print("  Fetching search JSON...")
        search_json = http_get_json(url, headers={"accept": "application/json"})
        (qdir / "search.json").write_text(json.dumps(search_json, indent=2))

        # 2) parse + initial filtering
        manifest_rows, selected_ids_by_ds, selected_purged_by_ds = parse_search(
            search_json,
            include_globs=args.include,
            exclude_globs=args.exclude,
        )

        # 3) latest-only refinement
        if args.latest_only:
            apply_latest_only(manifest_rows)
            selected_ids_by_ds, selected_purged_by_ds = rebuild_selected_maps(manifest_rows)

        # write manifest after all selection logic
        write_manifest_tsv(manifest_rows, qdir / "manifest.tsv")

        n_seen = len(manifest_rows)
        n_selected = sum(1 for r in manifest_rows if r["selected"])
        selected_versions = sorted(
            {int(r["phytozome_version"]) for r in manifest_rows if r["selected"] and str(r["phytozome_version"]).strip() != ""}
        )
        n_purged = sum(1 for r in manifest_rows if r["selected"] and r["file_status"] == "PURGED")
        n_datasets = len(selected_ids_by_ds)

        print(f"  Saw {n_seen} file(s) in results.")
        print(f"  Selected {n_selected} file(s) after filtering across {n_datasets} dataset(s).")
        if args.latest_only:
            if selected_versions:
                print(f"  Selected Phytozome version(s) (detected): {selected_versions} (kept newest)")
            else:
                print("  WARNING: Could not detect Phytozome versions in selected files; latest-only had no effect.")
        print(f"  Selected PURGED: {n_purged}")

        if n_selected == 0:
            print("  No files selected for this query (check globs or latest-only behavior). Skipping.")
            continue

        # 4) restore purged
        ensure_restored(token, selected_purged_by_ds, args.poll_seconds, args.max_wait_seconds)

        # 5) download + unzip
        zip_path = qdir / f"{name}.zip"
        download_selected(token, selected_ids_by_ds, zip_path)
        unzip(zip_path, qdir / "files")

        if not args.keep_zip:
            try:
                zip_path.unlink()
            except FileNotFoundError:
                pass

    print("\nDone.")


if __name__ == "__main__":
    main()
