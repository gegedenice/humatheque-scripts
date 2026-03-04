#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "requests",
#   "pandas",
#   "PyPDF2",
#   "pdf2image",
#   "minio",
# ]
# ///
"""
Thesis PDF -> images -> MinIO uploader (theses.fr)

Reads a CSV containing an 'oai_id' column (thesis identifiers),
downloads the thesis PDF from theses.fr/{id}/abes, converts first N pages
to PNG images, and uploads them to a MinIO bucket, under a subfolder.

Dependencies
  PyPDF2 needs Poppler utilities: apt-get install -y poppler-utils

Example:
  uv run humatheque_theses_to_minio.py \
    --csv path/_sample_filtered_humatheque_theses_diffusable_openaccess_flat.csv \
    --bucket-subfolder theses/theses.fr \
    --num-pages 10 \
    --batch-size 25

Env config:
  MINIO_ENDPOINT (default: localhost:9000)
  MINIO_ACCESS_KEY (default: minioadmin)
  MINIO_SECRET_KEY (default: minioadmin)
  MINIO_BUCKET (default: theses)
  MINIO_SECURE (default: false)
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from io import BytesIO
from pathlib import Path
from typing import List, Optional, Iterable

import pandas as pd
import requests
from PyPDF2 import PdfReader
from pdf2image import convert_from_path

from minio import Minio
from minio.error import S3Error


class MinioConfig:
    ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    BUCKET_NAME = os.getenv("MINIO_BUCKET", "images")
    SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download theses.fr PDFs, extract pages as images, upload to MinIO.")
    p.add_argument("--csv", required=True, help="Input CSV path containing column 'oai_id'")
    p.add_argument("--encoding", default="utf-8", help="CSV encoding (default: utf-8)")
    p.add_argument("--id-column", default="oai_id", help="Column containing thesis IDs (default: oai_id)")

    p.add_argument("--bucket-subfolder", default="theses/theses.fr", help="Prefix folder in bucket")
    p.add_argument("--num-pages", type=int, default=10, help="Number of PDF pages to convert (default: 10)")
    p.add_argument("--dpi", type=int, default=200, help="Rasterization DPI (default: 200)")
    p.add_argument("--timeout", type=int, default=60, help="HTTP timeout seconds (default: 60)")

    p.add_argument("--batch-size", type=int, default=50, help="How many theses per batch (default: 50)")
    p.add_argument("--start", type=int, default=0, help="Start index in ID list (default: 0)")
    p.add_argument("--limit", type=int, default=None, help="Max number of theses to process (default: all)")

    p.add_argument("--dry-run", action="store_true", help="Do not download/upload, just print planned actions")
    return p.parse_args(argv)


def get_minio_client() -> Minio:
    return Minio(
        MinioConfig.ENDPOINT,
        access_key=MinioConfig.ACCESS_KEY,
        secret_key=MinioConfig.SECRET_KEY,
        secure=MinioConfig.SECURE,
    )


def ensure_bucket_exists(client: Minio) -> bool:
    try:
        if not client.bucket_exists(MinioConfig.BUCKET_NAME):
            client.make_bucket(MinioConfig.BUCKET_NAME)
            print(f"✓ Created bucket: {MinioConfig.BUCKET_NAME}")
        return True
    except S3Error as e:
        print(f"✗ Error with bucket: {e}", file=sys.stderr)
        return False


def download_pdf_to_temp(url: str, timeout: int = 60) -> Optional[str]:
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
        tmp.write(resp.content)
        tmp.close()
        return tmp.name
    except requests.RequestException as e:
        print(f"✗ Download error {url}: {e}", file=sys.stderr)
        return None


def extract_pages_as_images(pdf_path: str, num_pages: int = 10, dpi: int = 200) -> Optional[List[bytes]]:
    try:
        reader = PdfReader(pdf_path)
        total_pages = len(reader.pages)
        pages_to_extract = min(num_pages, total_pages)
        if pages_to_extract <= 0:
            return []

        images = convert_from_path(
            pdf_path,
            first_page=1,
            last_page=pages_to_extract,
            dpi=dpi,
        )

        out: list[bytes] = []
        for img in images:
            buff = BytesIO()
            img.save(buff, format="PNG")
            out.append(buff.getvalue())
        return out
    except Exception as e:
        print(f"✗ PDF->images error: {e}", file=sys.stderr)
        return None


def upload_images_to_minio(
    client: Minio,
    thesis_id: str,
    images: List[bytes],
    bucket_subfolder: str,
) -> bool:
    try:
        prefix = bucket_subfolder.strip("/")

        for i, image_bytes in enumerate(images, start=1):
            object_name = f"{prefix}/{thesis_id}/p{i}.png" if prefix else f"{thesis_id}/p{i}.png"
            client.put_object(
                MinioConfig.BUCKET_NAME,
                object_name,
                BytesIO(image_bytes),
                length=len(image_bytes),
                content_type="image/png",
            )
            print(f"  ✓ Uploaded: {object_name}")
        return True
    except S3Error as e:
        print(f"✗ MinIO upload error: {e}", file=sys.stderr)
        return False


def process_thesis(
    thesis_id: str,
    client: Minio,
    bucket_subfolder: str,
    num_pages: int,
    dpi: int,
    timeout: int,
    dry_run: bool = False,
) -> bool:
    temp_pdf_path = None
    url = f"https://theses.fr/{thesis_id}/abes"

    try:
        print(f"Downloading: {url}")
        if dry_run:
            print("  (dry-run) skip download/convert/upload")
            return True

        temp_pdf_path = download_pdf_to_temp(url, timeout=timeout)
        if not temp_pdf_path:
            return False

        images = extract_pages_as_images(temp_pdf_path, num_pages=num_pages, dpi=dpi)
        if images is None:
            return False

        if not images:
            print("  ! No pages extracted (empty PDF?)", file=sys.stderr)
            return False

        ok = upload_images_to_minio(client, thesis_id, images, bucket_subfolder=bucket_subfolder)
        return ok
    finally:
        if temp_pdf_path and os.path.exists(temp_pdf_path):
            os.unlink(temp_pdf_path)


def batched(iterable: List[str], batch_size: int) -> Iterable[List[str]]:
    for i in range(0, len(iterable), batch_size):
        yield iterable[i : i + batch_size]


def process_multiple_theses_from_csv(
    csv_path: Path,
    id_column: str,
    bucket_subfolder: str,
    num_pages: int,
    dpi: int,
    timeout: int,
    batch_size: int,
    start: int,
    limit: Optional[int],
    encoding: str,
    dry_run: bool,
) -> int:
    df = pd.read_csv(csv_path, encoding=encoding)

    if id_column not in df.columns:
        print(f"[ERROR] Column '{id_column}' not found in CSV.", file=sys.stderr)
        return 3

    ids = (
        df[id_column]
        .dropna()
        .astype(str)
        .str.strip()
        .loc[lambda s: s != ""]
        .tolist()
    )

    # slice start/limit
    ids = ids[start : (start + limit) if limit is not None else None]

    if not ids:
        print("No thesis IDs to process.")
        return 0

    # MinIO init
    client = get_minio_client()
    if not ensure_bucket_exists(client):
        return 4

    total = len(ids)
    print(f"Processing {total} theses | batch_size={batch_size} | num_pages={num_pages} | bucket_subfolder='{bucket_subfolder}'")
    print(f"Bucket: {MinioConfig.BUCKET_NAME} @ {MinioConfig.ENDPOINT} (secure={MinioConfig.SECURE})")

    successful = 0
    failed = 0

    for bidx, batch_ids in enumerate(batched(ids, batch_size), start=1):
        print("\n" + "=" * 72)
        print(f"Batch {bidx} | items {((bidx-1)*batch_size)+1}-{((bidx-1)*batch_size)+len(batch_ids)} / {total}")
        print("=" * 72)

        for thesis_id in batch_ids:
            print(f"\n--- {thesis_id} ---")
            ok = process_thesis(
                thesis_id,
                client=client,
                bucket_subfolder=bucket_subfolder,
                num_pages=num_pages,
                dpi=dpi,
                timeout=timeout,
                dry_run=dry_run,
            )
            if ok:
                successful += 1
            else:
                failed += 1

    print("\n" + "=" * 72)
    print(f"Summary: {successful} successful, {failed} failed")
    print("=" * 72)

    return 0 if failed == 0 else 1


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"[ERROR] CSV not found: {csv_path}", file=sys.stderr)
        return 2

    return process_multiple_theses_from_csv(
        csv_path=csv_path,
        id_column=args.id_column,
        bucket_subfolder=args.bucket_subfolder,
        num_pages=args.num_pages,
        dpi=args.dpi,
        timeout=args.timeout,
        batch_size=args.batch_size,
        start=args.start,
        limit=args.limit,
        encoding=args.encoding,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))