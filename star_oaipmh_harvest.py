#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "pandas",
#   "requests",
#   "lxml"
# ]
# ///
"""
OAI-PMH harvester for the STAR (theses.fr) repository.

Harvests thesis records exposed via OAI-PMH and exports a flat CSV,
restricted to records explicitly marked as Open Access (dc:rights).

Features:
- OAI-PMH ListRecords harvesting with resumptionToken support
- Optional filtering by setSpec (e.g. diffusable, ddc:xxx, institution code)
- Extraction of core Dublin Core metadata (oai_dc)
- Language-aware description fields (fr / en)
- One-column-per-contributor flattening

Usage:
  python star_oaipmh_harvest.py
  uv run star_oaipmh_harvest.py --max-pages 1 --max-contributors 2
"""
import csv
import time
import requests
import argparse
from lxml import etree
import pandas as pd
import re
import sys
from typing import Optional

BASE = "https://staroai.theses.fr/OAIHandler"

NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
    "dc": "http://purl.org/dc/elements/1.1/",
    "xml": "http://www.w3.org/XML/1998/namespace",
}

# --------------------------
# Helpers
# --------------------------

def texts(nodes):
    out = []
    for n in nodes:
        t = (n.text or "").strip()
        if t:
            out.append(t)
    return out

def join_texts(nodes, sep=" | "):
    return sep.join(texts(nodes))

def extract_year(date_str):
    """Extract YYYY from a date string like YYYY or YYYY-MM-DD etc."""
    if not date_str:
        return ""
    m = re.search(r"\b(18|19|20)\d{2}\b", date_str)
    return m.group(0) if m else ""

def oai_params(metadata_prefix="oai_dc", set_spec=None, token=None):
    if token:
        return {"verb": "ListRecords", "resumptionToken": token}
    p = {"verb": "ListRecords", "metadataPrefix": metadata_prefix}
    if set_spec:
        p["set"] = set_spec
    return p

def parse_page(xml_bytes):
    root = etree.fromstring(xml_bytes)
    records = root.xpath("//oai:ListRecords/oai:record", namespaces=NS)

    token_el = root.xpath("//oai:ListRecords/oai:resumptionToken", namespaces=NS)
    token = ""
    if token_el:
        token = (token_el[0].text or "").strip()

    return records, token

def has_open_access(dc_root) -> bool:
    """Return True if any dc:rights equals 'Open Access' (case-insensitive)."""
    rights_vals = texts(dc_root.xpath("dc:rights", namespaces=NS))
    return any(rv.strip().lower() == "open access" for rv in rights_vals)

# --------------------------
# Record extraction
# --------------------------

def extract_record(rec, max_contributors=5):
    # header
    oai_id = rec.xpath("string(oai:header/oai:identifier)", namespaces=NS).strip()
    datestamp = rec.xpath("string(oai:header/oai:datestamp)", namespaces=NS).strip()
    sets = texts(rec.xpath("oai:header/oai:setSpec", namespaces=NS))
    setSpecs_raw = " | ".join(sets)

    # derive ddc + etab + diffusable
    set_ddc = ""
    set_etab = ""
    is_diffusable = "0"

    for s in sets:
        if s.startswith("ddc:"):
            set_ddc = s
        elif s == "diffusable":
            is_diffusable = "1"
        else:
            # heuristique : un setSpec sans ":" et pas "diffusable" est souvent un code établissement
            if ":" not in s and s != "diffusable":
                if not set_etab:
                    set_etab = s

    # metadata
    md = rec.xpath("oai:metadata/oai_dc:dc", namespaces=NS)

    row = {
        "oai_id": oai_id,
        "setSpecs_raw": setSpecs_raw,
        "set_etab": set_etab,
        "set_ddc": set_ddc,
        "is_diffusable": is_diffusable,

        "title": "",
        "subject": "",
        "description_fr": "",
        "description_en": "",
        "language": "",
        "identifier": "",
        "creator": "",
        "date": "",
        "year": "",
        "rights": "",
    }

    for i in range(1, max_contributors + 1):
        row[f"contributor_{i}"] = ""

    # deleted record -> no metadata
    if not md:
        return row, False

    dc_root = md[0]

    # Filter: dc:rights == "Open Access"
    if not has_open_access(dc_root):
        return row, False

    # Keep rights values (concat is OK for rights)
    row["rights"] = join_texts(dc_root.xpath("dc:rights", namespaces=NS), sep=" | ")

    # Basic DC fields (NO dc:type)
    row["title"] = join_texts(dc_root.xpath("dc:title", namespaces=NS), sep=" | ")
    row["language"] = join_texts(dc_root.xpath("dc:language", namespaces=NS), sep=" | ")
    row["identifier"] = join_texts(dc_root.xpath("dc:identifier", namespaces=NS), sep=" | ")
    row["creator"] = join_texts(dc_root.xpath("dc:creator", namespaces=NS), sep=" | ")

    # subject: concat with |
    row["subject"] = join_texts(dc_root.xpath("dc:subject", namespaces=NS), sep=" | ")

    # date + year (take first date if multiple)
    row["date"] = join_texts(dc_root.xpath("dc:date", namespaces=NS), sep=" | ")
    first_date = row["date"].split(" | ")[0].strip() if row["date"] else ""
    row["year"] = extract_year(first_date)

    # descriptions split by lang (keep both)
    desc_fr = dc_root.xpath("dc:description[@xml:lang='fr']", namespaces=NS)
    desc_en = dc_root.xpath("dc:description[@xml:lang='en']", namespaces=NS)
    desc_nolang = dc_root.xpath("dc:description[not(@xml:lang)]", namespaces=NS)

    row["description_fr"] = join_texts(desc_fr, sep="\n\n")
    row["description_en"] = join_texts(desc_en, sep="\n\n")
    if not row["description_fr"] and desc_nolang:
        row["description_fr"] = join_texts(desc_nolang, sep="\n\n")

    # contributors: keep as-is, no concat, 1 column per occurrence
    contributors = texts(dc_root.xpath("dc:contributor", namespaces=NS))
    for idx, val in enumerate(contributors[:max_contributors], start=1):
        row[f"contributor_{idx}"] = val

    return row, True

# --------------------------
# Harvest
# --------------------------

def harvest(
    set_spec: str,
    out_csv: str,
    metadata_prefix: str = "oai_dc",
    sleep_s: float = 0.2,
    max_pages: Optional[int] = None,
    max_contributors: int = 3
):
    fieldnames = [
        "oai_id", "setSpecs_raw", "set_etab", "set_ddc", "is_diffusable",
        "title", "subject", "description_fr", "description_en",
        "language", "identifier", "creator", "date", "year", "rights",
    ] + [f"contributor_{i}" for i in range(1, max_contributors + 1)]

    kept = 0
    seen = 0

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        token = None
        page = 0

        while True:
            params = oai_params(metadata_prefix=metadata_prefix, set_spec=set_spec, token=token)
            r = requests.get(BASE, params=params, timeout=90)
            r.raise_for_status()

            records, token = parse_page(r.content)

            for rec in records:
                status = rec.xpath("string(oai:header/@status)", namespaces=NS).strip()
                if status == "deleted":
                    continue

                row, ok = extract_record(rec, max_contributors=max_contributors)
                seen += 1
                if ok:
                    writer.writerow(row)
                    kept += 1

            page += 1
            if max_pages is not None and page >= max_pages:
                break
            if not token:
                break

            time.sleep(sleep_s)

    print(f"Done. Seen={seen}, kept(Open Access)={kept}, output={out_csv}")
    return out_csv

def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="STAR OAI-PMH harvester (theses.fr)"
    )

    parser.add_argument(
        "--set-spec",
        default="diffusable",
        help="OAI setSpec to harvest (e.g. diffusable, ddc:600, CNAM)"
    )

    parser.add_argument(
        "--out-csv",
        default="theses_diffusable_openaccess_flat.csv",
        help="Output CSV path"
    )

    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Number of pages to harvest (100 records/page). Harvest all if omitted."
    )

    parser.add_argument(
        "--max-contributors",
        type=int,
        default=3,
        help="Number of dc:contributor fields to extract (one per column)"
    )

    return parser.parse_args(argv)

if __name__ == "__main__":
    args = parse_args(sys.argv[1:])

    harvest(
        set_spec=args.set_spec,
        out_csv=args.out_csv,
        max_pages=args.max_pages,
        max_contributors=args.max_contributors
    )
