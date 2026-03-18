#!/usr/bin/env -S uv run
# /// script
# dependencies = [
#   "pandas",
#   "requests",
#   "lxml",
# ]
# ///
"""
DUMAS / SUDOC HARVEST + HAL ENRICHMENT SCRIPT

This script harvests thesis records from the SUDOC SRU API (UNIMARC format),
filters records that link to DUMAS (HAL theses repository), and enriches them
with metadata retrieved from the official HAL API.

--------------------------------------------------
PIPELINE OVERVIEW
--------------------------------------------------

1. SRU HARVEST (SUDOC / ABES)
   - Queries the SUDOC SRU endpoint using a CQL query.
   - Retrieves UNIMARC XML records in batches.
   - Extracts:
        - PPN (controlfield 001)
        - URL (field 856$u)

2. FILTERING
   - Keeps only records whose URL contains:
        "dumas.ccsd.cnrs"
   - These correspond to theses deposited in DUMAS (HAL).

3. MULTI-QUERY STRATEGY
   - Runs the SRU harvest for multiple diploma labels:
        - Mémoire de master recherche 1re année
        - Mémoire de master recherche 2e année
        - Mémoire de maîtrise
   - Concatenates results into a single dataset.

4. HAL API ENRICHMENT
   - For each DUMAS URL:
        → extracts the HAL identifier (e.g. dumas-04953548)
        → queries the HAL search API
   - Retrieves structured metadata:
        - hal_id
        - uri
        - title (HTML cleaned)
        - authors
        - institutions (excluding "DUMAS")
        - domain
        - open access flag
        - document URL
        - abstract (FR + generic)
        - language
        - defense year (normalized to YYYY)

5. CLEANING
   - Removes records without HAL match (hal_id is null)
   - Deduplicates on hal_id

6. EXPORT
   - Saves final dataset as CSV

--------------------------------------------------
OUTPUT COLUMNS
--------------------------------------------------

The final dataset includes:

- ppn
- url (original SUDOC link)
- hal_id
- dumas_id
- uri
- title
- authLastNameFirstName
- etab
- hal_domain
- is_diffusable
- document_url
- description
- description_fr
- language
- year

--------------------------------------------------
HOW TO RUN
--------------------------------------------------

Using uv (recommended):

    uv run sudoc_dumas_memoires_harvest.py --output dumas.csv

Custom diploma filters:

    uv run sudoc_dumas_memoires_harvest.py \
        --diplomes "Mémoire de maîtrise" "Mémoire de master recherche 2e année" \
        --output results.csv
--------------------------------------------------
"""
from __future__ import annotations

import argparse
import time
import re
from datetime import datetime
from urllib.parse import urlparse, quote

import pandas as pd
import requests
import xml.etree.ElementTree as ET


# =========================================================
# ------------------- UNIMARC PARSER ----------------------
# =========================================================

class RecordParser:
    def get_subfield(self, record, tag, code):
        path = f".//datafield[@tag='{tag}']/subfield[@code='{code}']"
        element = record.find(path)
        return element.text if element is not None else None

    def get_controlfield(self, record, tag):
        element = record.find(f".//controlfield[@tag='{tag}']")
        return element.text if element is not None else None

    def parse_unimarc_thesis_record(self, record):
        return {
            "ppn": (self.get_controlfield(record, "001") or "").strip(),
            "url": (self.get_subfield(record, "856", "u") or "").strip(),
        }


# =========================================================
# ------------------- SRU HARVESTER ------------------------
# =========================================================

class SRUHarvester:
    def __init__(self, query: str, batch_size: int = 100):
        self.base_url = "https://sudoc.abes.fr/cbs/sru/"
        self.headers = {"Accept": "application/xml"}
        self.params = {
            "operation": "searchRetrieve",
            "version": "1.1",
            "recordSchema": "unimarc",
        }
        self.query = quote(query)
        self.batch_size = batch_size
        self.parser = RecordParser()
        self.namespaces = {"srw": "http://www.loc.gov/zing/srw/"}

    def get_number_of_records(self) -> int:
        self.params.update({"maximumRecords": 1, "startRecord": 1})
        url = f"{self.base_url}?{'&'.join([f'{k}={v}' for k,v in self.params.items()])}&query={self.query}"

        r = requests.get(url, headers=self.headers)
        r.raise_for_status()

        root = ET.fromstring(r.content)
        el = root.find(".//srw:numberOfRecords", namespaces=self.namespaces)
        return int(el.text) if el is not None else 0

    def fetch_all(self) -> pd.DataFrame:
        total = self.get_number_of_records()
        rows = []

        for start in range(1, total + 1, self.batch_size):
            print(f"Fetching {start} → {start+self.batch_size}")

            self.params.update({
                "maximumRecords": self.batch_size,
                "startRecord": start
            })

            url = f"{self.base_url}?{'&'.join([f'{k}={v}' for k,v in self.params.items()])}&query={self.query}"

            r = requests.get(url, headers=self.headers)
            r.raise_for_status()

            root = ET.fromstring(r.content)
            records = root.findall(".//srw:record", namespaces=self.namespaces)

            for rec in records:
                data_el = rec.find(".//srw:recordData", namespaces=self.namespaces)
                if data_el is None:
                    continue

                record = data_el.find(".//record")
                if record is None:
                    continue

                parsed = self.parser.parse_unimarc_thesis_record(record)
                rows.append(parsed)

        return pd.DataFrame(rows)


# =========================================================
# ------------------- HAL HELPERS --------------------------
# =========================================================

def extract_record_id_from_url(url: str) -> str | None:
    path = urlparse(url).path.strip("/")
    return path.split("/")[0] if path else None


def strip_html_tags(text):
    if text is None:
        return None
    return re.sub(r"<[^>]+>", "", str(text)).strip()


def ensure_list(value):
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def first_or_none(value):
    return next((v for v in ensure_list(value) if v), None)


def join_list(value):
    vals = [str(v).strip() for v in ensure_list(value) if v]
    return "|".join(vals) if vals else None


def join_collnames_excluding_dumas(coll_names):
    vals = []
    for v in ensure_list(coll_names):
        if v and str(v).upper() != "DUMAS":
            vals.append(str(v).strip())
    return "|".join(vals) if vals else None


def normalize_year(value):
    if value is None:
        return None

    if isinstance(value, list):
        value = first_or_none(value)

    if not value:
        return None

    value = str(value)

    for fmt in ("%Y-%m-%d", "%Y-%m"):
        try:
            return str(datetime.strptime(value, fmt).year)
        except:
            pass

    m = re.search(r"(19|20)\d{2}", value)
    return m.group(0) if m else None


# =========================================================
# ------------------- HAL API ------------------------------
# =========================================================

def fetch_hal_doc(url, session):
    record_id = extract_record_id_from_url(url)
    if not record_id:
        return None

    resp = session.get(
        "https://api.archives-ouvertes.fr/search/",
        params={
            "q": f"halId_s:{record_id}",
            "wt": "json",
            "rows": 1,
            "fl": ",".join([
                "openAccess_bool",
                "primaryDomain_s",
                "title_s",
                "authLastNameFirstName_s",
                "language_s",
                "halId_s",
                "uri_s",
                "defenseDate_s",
                "collName_s",
                "fileMain_s",
                "abstract_s",
                "fr_abstract_s",
            ]),
        },
        timeout=30,
    )
    resp.raise_for_status()

    docs = resp.json().get("response", {}).get("docs", [])
    return docs[0] if docs else None


def parse_hal_doc(doc, url):
    return {
        "hal_id": doc.get("halId_s"),
        "dumas_id": extract_record_id_from_url(url),
        "uri": doc.get("uri_s"),
        "title": strip_html_tags(first_or_none(doc.get("title_s"))),
        "authLastNameFirstName": first_or_none(doc.get("authLastNameFirstName_s")),
        "etab": join_collnames_excluding_dumas(doc.get("collName_s")),
        "hal_domain": doc.get("primaryDomain_s"),
        "is_diffusable": doc.get("openAccess_bool"),
        "document_url": doc.get("fileMain_s"),
        "description": strip_html_tags(first_or_none(doc.get("abstract_s"))),
        "description_fr": strip_html_tags(first_or_none(doc.get("fr_abstract_s"))),
        "language": join_list(doc.get("language_s")),
        "year": normalize_year(doc.get("defenseDate_s")),
    }


def enrich_dataframe(df):
    rows = []

    with requests.Session() as session:
        for _, r in df.iterrows():
            url = r["url"]

            try:
                doc = fetch_hal_doc(url, session)
                rows.append(parse_hal_doc(doc, url) if doc else {})
                time.sleep(0.05)
            except Exception as e:
                rows.append({"hal_error": str(e)})

    return pd.concat([df.reset_index(drop=True), pd.DataFrame(rows)], axis=1)


# =========================================================
# ------------------- MAIN PIPELINE ------------------------
# =========================================================

def harvest(diplomes):
    dfs = []

    for d in diplomes:
        print(f"\n=== Harvesting: {d} ===")

        query = f'nth="{d}" and tdo=o and apu>=1975'
        harvester = SRUHarvester(query=query, batch_size=100)

        df = harvester.fetch_all()

        df = df[df["url"].str.contains("dumas.ccsd.cnrs", na=False)]

        dfs.append(df)

    df_all = pd.concat(dfs, ignore_index=True)

    df_all = enrich_dataframe(df_all)

    df_all = df_all.dropna(subset=["hal_id"]).drop_duplicates(subset=["hal_id"])

    return df_all


# =========================================================
# ------------------- CLI --------------------------------
# =========================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="dumas.csv")
    parser.add_argument(
        "--diplomes",
        nargs="+",
        default=[
            "Mémoire de master recherche 1re année",
            "Mémoire de master recherche 2e année",
            "Mémoire de maîtrise",
        ],
    )

    args = parser.parse_args()

    df = harvest(args.diplomes)

    df.to_csv(args.output, index=False)
    print(f"\nSaved → {args.output}")


if __name__ == "__main__":
    main()