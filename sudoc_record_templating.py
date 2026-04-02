#!/usr/bin/env -S uv run
# /// script
# dependencies = [
#   "pandas",
#   "requests",
#   "lxml",
# ]
# ///

"""
HUMATHEQUE / SUDOC TEMPLATE HARVEST SCRIPT

Pipeline
--------
1. Read theses and dissertations input CSV files
2. Resolve thesis oai_id (NNT) -> SUDOC PPN via ABES service
3. Merge theses + dissertations into a single dataframe
4. Resolve base_id -> case_id / image_uri from Postgres API
5. Flag outlier annotations
6. Fetch SUDOC XML records for each PPN
7. Extract templated metadata for theses / dissertations
8. Export merged outputs as CSV

Example
-------
uv run sudoc_record_templating.py \
  --theses-csv _sample_filtered_humatheque_theses_diffusable_openaccess_flat.csv \
  --memoires-csv _sample_filtered_memoires_dumas_openaccess_flat.csv \
  --cases-api-url https://data.smartbiblia.fr/cases?limit=300 \
  --output-prefix humatheque_harvest

Outputs
-------
- <prefix>_documents.csv
- <prefix>_theses_metadata.csv
- <prefix>_memoires_metadata.csv
- <prefix>_templated_records.csv
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd
import requests


DEFAULT_THESES_FIELDS = {
    "title": {
        "desc": "Main title of the thesis as it appears on the title page",
        "mapping": "title|(200,a)|all:false",
    },
    "subtitle": {
        "desc": "Subtitle or remainder of the title, usually following a colon; null if not present",
        "mapping": "subtitle|(200,e)|all:false",
    },
    "author": {
        "desc": "Full name of the author (student) who wrote the thesis",
        "mapping": "author|(200,f)|all:false",
    },
    "degree_type": {
        "desc": "Academic degree sought by the author",
        "mapping": "degree_type|(328,b)|all:false",
    },
    "discipline": {
        "desc": "Academic field or discipline of the thesis if explicitly stated; null if not present.",
        "mapping": "discipline|(328,c)|all:false",
    },
    "granting_institution": {
        "desc": "Institution where the thesis was submitted and the degree is granted",
        "mapping": "granting_institution|(328,e)|all:false",
    },
    "doctoral_school": {
        "desc": "Doctoral school or graduate program, if explicitly mentioned",
        "mapping": "doctoral_school|(711,a,4=996)|all:true",
    },
    "co_tutelle_institutions": {
        "desc": "Institutions involved in a joint supervision or co-tutelle agreement",
        "mapping": "co_tutelle_institutions|(711,a,4=995)|all:true",
    },
    "partner_institutions": {
        "desc": "Partner institutions associated with the thesis but not granting the degree",
        "mapping": "partner_institutions|(711,a,4=985)|all:true",
    },
    "defense_year": {
        "desc": "Year the thesis was defended",
        "mapping": "defense_year|(328,d)|all:false",
    },
    "thesis_advisor": {
        "desc": "Main thesis advisor or supervisor",
        "mapping": "thesis_advisor|(701,b,4=727),(701,a,4=727)|all:true;sep: ;list_sep:|",
    },
    "jury_president": {
        "desc": "President or chair of the thesis examination committee",
        "mapping": "jury_president|(701,b,4=956),(701,a,4=956)|all:true;sep: ;list_sep:|",
    },
    "reviewers": {
        "desc": "Reviewers or rapporteurs of the thesis",
        "mapping": "reviewers|(701,b,4=958),(701,a,4=958)|all:true;sep: ;list_sep:|",
    },
    "committee_members": {
        "desc": "Other thesis committee or jury members",
        "mapping": "committee_members|(701,b,4=555),(701,a,4=555)|all:true;sep: ;list_sep:|",
    },
    "language": {
        "desc": "Language in ISO 639-3 codes",
        "mapping": "language|(101,a)|all:false",
    },
    "abstract": {
        "desc": "Abstract, if explicitly stated",
        "mapping": "abstract|(330,a)|all:false",
    },
}


DEFAULT_DISSERTATION_FIELDS = {
    "title": {
        "desc": "Main title of the dissertation as it appears on the title page",
        "mapping": "title|(200,a)|all:false",
    },
    "subtitle": {
        "desc": "Subtitle or remainder of the title, usually following a colon; null if not present",
        "mapping": "subtitle|(200,e)|all:false",
    },
    "author": {
        "desc": "Full name of the author (student) who wrote the dissertation",
        "mapping": "author|(200,f)|all:false",
    },
    "degree_type": {
        "desc": "Academic degree sought by the author",
        "mapping": "degree_type|(328,b)|all:false",
    },
    "discipline": {
        "desc": "Academic field or discipline of the dissertation if explicitly stated; null if not present.",
        "mapping": "discipline|(328,c)|all:false",
    },
    "granting_institution": {
        "desc": "Institution where the dissertation was submitted and the degree is granted",
        "mapping": "granting_institution|(328,e)|all:false",
    },
    "doctoral_school": {
        "desc": "Doctoral school or graduate program, if explicitly mentioned",
        "mapping": "doctoral_school|(711,a,4=996)|all:true",
    },
    "co_tutelle_institutions": {
        "desc": "Institutions involved in a joint supervision or co-tutelle agreement",
        "mapping": "co_tutelle_institutions|(711,a,4=995)|all:true",
    },
    "partner_institutions": {
        "desc": "Partner institutions associated with the dissertation but not granting the degree",
        "mapping": "partner_institutions|(711,a,4=985)|all:true",
    },
    "defense_year": {
        "desc": "Year the dissertation was defended",
        "mapping": "defense_year|(328,d)|all:false",
    },
    "dissertation_advisor": {
        "desc": "Main dissertation advisor or supervisor",
        "mapping": "dissertation_advisor|(701,b,4=003),(701,a,4=003)|all:true;sep: ;list_sep:|",
    },
    "jury_president": {
        "desc": "President or chair of the dissertation examination committee",
        "mapping": "jury_president|(701,b,4=956),(701,a,4=956)|all:true;sep: ;list_sep:|",
    },
    "reviewers": {
        "desc": "Reviewers or rapporteurs of the dissertation",
        "mapping": "reviewers|(701,b,4=958),(701,a,4=958)|all:true;sep: ;list_sep:|",
    },
    "committee_members": {
        "desc": "Other dissertation committee or jury members",
        "mapping": "committee_members|(701,b,4=555),(701,a,4=555)|all:true;sep: ;list_sep:|",
    },
    "language": {
        "desc": "Language in ISO 639-3 codes",
        "mapping": "language|(101,a)|all:false",
    },
    "abstract": {
        "desc": "Abstract, if explicitly stated",
        "mapping": "abstract|(330,a)|all:false",
    },
}


DEFAULT_OUTLIERS = [
    "e5bd3fcb-4b4e-48d0-9e8a-d4c690978fce",
    "10c1e36b-e778-4baf-b6fb-39aec0ff114f",
    "d2f1107d-ab72-4b61-8340-4c0fc052dfac",
    "3453c748-0834-40a3-bff9-3a1af7b8f1f0",
    "04135df7-49aa-46e1-bdaf-445207994044",
    "e6fe30ba-3c7e-48ec-9844-0fcf2ad0a4a4",
    "ff64909f-490e-4d63-a675-ed9be7bcfbb7",
    "674a70fb-69aa-48d9-aecf-fc5dc254c45d",
    "490b3868-4609-46d7-a247-525de840f2a6",
    "f6ca23bc-a63f-429d-ad5b-ff232631453f",
    "e6fe30ba-3c7e-48ec-9844-0fcf2ad0a4a4",
    "8b058bf0-7829-4f28-b500-b9974f1b54f6",
]

PAIR_PATTERN = re.compile(
    r"\(\s*([0-9]{3})\s*,\s*([a-z0-9])(?:\s*,\s*([a-z0-9])\s*(=|~=)\s*([^)]+?)\s*)?\)"
)


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def safe_json_dumps(data: dict) -> str:
    return json.dumps(data, ensure_ascii=False)


def load_inputs(theses_csv: str, memoires_csv: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    theses = pd.read_csv(theses_csv)[["oai_id"]].copy()
    memoires = pd.read_csv(memoires_csv)[["ppn", "hal_id"]].copy()
    return theses, memoires


def get_sudoc_ppn(oai_id: str, session: requests.Session, timeout: int = 10) -> str:
    """Fetch the Sudoc PPN for a given thesis oai_id / NNT."""
    url = f"https://www.sudoc.fr/services/nnt2ppn/{oai_id}&format=text/json"
    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        data = response.json()

        if not data or "sudoc" not in data or "results" not in data["sudoc"]:
            return ""

        results = data["sudoc"]["results"]
        ppn_m_type = ""
        first_ppn = ""

        if isinstance(results, dict):
            result = results.get("result")
            if result:
                first_ppn = result.get("ppn", "")
                if result.get("typerecord") == "m":
                    ppn_m_type = first_ppn

        elif isinstance(results, list):
            for item in results:
                result = item.get("result")
                if not result:
                    continue
                current_ppn = result.get("ppn", "")
                if not first_ppn:
                    first_ppn = current_ppn
                if result.get("typerecord") == "m":
                    ppn_m_type = current_ppn
                    break

        return ppn_m_type if ppn_m_type else first_ppn

    except Exception:
        return ""


def enrich_theses_with_ppn(theses: pd.DataFrame, sleep_s: float = 0.0) -> pd.DataFrame:
    theses = theses.copy()
    with requests.Session() as session:
        theses["ppn"] = theses["oai_id"].apply(lambda x: get_sudoc_ppn(x, session=session))
        if sleep_s > 0:
            time.sleep(sleep_s)
    return theses


def prepare_documents_dataframe(theses: pd.DataFrame, memoires: pd.DataFrame) -> pd.DataFrame:
    theses = theses.rename(columns={"oai_id": "base_id"}).copy()
    memoires = memoires.rename(columns={"hal_id": "base_id"}).copy()

    theses["doc_type"] = "these"
    memoires["doc_type"] = "memoire"

    df = pd.concat([theses, memoires], ignore_index=True)
    return df


def fetch_cases_table(cases_api_url: str, session: requests.Session, timeout: int = 20) -> pd.DataFrame:
    response = session.get(cases_api_url, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    return pd.DataFrame(data)


def build_case_lookup(cases_df: pd.DataFrame) -> list[tuple[str, str, str]]:
    rows = []
    for _, row in cases_df.iterrows():
        case_id = row.get("case_id")
        image_uri = row.get("image_uri")
        if pd.notna(case_id) and pd.notna(image_uri):
            rows.append((str(case_id), str(image_uri), str(image_uri)))
    return rows


def get_doc_url_from_lookup(base_id: str, case_lookup: list[tuple[str, str, str]]) -> tuple[str | None, str | None]:
    if pd.isna(base_id):
        return None, None
    base_id = str(base_id)
    for case_id, image_uri, image_uri_search in case_lookup:
        if base_id in image_uri_search:
            return case_id, image_uri
    return None, None


def attach_case_info(df: pd.DataFrame, cases_api_url: str) -> pd.DataFrame:
    df = df.copy()
    with requests.Session() as session:
        cases_df = fetch_cases_table(cases_api_url=cases_api_url, session=session)
    case_lookup = build_case_lookup(cases_df)
    df[["case_id", "image_uri"]] = df.apply(
        lambda row: get_doc_url_from_lookup(row["base_id"], case_lookup),
        axis=1,
        result_type="expand",
    )
    return df


def flag_outliers(df: pd.DataFrame, outliers: list[str]) -> pd.DataFrame:
    df = df.copy()
    outlier_set = set(outliers)
    df["annot_type"] = df["case_id"].apply(
        lambda x: "outlier" if pd.notna(x) and str(x) in outlier_set else "normal"
    )
    return df


def fetch_ppn_xml(ppn: str, session: requests.Session, timeout: int = 20) -> ET.Element:
    url = f"https://www.sudoc.fr/{ppn}.xml"
    response = session.get(url, headers={"Accept": "application/xml"}, timeout=timeout)
    response.raise_for_status()
    return ET.fromstring(response.content.decode("utf-8"))


def extract_field_robust(
    root: ET.Element,
    selectors: list[dict],
    get_all: bool = False,
    subfield_separator: str = " ",
):
    def matching_datafields(selector: dict) -> list[ET.Element]:
        tag = selector["tag"]
        filter_code = selector.get("filter_code")
        filter_op = selector.get("filter_op")
        filter_value = selector.get("filter_value")

        datafields = []
        for datafield in root.findall(f".//datafield[@tag='{tag}']"):
            if filter_code is not None:
                has_required_filter = False
                for sf in datafield.findall("./subfield"):
                    if sf.get("code") != filter_code:
                        continue
                    sf_text = (sf.text or "").strip()
                    if filter_op == "=" and sf_text == filter_value:
                        has_required_filter = True
                        break
                    if filter_op == "~=" and filter_value in sf_text:
                        has_required_filter = True
                        break
                if not has_required_filter:
                    continue
            datafields.append(datafield)
        return datafields

    def values_for_selector(selector: dict) -> list[str]:
        code = selector["code"]
        values = []
        for datafield in matching_datafields(selector):
            for subfield in datafield.findall(f"./subfield[@code='{code}']"):
                if subfield.text:
                    values.append(subfield.text.strip())
        return values

    if get_all:
        selector_groups: dict[tuple, list[dict]] = {}
        group_order: list[tuple] = []

        for selector in selectors:
            group_key = (
                selector["tag"],
                selector.get("filter_code"),
                selector.get("filter_op"),
                selector.get("filter_value"),
            )
            if group_key not in selector_groups:
                selector_groups[group_key] = []
                group_order.append(group_key)
            selector_groups[group_key].append(selector)

        values = []
        for group_key in group_order:
            grouped_selectors = selector_groups[group_key]
            if len(grouped_selectors) == 1:
                values.extend(values_for_selector(grouped_selectors[0]))
                continue

            for datafield in matching_datafields(grouped_selectors[0]):
                parts = []
                for selector in grouped_selectors:
                    code = selector["code"]
                    for subfield in datafield.findall(f"./subfield[@code='{code}']"):
                        if subfield.text and subfield.text.strip():
                            parts.append(subfield.text.strip())
                if parts:
                    values.append(subfield_separator.join(parts))
        return values

    for selector in selectors:
        selector_values = values_for_selector(selector)
        if selector_values:
            return selector_values[0]
    return None


def parse_mapping_line(raw_line: str) -> dict:
    raw_parts = raw_line.split("|", maxsplit=2)
    if len(raw_parts) != 3:
        raise ValueError(
            f"Invalid mapping '{raw_line}'. Expected format: Nom|(200,a),(200,b)|all:true"
        )

    field_name = raw_parts[0].strip()
    pairs_part = raw_parts[1].strip()
    all_part = raw_parts[2]

    if not field_name:
        raise ValueError(f"Invalid mapping '{raw_line}': field name is empty.")

    pair_matches = PAIR_PATTERN.findall(pairs_part)
    if not pair_matches:
        raise ValueError(f"Invalid mapping '{raw_line}': no (tag,code) pair found.")

    selectors = []
    for tag, code, filter_code, filter_op, filter_value in pair_matches:
        selectors.append(
            {
                "tag": tag.strip(),
                "code": code.strip(),
                "filter_code": filter_code.strip() if filter_code else None,
                "filter_op": filter_op.strip() if filter_op else None,
                "filter_value": filter_value.strip() if filter_value else None,
            }
        )

    option_parts = [part for part in all_part.split(";") if part.strip()]
    if not option_parts:
        raise ValueError(
            f"Invalid mapping '{raw_line}': third section must be all:true or all:false."
        )

    all_split = [part.strip() for part in option_parts[0].split(":", maxsplit=1)]
    if len(all_split) != 2 or all_split[0].lower() != "all":
        raise ValueError(
            f"Invalid mapping '{raw_line}': third section must start with all:true or all:false."
        )

    get_all = all_split[1].lower() in {"true", "1", "yes", "y"}

    separator = " "
    list_separator = " | "

    for option in option_parts[1:]:
        option_split = option.split(":", maxsplit=1)
        if len(option_split) != 2:
            raise ValueError(
                f"Invalid mapping '{raw_line}': malformed option '{option}'."
            )

        option_name = option_split[0].strip().lower()
        option_value = option_split[1]

        if option_name in {"sep", "separator"}:
            separator = bytes(option_value, "utf-8").decode("unicode_escape")
        elif option_name in {"list_sep", "list_separator"}:
            list_separator = bytes(option_value, "utf-8").decode("unicode_escape")
        else:
            raise ValueError(
                f"Invalid mapping '{raw_line}': unknown option '{option_name}'."
            )

    return {
        "field_name": field_name,
        "selectors": selectors,
        "get_all": get_all,
        "separator": separator,
        "list_separator": list_separator,
    }


def build_mappings(mapping_lines: list[str]) -> list[dict]:
    mappings = []
    for line in mapping_lines:
        clean = line.strip()
        if clean:
            mappings.append(parse_mapping_line(clean))
    if not mappings:
        raise ValueError("No metadata mapping provided.")
    return mappings


def build_dataframe(
    ppns: list[str],
    mappings: list[dict],
    sleep_s: float = 0.0,
) -> pd.DataFrame:
    if not ppns:
        raise ValueError("No PPN found in input list.")

    rows = []
    with requests.Session() as session:
        for idx, ppn in enumerate(ppns, start=1):
            row = {"PPN": ppn}
            try:
                root = fetch_ppn_xml(ppn=ppn, session=session)
            except Exception as exc:
                row["error"] = str(exc)
                for mapping in mappings:
                    row[mapping["field_name"]] = None
                rows.append(row)
                continue

            for mapping in mappings:
                value = extract_field_robust(
                    root=root,
                    selectors=mapping["selectors"],
                    get_all=mapping["get_all"],
                    subfield_separator=mapping["separator"],
                )
                if isinstance(value, list):
                    row[mapping["field_name"]] = mapping["list_separator"].join(value)
                else:
                    row[mapping["field_name"]] = value

            rows.append(row)

            if sleep_s > 0:
                time.sleep(sleep_s)

            if idx % 50 == 0:
                eprint(f"Processed {idx}/{len(ppns)} PPNs")

    return pd.DataFrame(rows)


def harvest_metadata_from_ppn_with_exports(
    ppns: list[str],
    metadata_mappings: list[str],
    sleep_s: float = 0.0,
) -> pd.DataFrame:
    mappings = build_mappings(metadata_mappings)
    return build_dataframe(ppns=ppns, mappings=mappings, sleep_s=sleep_s)


def mappings_from_dict(mapping_dict: dict) -> list[str]:
    return [
        item["mapping"]
        for item in mapping_dict.values()
        if isinstance(item, dict) and "mapping" in item
    ]


def transform_metadata_to_templated_json(df_metadata: pd.DataFrame) -> pd.DataFrame:
    df_out = df_metadata[["PPN"]].copy()
    df_out["sudoc_record_templated"] = df_metadata.drop(columns=["PPN"]).apply(
        lambda row: safe_json_dumps(row.to_dict()),
        axis=1,
    )
    return df_out


def run_pipeline(
    theses_csv: str,
    memoires_csv: str,
    cases_api_url: str,
    outliers: list[str],
    sleep_ppn_lookup: float = 0.0,
    sleep_xml_lookup: float = 0.0,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    eprint("Loading input CSV files...")
    theses, memoires = load_inputs(theses_csv=theses_csv, memoires_csv=memoires_csv)

    eprint("Resolving thesis OAI IDs to SUDOC PPN...")
    theses = enrich_theses_with_ppn(theses=theses, sleep_s=sleep_ppn_lookup)

    eprint("Preparing merged documents dataframe...")
    df_documents = prepare_documents_dataframe(theses=theses, memoires=memoires)

    eprint("Resolving case_id / image_uri from cases API...")
    df_documents = attach_case_info(df=df_documents, cases_api_url=cases_api_url)

    eprint("Flagging annotation outliers...")
    df_documents = flag_outliers(df=df_documents, outliers=outliers)

    thesis_fields = mappings_from_dict(DEFAULT_THESES_FIELDS)
    dissertation_fields = mappings_from_dict(DEFAULT_DISSERTATION_FIELDS)

    thesis_ppn_list = [
        str(ppn)
        for ppn in df_documents.loc[df_documents["doc_type"] == "these", "ppn"].dropna().tolist()
        if str(ppn).strip()
    ]
    memoire_ppn_list = [
        str(ppn)
        for ppn in df_documents.loc[df_documents["doc_type"] == "memoire", "ppn"].dropna().tolist()
        if str(ppn).strip()
    ]

    eprint(f"Harvesting thesis metadata from {len(thesis_ppn_list)} PPNs...")
    df_thesis_metadata = harvest_metadata_from_ppn_with_exports(
        ppns=thesis_ppn_list,
        metadata_mappings=thesis_fields,
        sleep_s=sleep_xml_lookup,
    )

    eprint(f"Harvesting dissertation metadata from {len(memoire_ppn_list)} PPNs...")
    df_memoire_metadata = harvest_metadata_from_ppn_with_exports(
        ppns=memoire_ppn_list,
        metadata_mappings=dissertation_fields,
        sleep_s=sleep_xml_lookup,
    )

    df_thesis_transformed = transform_metadata_to_templated_json(df_thesis_metadata)
    df_memoire_transformed = transform_metadata_to_templated_json(df_memoire_metadata)

    df_templated = pd.concat(
        [df_thesis_transformed, df_memoire_transformed],
        ignore_index=True,
    )

    df_documents = df_documents.dropna(subset=["image_uri"]).copy()

    return df_documents, df_thesis_metadata, df_memoire_metadata, df_templated


def save_outputs(
    df_documents: pd.DataFrame,
    df_thesis_metadata: pd.DataFrame,
    df_memoire_metadata: pd.DataFrame,
    df_templated: pd.DataFrame,
    output_prefix: str,
) -> None:
    output_prefix = str(Path(output_prefix))
    documents_path = f"{output_prefix}_documents.csv"
    thesis_path = f"{output_prefix}_theses_metadata.csv"
    memoires_path = f"{output_prefix}_memoires_metadata.csv"
    templated_path = f"{output_prefix}_templated_records.csv"

    df_documents.to_csv(documents_path, index=False)
    df_thesis_metadata.to_csv(thesis_path, index=False)
    df_memoire_metadata.to_csv(memoires_path, index=False)
    df_templated.to_csv(templated_path, index=False)

    print(f"Saved -> {documents_path}")
    print(f"Saved -> {thesis_path}")
    print(f"Saved -> {memoires_path}")
    print(f"Saved -> {templated_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--theses-csv",
        default="_sample_filtered_humatheque_theses_diffusable_openaccess_flat.csv",
        help="CSV containing at least column: oai_id",
    )
    parser.add_argument(
        "--memoires-csv",
        default="_sample_filtered_memoires_dumas_openaccess_flat.csv",
        help="CSV containing at least columns: ppn, hal_id",
    )
    parser.add_argument(
        "--cases-api-url",
        default="https://data.smartbiblia.fr/cases?limit=300",
        help="Cases API endpoint used to resolve base_id -> case_id / image_uri",
    )
    parser.add_argument(
        "--output-prefix",
        default="humatheque_harvest",
        help="Prefix for output CSV files",
    )
    parser.add_argument(
        "--sleep-ppn-lookup",
        type=float,
        default=0.0,
        help="Optional sleep between PPN lookup calls",
    )
    parser.add_argument(
        "--sleep-xml-lookup",
        type=float,
        default=0.0,
        help="Optional sleep between XML lookup calls",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    (
        df_documents,
        df_thesis_metadata,
        df_memoire_metadata,
        df_templated,
    ) = run_pipeline(
        theses_csv=args.theses_csv,
        memoires_csv=args.memoires_csv,
        cases_api_url=args.cases_api_url,
        outliers=DEFAULT_OUTLIERS,
        sleep_ppn_lookup=args.sleep_ppn_lookup,
        sleep_xml_lookup=args.sleep_xml_lookup,
    )

    save_outputs(
        df_documents=df_documents,
        df_thesis_metadata=df_thesis_metadata,
        df_memoire_metadata=df_memoire_metadata,
        df_templated=df_templated,
        output_prefix=args.output_prefix,
    )


if __name__ == "__main__":
    main()