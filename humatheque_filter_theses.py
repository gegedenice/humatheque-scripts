#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "pandas",
# ]
# ///
"""
Filter + enrich a STAR export CSV (theses_diffusable_openaccess_flat.csv)
for Humathèque institutions and add a human-readable DDC set name.

Usage:
  uv run humatheque_filter_theses.py --input /path/theses_diffusable_openaccess_flat.csv
  uv run humatheque_filter_theses.py --input /path/theses_diffusable_openaccess_flat.csv --output /path/_filtered_humatheque_theses_diffusable_openaccess_flat.csv --etabs EHES EPHE PA13 PA01
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd


# --- Default DDC setSpec -> label mapping ---
# Source: https://theses.fr/schemas/tef/recommandation/oai_sets.html
DEFAULT_SETSPECS = [
  {
    "setspec": "ddc:000",
    "setname": "Informatique, information, généralités",
    "desc_en": "Data processing, information science, general collection"
  },
  {
    "setspec": "ddc:004",
    "setname": "Informatique",
    "desc_en": "Data processing, computer science"
  },
  {
    "setspec": "ddc:020",
    "setname": "Bibliothéconomie et sciences de l'information",
    "desc_en": "Library and information sciences"
  },
  {
    "setspec": "ddc:060",
    "setname": "Organisations générales et muséologie",
    "desc_en": "General organizations and museology"
  },
  {
    "setspec": "ddc:070",
    "setname": "Médias d'information, journalisme, édition",
    "desc_en": "News media, journalism, publishing"
  },
  {
    "setspec": "ddc:090",
    "setname": "Manuscrits et livres rares",
    "desc_en": "Manuscripts and rare books"
  },
  {
    "setspec": "ddc:100",
    "setname": "Philosophie, psychologie",
    "desc_en": "Philosophy, psychology"
  },
  {
    "setspec": "ddc:110",
    "setname": "Métaphysique",
    "desc_en": "Metaphysics"
  },
  {
    "setspec": "ddc:120",
    "setname": "Epistémologie, causalité, genre humain",
    "desc_en": "Epistemology, causation, humankind"
  },
  {
    "setspec": "ddc:130",
    "setname": "Phénomènes paranormaux, seudosciences",
    "desc_en": "Parapsychology, occultism"
  },
  {
    "setspec": "ddc:140",
    "setname": "Les divers systèmes et écoles philosophiques",
    "desc_en": "Specific philosophical schools"
  },
  {
    "setspec": "ddc:150",
    "setname": "Psychologie",
    "desc_en": "Psychology"
  },
  {
    "setspec": "ddc:160",
    "setname": "Logique",
    "desc_en": "Logic"
  },
  {
    "setspec": "ddc:170",
    "setname": "Morale (éthique)",
    "desc_en": "Ethics"
  },
  {
    "setspec": "ddc:180",
    "setname": "Philosophie de l'Antiquité, du Moyen Âge, de l'Orient",
    "desc_en": "Ancient, medieval, eastern philosophy"
  },
  {
    "setspec": "ddc:190",
    "setname": "Philosophie occidentale moderne et philosophies non orientales",
    "desc_en": "Modern western philosophy"
  },
  {
    "setspec": "ddc:200",
    "setname": "Religion",
    "desc_en": "Religion"
  },
  {
    "setspec": "ddc:210",
    "setname": "Philosophie et théorie de la religion",
    "desc_en": "Philosophy and theory of religion"
  },
  {
    "setspec": "ddc:220",
    "setname": "Bible",
    "desc_en": "Bible"
  },
  {
    "setspec": "ddc:230",
    "setname": "Théologie chrétienne",
    "desc_en": "Christian theology"
  },
  {
    "setspec": "ddc:240",
    "setname": "Théologie morale et pratiques chrétiennes",
    "desc_en": "Christian moral and devotional theology"
  },
  {
    "setspec": "ddc:250",
    "setname": "Eglises locales, ordres religieux chrétiens",
    "desc_en": "Local church. Christian orders"
  },
  {
    "setspec": "ddc:260",
    "setname": "Théologie chrétienne et société, ecclésiologie",
    "desc_en": "Social and ecclesiastical theology"
  },
  {
    "setspec": "ddc:270",
    "setname": "Histoire et géographie du christianisme et de l'Eglise chrétienne",
    "desc_en": "History of Christianity and Christian church"
  },
  {
    "setspec": "ddc:280",
    "setname": "Confessions et sectes de l'Eglise chrétienne",
    "desc_en": "Christian denominations and sects"
  },
  {
    "setspec": "ddc:290",
    "setname": "Autres religions",
    "desc_en": "Other religions"
  },
  {
    "setspec": "ddc:300",
    "setname": "Sciences sociales, sociologie, anthropologie",
    "desc_en": "Social sciences, sociology, anthropology"
  },
  {
    "setspec": "ddc:310",
    "setname": "Statistiques générales",
    "desc_en": "Collections of general statistics"
  },
  {
    "setspec": "ddc:320",
    "setname": "Science politique",
    "desc_en": "Political science"
  },
  {
    "setspec": "ddc:330",
    "setname": "Economie",
    "desc_en": "Economics"
  },
  {
    "setspec": "ddc:340",
    "setname": "Droit",
    "desc_en": "Law"
  },
  {
    "setspec": "ddc:350",
    "setname": "Administration publique. Arts et science militaires",
    "desc_en": "Public administration. Military science"
  },
  {
    "setspec": "ddc:360",
    "setname": "Problèmes et services sociaux",
    "desc_en": "Social problems and services"
  },
  {
    "setspec": "ddc:370",
    "setname": "Education et enseignement",
    "desc_en": "Education and teaching"
  },
  {
    "setspec": "ddc:380",
    "setname": "Commerce, communications, transports",
    "desc_en": "Commerce, communication, transportation"
  },
  {
    "setspec": "ddc:390",
    "setname": "Ethnologie",
    "desc_en": "Ethnology"
  },
  {
    "setspec": "ddc:400",
    "setname": "Langues et linguistique",
    "desc_en": "Language and linguistics"
  },
  {
    "setspec": "ddc:410",
    "setname": "Linguistique générale",
    "desc_en": "Linguistics"
  },
  {
    "setspec": "ddc:420",
    "setname": "Langue anglaise. Anglo-saxon",
    "desc_en": "English and old english (anglo-saxon)"
  },
  {
    "setspec": "ddc:430",
    "setname": "Langues germaniques. Allemand",
    "desc_en": "Germanic languages. German"
  },
  {
    "setspec": "ddc:440",
    "setname": "Langues romanes. Français",
    "desc_en": "Romance languages. French"
  },
  {
    "setspec": "ddc:450",
    "setname": "Langues italienne, roumaine, rhéto-romane",
    "desc_en": "Italian, romanian, rhaeto-romanic languages"
  },
  {
    "setspec": "ddc:460",
    "setname": "Langues espagnole et portugaise",
    "desc_en": "Spanish and Portuguese languages"
  },
  {
    "setspec": "ddc:470",
    "setname": "Langues italiques. Latin",
    "desc_en": "Italic languages. Latin"
  },
  {
    "setspec": "ddc:480",
    "setname": "Langues helléniques. Grec classique",
    "desc_en": "Hellenic languages. Classical greek"
  },
  {
    "setspec": "ddc:490",
    "setname": "Autres langues",
    "desc_en": "Other languages"
  },
  {
    "setspec": "ddc:500",
    "setname": "Sciences de la nature et mathématiques",
    "desc_en": "Natural sciences and mathematics"
  },
  {
    "setspec": "ddc:510",
    "setname": "Mathématiques",
    "desc_en": "Mathematics"
  },
  {
    "setspec": "ddc:520",
    "setname": "Astronomie, cartographie, géodésie",
    "desc_en": "Astronomy, cartography (map making), geodesy"
  },
  {
    "setspec": "ddc:530",
    "setname": "Physique",
    "desc_en": "Physics"
  },
  {
    "setspec": "ddc:540",
    "setname": "Chimie, minéralogie, cristallographie",
    "desc_en": "Chemistry, mineralogy, crystallography"
  },
  {
    "setspec": "ddc:550",
    "setname": "Sciences de la terre",
    "desc_en": "Earth sciences"
  },
  {
    "setspec": "ddc:560",
    "setname": "Paléontologie. Paléozoologie",
    "desc_en": "Paleontology. Paleozoology"
  },
  {
    "setspec": "ddc:570",
    "setname": "Sciences de la vie, biologie, biochimie",
    "desc_en": "Life sciences, biology, biochemistry"
  },
  {
    "setspec": "ddc:580",
    "setname": "Plantes. Botanique",
    "desc_en": "Botanical Sciences"
  },
  {
    "setspec": "ddc:590",
    "setname": "Animaux. Zoologie",
    "desc_en": "Animals. Zoology"
  },
  {
    "setspec": "ddc:600",
    "setname": "Technologie (Sciences appliquées)",
    "desc_en": "Technology (applied sciences)"
  },
  {
    "setspec": "ddc:610",
    "setname": "Médecine et santé",
    "desc_en": "Medicine and health"
  },
  {
    "setspec": "ddc:620",
    "setname": "Sciences de l'ingénieur",
    "desc_en": "Engineering"
  },
  {
    "setspec": "ddc:630",
    "setname": "Agronomie, agriculture et médecine vétérinaire",
    "desc_en": "Agronomy, agriculture, veterinary medicine"
  },
  {
    "setspec": "ddc:640",
    "setname": "Economie domestique. Vie familiale",
    "desc_en": "Home and family management"
  },
  {
    "setspec": "ddc:650",
    "setname": "Gestion et organisation de l'entreprise",
    "desc_en": "Management and office management"
  },
  {
    "setspec": "ddc:660",
    "setname": "Génie chimique, technologies alimentaires",
    "desc_en": "Chemical engineering, food technology"
  },
  {
    "setspec": "ddc:670",
    "setname": "Fabrication industrielle",
    "desc_en": "Manufacturing"
  },
  {
    "setspec": "ddc:680",
    "setname": "Fabrication de produits à usages spécifiques",
    "desc_en": "Manufacture for specific uses"
  },
  {
    "setspec": "ddc:690",
    "setname": "Bâtiments",
    "desc_en": "Buildings"
  },
  {
    "setspec": "ddc:700",
    "setname": "Arts. Beaux-arts et arts décoratifs",
    "desc_en": "Art. Fine and decorative arts"
  },
  {
    "setspec": "ddc:710",
    "setname": "Urbanisme",
    "desc_en": "Urban planning"
  },
  {
    "setspec": "ddc:720",
    "setname": "Architecture",
    "desc_en": "Architecture"
  },
  {
    "setspec": "ddc:730",
    "setname": "Arts plastiques. Sculpture",
    "desc_en": "Plastic arts. Sculpture"
  },
  {
    "setspec": "ddc:740",
    "setname": "Dessin. Arts décoratifs",
    "desc_en": "Drawing. Decorative arts"
  },
  {
    "setspec": "ddc:750",
    "setname": "Peinture",
    "desc_en": "Painting"
  },
  {
    "setspec": "ddc:760",
    "setname": "Arts graphiques",
    "desc_en": "Graphic arts"
  },
  {
    "setspec": "ddc:770",
    "setname": "Photographie et les photographies, art numérique",
    "desc_en": "Photography and photographs and computer art"
  },
  {
    "setspec": "ddc:780",
    "setname": "Musique",
    "desc_en": "Music"
  },
  {
    "setspec": "ddc:790",
    "setname": "Arts du spectacle, loisirs",
    "desc_en": "Recreational and performing arts"
  },
  {
    "setspec": "ddc:796",
    "setname": "Sport",
    "desc_en": "Sports"
  },
  {
    "setspec": "ddc:800",
    "setname": "Histoire et critique littéraires, rhétorique",
    "desc_en": "History and critical literature, rhetoric"
  },
  {
    "setspec": "ddc:810",
    "setname": "Littérature américaine en anglais",
    "desc_en": "American literature in english"
  },
  {
    "setspec": "ddc:820",
    "setname": "Littératures anglaise et anglo-saxonne",
    "desc_en": "English and old english (anglo-saxon) literatures"
  },
  {
    "setspec": "ddc:830",
    "setname": "Littérature allemande",
    "desc_en": "German literature"
  },
  {
    "setspec": "ddc:840",
    "setname": "Littérature de langues romanes. Littérature française",
    "desc_en": "Literatures of Romance languages. French literature"
  },
  {
    "setspec": "ddc:850",
    "setname": "Littérature italienne",
    "desc_en": "Italian literature"
  },
  {
    "setspec": "ddc:860",
    "setname": "Littératures espagnole et portugaise",
    "desc_en": "Spanish and Portuguese literatures"
  },
  {
    "setspec": "ddc:870",
    "setname": "Littérature latine",
    "desc_en": "Latin literature"
  },
  {
    "setspec": "ddc:880",
    "setname": "Littérature grecque",
    "desc_en": "Greek literature"
  },
  {
    "setspec": "ddc:890",
    "setname": "Littératures des autres langues",
    "desc_en": "Literatures of other languages"
  },
  {
    "setspec": "ddc:900",
    "setname": "Géographie et histoire",
    "desc_en": "Geography and history"
  },
  {
    "setspec": "ddc:910",
    "setname": "Géographie et voyages",
    "desc_en": "Geography and travel"
  },
  {
    "setspec": "ddc:920",
    "setname": "Biographies générales, généalogie, emblèmes",
    "desc_en": "Biography, genealogy and insignia"
  },
  {
    "setspec": "ddc:930",
    "setname": "Histoire ancienne et préhistoire",
    "desc_en": "History of ancient world and prehistory"
  },
  {
    "setspec": "ddc:940",
    "setname": "Histoire moderne et contemporaine de l'Europe",
    "desc_en": "Modern and ancient history of Europe"
  },
  {
    "setspec": "ddc:944",
    "setname": "Histoire générale de la France",
    "desc_en": "General history of France"
  },
  {
    "setspec": "ddc:950",
    "setname": "Histoire générale de l'Asie, Orient, Extrême-Orient",
    "desc_en": "General history of Asia, Orient, Far East"
  },
  {
    "setspec": "ddc:960",
    "setname": "Histoire générale de l'Afrique",
    "desc_en": "General history of Africa"
  },
  {
    "setspec": "ddc:970",
    "setname": "Histoire générale de l'Amérique du Nord",
    "desc_en": "General history of North America"
  },
  {
    "setspec": "ddc:980",
    "setname": "Histoire générale de l'Amérique du Sud",
    "desc_en": "General history of South America"
  },
  {
    "setspec": "ddc:990",
    "setname": "Histoire générale des autres parties du monde, des mondes extraterrestres. Iles du Pacifique",
    "desc_en": "General history of other parts of world, of extraterrestrial worlds, of Pacific Ocean Islands"
  }
]


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Humathèque theses CSV filter + DDC label enrichment")
    p.add_argument(
        "--input",
        required=True,
        help="Input CSV path (e.g. theses_diffusable_openaccess_flat.csv)",
    )
    p.add_argument(
        "--output",
        default=None,
        help="Output CSV path (default: same dir, prefixed with _filtered_humatheque_)",
    )
    p.add_argument(
        "--etabs",
        nargs="+",
        default=["EHES", "EPHE", "PA13", "PA01"],
        help="Accepted set_etab codes",
    )
    p.add_argument(
        "--encoding",
        default="utf-8",
        help="CSV encoding (default: utf-8)",
    )
    return p.parse_args(argv)


def build_ddc_map(setspecs: list[dict]) -> dict[str, str]:
    # setspec -> setname
    return {item["setspec"]: item["setname"] for item in setspecs if item.get("setspec") and item.get("setname")}


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    in_path = Path(args.input)
    if not in_path.exists():
        print(f"[ERROR] Input not found: {in_path}", file=sys.stderr)
        return 2

    out_path = Path(args.output) if args.output else in_path.with_name(f"_filtered_humatheque_{in_path.name}")

    df = pd.read_csv(in_path, encoding=args.encoding)

    # Filter on set_etab
    if "set_etab" not in df.columns:
        print("[ERROR] Missing column 'set_etab' in input CSV.", file=sys.stderr)
        return 3
    df_filter = df[df["set_etab"].isin(args.etabs)].copy()

    # Add setname (label) next to set_ddc
    if "set_ddc" not in df_filter.columns:
        print("[ERROR] Missing column 'set_ddc' in input CSV.", file=sys.stderr)
        return 4

    ddc_map = build_ddc_map(DEFAULT_SETSPECS)
    df_filter["setname"] = df_filter["set_ddc"].map(ddc_map)

    # Reorder columns: place 'setname' right after 'set_ddc'
    cols = df_filter.columns.tolist()
    if "setname" in cols and "set_ddc" in cols:
        cols.remove("setname")
        set_ddc_idx = cols.index("set_ddc")
        cols.insert(set_ddc_idx + 1, "setname")
        df_filter = df_filter[cols]

    df_filter.to_csv(out_path, index=False, encoding=args.encoding)
    print(f"Done. Rows in={len(df)}, rows out={len(df_filter)}, output={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))