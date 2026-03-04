#!/usr/bin/env python3
"""
Como Voto - Data Processor / Site Generator
============================================
Reads scraped voting data from consolidated JSON files in data/
and generates aggregated JSON files for the interactive frontend
(docs/data/ directory for GitHub Pages).

Features:
  - Reads compact consolidated DB (one file per chamber)
  - Cross-chamber name matching (legislators in both Diputados & Senadores)
  - Law grouping (EN GENERAL + EN PARTICULAR articles -> single law)
  - Common law name mapping
  - Waffle visualization data output
  - Contested-only alignment calculation
"""

from __future__ import annotations

import json
import os
import re
import sys
import logging
import unicodedata
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import time

# Import ConsolidatedDB and helpers from scraper
from scraper import ConsolidatedDB, classify_bloc, VOTE_DECODE

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DOCS_DIR = BASE_DIR / "docs"
DOCS_DATA_DIR = DOCS_DIR / "data"
FOTOS_DIR = DOCS_DIR / "fotos"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("processor")


# ---------------------------------------------------------------------------
# Party classification for law-search breakdown
# ---------------------------------------------------------------------------
# Unlike classify_bloc() (PJ/PRO/LLA/OTHER coalitions used for alignment
# tracking), this classifies into the five actual major parties by their
# real bloc name at the time of voting.

_PJ_PARTY_KW = [
    "justicialista", "frente de todos", "frente para la victoria",
    "unión por la patria", "union por la patria",
    "frente renovador", "peronismo", "peronista",
    "frente cívico por santiago", "frente civico por santiago",
    "movimiento popular neuquino", "pj ", "pj frente",
    "unidad ciudadana", "frente nacional y popular",
]
_UCR_PARTY_KW = [
    "ucr", "unión cívica radical", "union civica radical",
    "radical", "evolución radical", "evolucion radical",
    "democracia para siempre",
]
_PRO_PARTY_KW = [
    "propuesta republicana", "union pro", "unión pro",
    "frente pro", "cambiemos", "juntos por el cambio",
]
_LLA_PARTY_KW = ["la libertad avanza", "libertad avanza"]
_CC_PARTY_KW = ["coalición cívica", "coalicion civica", "a.r.i"]

_PRO_WORD_RE = re.compile(r"\bpro\b")

# Regex helpers for extracting section labels from votación titles
# Senado reference suffixes: use known code prefixes (PE, CD, JGM, S)
# to avoid eating roman numerals concatenated before them
_REF_SUFFIX_RE = re.compile(
    r'\.?(?:PE|CD|JGM|S)-\d+/\d+(?:-[A-Z]+)?[,.]?\s*O\.?\s*D\.?\s*\d+/\d+[,.]?\s*$',
    re.IGNORECASE,
)
_OD_SUFFIX_RE = re.compile(r'[.,]?\s*O\.?\s*D\.?\s*\d+/\d+[,.]?\s*$', re.IGNORECASE)
_OD_PREFIX_RE = re.compile(
    r'^(?:VOTACI[OÓ]N:?\s*)?(?:O\.?\s*D\.?\s*\d+\s*[-–—]\s*)?',
    re.IGNORECASE,
)
_EXP_PREFIX_RE = re.compile(
    r'^(?:EXP(?:EDIENTE)?\.?\s*\S+\s*[-–—]\s*'
    r'(?:O\.?\s*D\.?\s*\d+\s*[-–—]\s*)?)?',
    re.IGNORECASE,
)
_DICT_RE = re.compile(r'\bDICT\.\s*DE\s*(?:MAY|MIN)\.?\s*', re.IGNORECASE)
_EN_GRAL_RE = re.compile(
    r'(?:VOT\.?\s*)?EN\s+G(?:ENERAL|RAL)\.?', re.IGNORECASE
)
_TITULO_RE = re.compile(r'T[IÍ]TULO\s+([IVXLCDM]+|\d+)', re.IGNORECASE)
_CAPITULO_RE = re.compile(
    r'CAP[IÍ]?(?:TULO)?[.\s]\s*([IVXLCDM]+|\d+)', re.IGNORECASE
)
_ARTICULO_RE = re.compile(
    r'ARTS?[IÍ]?(?:CULOS?)?[.\s°º]\s*(\d+(?:\s*[°º])?)'
    r'(?:\s*(?:AL?|Y|,)\s*(\d+(?:\s*[°º])?))?',
    re.IGNORECASE,
)
_INCISO_RE = re.compile(
    r'INCISOS?\s+([A-Z](?:\s*(?:AL|Y|,)\s*[A-Z])*)', re.IGNORECASE
)


def _clean_votacion_title(title: str) -> str:
    """Strip noise from a votación title for section extraction."""
    s = title.strip()
    # Strip Senado reference suffixes (PE-159/25-PL,O.D. 699/2025)
    s = _REF_SUFFIX_RE.sub('', s)
    # Strip standalone O.D. NNN/NNNN at end
    s = _OD_SUFFIX_RE.sub('', s)
    # Strip Diputados O.D. N - prefix
    s = _OD_PREFIX_RE.sub('', s)
    # Strip Exp. / Expediente prefix
    s = _EXP_PREFIX_RE.sub('', s)
    # Strip 'Orden del Día NNN -' prefix
    s = re.sub(r'^Orden\s+del\s+D[ií]a\s+\d+\s*[-–—.]\s*',
               '', s, flags=re.IGNORECASE)
    # Strip DICT. DE MAY.
    s = _DICT_RE.sub('', s)
    return s.strip(' .,')


def extract_section_label(title: str, vtype: str = "") -> str:
    """Extract a descriptive section label from a votación title.

    Returns labels like 'En General', 'Título I', 'Título V, Cap. II,
    Arts. 87 a 91', etc.  Falls back to *vtype* or empty string.
    """
    cleaned = _clean_votacion_title(title)

    # En General
    if _EN_GRAL_RE.search(cleaned) or 'EN GENERAL' in vtype.upper():
        return 'En General'

    parts: list[str] = []
    for m in _TITULO_RE.finditer(cleaned):
        parts.append(f'Título {m.group(1)}')
    for m in _CAPITULO_RE.finditer(cleaned):
        parts.append(f'Cap. {m.group(1)}')
    for m in _ARTICULO_RE.finditer(cleaned):
        n1 = m.group(1).replace('°', '').replace('º', '').strip()
        if m.group(2):
            n2 = m.group(2).replace('°', '').replace('º', '').strip()
            parts.append(f'Arts. {n1} a {n2}')
        else:
            parts.append(f'Art. {n1}')
    for m in _INCISO_RE.finditer(cleaned):
        parts.append(f'Inc. {m.group(1)}')

    if parts:
        # Deduplicate while preserving order
        seen: set[str] = set()
        unique: list[str] = []
        for p in parts:
            if p not in seen:
                seen.add(p)
                unique.append(p)
        return ', '.join(unique)

    # Fallback: detect 'en particular' in title text
    if re.search(r'en\s+particular', cleaned, re.IGNORECASE):
        return 'En Particular'

    # Fallback to vtype (normalise case)
    vtype_clean = vtype.strip()
    if vtype_clean:
        vu = vtype_clean.upper()
        if 'EN PARTICULAR' in vu:
            return 'En Particular'
        if 'EN GENERAL' in vu:
            return 'En General'
        return vtype_clean
    return ''


def classify_bloc_party(bloc_name: str) -> str:
    """Classify a bloc name into one of five real parties or OTHER.

    Returns one of: PJ, UCR, PRO, LLA, CC, OTHER.
    """
    name = bloc_name.lower().strip()
    for kw in _PJ_PARTY_KW:
        if kw in name:
            return "PJ"
    for kw in _UCR_PARTY_KW:
        if kw in name:
            return "UCR"
    if _PRO_WORD_RE.search(name):
        return "PRO"
    for kw in _PRO_PARTY_KW:
        if kw in name:
            return "PRO"
    for kw in _LLA_PARTY_KW:
        if kw in name:
            return "LLA"
    for kw in _CC_PARTY_KW:
        if kw in name:
            return "CC"
    return "OTROS"
 

# ---------------------------------------------------------------------------
# Province name normalization
# ---------------------------------------------------------------------------
_PROVINCE_CANONICAL: dict[str, str] = {
    "buenos aires": "Buenos Aires",
    "c.a.b.a.": "CABA",
    "capital federal": "CABA",
    "ciudad autonoma de buenos aires": "CABA",
    "ciudad autónoma de buenos aires": "CABA",
    "catamarca": "Catamarca",
    "chaco": "Chaco",
    "chubut": "Chubut",
    "corrientes": "Corrientes",
    "cordoba": "Córdoba",
    "córdoba": "Córdoba",
    "entre rios": "Entre Ríos",
    "entre ríos": "Entre Ríos",
    "formosa": "Formosa",
    "jujuy": "Jujuy",
    "la pampa": "La Pampa",
    "la rioja": "La Rioja",
    "mendoza": "Mendoza",
    "misiones": "Misiones",
    "neuquen": "Neuquén",
    "neuquén": "Neuquén",
    "rio negro": "Río Negro",
    "río negro": "Río Negro",
    "salta": "Salta",
    "san juan": "San Juan",
    "san luis": "San Luis",
    "santa cruz": "Santa Cruz",
    "santa fe": "Santa Fe",
    "santiago del estero": "Santiago del Estero",
    "tierra del fuego": "Tierra del Fuego",
    "tierra del fuego, antartida e islas del atlantico sur": "Tierra del Fuego",
    "tierra del fuego, antártida e islas del atlántico sur": "Tierra del Fuego",
    "tucuman": "Tucumán",
    "tucumán": "Tucumán",
}


def normalize_province(province: str) -> str:
    """Return the canonical Title-Case province name."""
    key = unicodedata.normalize("NFC", province.strip().lower())
    return _PROVINCE_CANONICAL.get(key, province.strip())


# ---------------------------------------------------------------------------
# Common law names mapping
# ---------------------------------------------------------------------------
COMMON_LAW_NAMES = [
        # --- Major 2023-2024+ laws ---
        (['bases y puntos de partida', "ley de bases", "ley bases"],
         "Ley Bases"),
        (["medidas fiscales paliativas", "paquete fiscal"],
         "Paquete Fiscal"),
        (["régimen de incentivo para grandes inversiones",
            "regimen de incentivo para grandes inversiones", "rigi"],
         "RIGI"),
        (["decreto de necesidad y urgencia 70", "dnu 70"],
         "DNU 70/2023"),

        # --- Economy & taxes ---
        (["impuesto a las ganancias"], "Impuesto a las Ganancias"),
        (["bienes personales"], "Bienes Personales"),
        (["presupuesto general", "presupuesto de la administración",
            "presupuesto de la administracion", "ley de presupuesto"],
         "Presupuesto"),
        (["movilidad jubilatoria", "movilidad previsional"],
         "Movilidad Jubilatoria"),
        (["régimen previsional", "regimen previsional"],
         "Regimen Previsional"),
        (["privatización", "privatizacion"], "Privatizaciones"),
        (["deuda externa", "reestructuración de deuda",
            "reestructuracion de deuda"], "Deuda Externa"),
        (["monotributo", "régimen simplificado para pequeños contribuyentes",
            "regimen simplificado para pequeños contribuyentes"],
         "Monotributo"),
        (["consenso fiscal"], "Consenso Fiscal"),
        (["fondo monetario internacional", "fondo monetario"],
         "FMI"),
        (["derechos de exportación", "derechos de exportacion",
            "retenciones agropecuarias"],
         "Retenciones / Derechos de Exportacion"),

        # --- Electoral & institutional ---
        (["boleta única", "boleta unica"], "Boleta Unica de Papel"),
        (["ficha limpia"], "Ficha Limpia"),
        (["régimen electoral", "regimen electoral",
            "código electoral", "codigo electoral"],
         "Regimen Electoral"),
        (["paridad de género", "paridad de genero"],
         "Paridad de Genero"),
        (["consejo de la magistratura"], "Consejo de la Magistratura"),

        # --- Codes ---
        (["código procesal penal", "codigo procesal penal"],
         "Codigo Procesal Penal"),
        (["código penal", "codigo penal"], "Codigo Penal"),
        (["código civil", "codigo civil"], "Codigo Civil"),
        (["código aduanero", "codigo aduanero"], "Codigo Aduanero"),

        # --- Justice & security ---
        (["juicio en ausencia"], "Juicio en Ausencia"),
        (["régimen penal juvenil", "penal juvenil"],
         "Regimen Penal Juvenil"),
        (["lavado de activos"], "Lavado de Activos"),
        (["extinción de dominio", "extincion de dominio"],
         "Extincion de Dominio"),
        (["inteligencia nacional", "agencia federal de inteligencia"],
         "Inteligencia Nacional"),
        (["narcotráfico", "narcotrafico"], "Narcotrafico"),

        # --- Labor ---
        (["reforma laboral", "modernización laboral",
            "modernizacion laboral"], "Reforma Laboral"),
        (["teletrabajo"], "Teletrabajo"),
        (["trabajo agrario"], "Trabajo Agrario"),

        # --- Housing & property ---
        (["ley de alquileres", "locaciones urbanas"],
         "Ley de Alquileres"),
        (["tierras rurales", "dominio nacional sobre la propiedad"],
         "Tierras Rurales"),

        # --- Social & rights ---
        (["interrupción voluntaria del embarazo",
            "interrupcion voluntaria del embarazo", "aborto"],
         "IVE / Aborto"),
        (["violencia de género", "violencia de genero"],
         "Violencia de Genero"),
        (["emergencia alimentaria"], "Emergencia Alimentaria"),
        (["matrimonio igualitario",
            "matrimonio entre personas del mismo sexo",
            "matrimonio civil",
            "código civil, sobre matrimonio",
            "codigo civil, sobre matrimonio"],
         "Matrimonio Igualitario"),
        (["identidad de género", "identidad de genero",
            "ley de identidad"],
         "Identidad de Genero"),
        (["salud mental"], "Salud Mental"),
        (["barrios populares"], "Barrios Populares"),

        # --- Education & science ---
        (["financiamiento universitario"], "Financiamiento Universitario"),
        (["educación sexual", "educacion sexual"], "Educacion Sexual"),
        (["financiamiento de la ciencia",
            "financiamiento científico", "financiamiento cientifico",
            "ciencia, tecnología e innovación",
            "ciencia, tecnologia e innovacion",
            "ciencia y tecnología", "ciencia y tecnologia"],
         "Financiamiento Cientifico"),

        # --- Health ---
        (["cannabis medicinal", "uso medicinal de la planta de cannabis"],
         "Cannabis Medicinal"),
        (["cadena de frío de los medicamentos",
            "cadena de frio de los medicamentos",
            "producción pública de medicamentos",
            "produccion publica de medicamentos"],
         "Ley de Medicamentos"),

        # --- Environment ---
        (["etiquetado frontal"], "Etiquetado Frontal"),
        (["humedales"], "Ley de Humedales"),
        (["manejo del fuego"], "Manejo del Fuego"),
        (["glaciares"], "Ley de Glaciares"),
        (["energías renovables", "energias renovables",
            "fuentes renovables de energía",
            "fuentes renovables de energia",
            "energía renovable", "energia renovable"],
         "Energias Renovables"),

        # --- Consumer & commerce ---
        (["góndolas", "gondolas"], "Ley de Gondolas"),
        (["economía del conocimiento", "economia del conocimiento"],
         "Economia del Conocimiento"),
        (["compre argentino", "compre nacional"], "Compre Argentino"),

        # --- Media & communication ---
        (["servicios de comunicación audiovisual",
            "servicios de comunicacion audiovisual", "ley de medios"],
         "Ley de Medios"),

        # --- Transparency ---
        (["acceso a la información pública",
            "acceso a la informacion publica"],
         "Acceso a Info. Publica"),

        # --- Transport & safety ---
        (["seguridad vial", "tránsito y seguridad vial",
            "transito y seguridad vial"],
         "Seguridad Vial"),
        (["ludopatía", "ludopatia", "apuestas en línea",
            "apuestas en linea", "juegos de azar y apuestas"],
         "Ludopatia / Apuestas Online"),

        # --- Other ---
        (["defensa nacional"], "Defensa Nacional"),
        (["inocencia fiscal"], "Inocencia Fiscal"),
]


def COMMON_NORM(s: str) -> str:
        return unicodedata.normalize('NFKD', (s or '')).encode('ascii', 'ignore').decode('ascii').lower()


# Precompute normalized keywords per rule so we don't re-normalize on
# every call to get_common_name.
_COMMON_LAW_RULES: list[tuple[list[str], str]] = []
for _keywords, _common_name in COMMON_LAW_NAMES:
        _COMMON_LAW_RULES.append(
                ([COMMON_NORM(kw) for kw in _keywords], _common_name)
        )


def _kw_matches(kw_norm: str, t_norm: str) -> bool:
        """Return True if *kw_norm* is found inside *t_norm*.

        Short tokens (<=4 chars) require a whole-word boundary match to
        avoid accidental substring hits; longer phrases use plain substring
        matching which is more forgiving with punctuation.
        """
        if len(kw_norm) <= 4:
                return bool(re.search(r"\b" + re.escape(kw_norm) + r"\b", t_norm))
        return kw_norm in t_norm


def get_common_name(title: str) -> str | None:
        """Return the best-matching common law name for *title*, or None.

        Every rule in COMMON_LAW_NAMES is evaluated against the title.
        For each rule we count how many of its keywords match and sum
        their lengths.  The rule with the highest score wins — scored by
        ``(total_matched_keyword_length, matched_token_count)`` — which
        avoids ambiguous first-match-wins behaviour and ensures more
        specific rules beat shorter / vaguer ones.
        """
        if not title:
                return None
        t_norm = COMMON_NORM(title)

        best_name: str | None = None
        best_score: tuple[int, int] = (0, 0)

        for norm_keywords, common_name in _COMMON_LAW_RULES:
                matched_length = 0
                matched_count = 0
                for kw_norm in norm_keywords:
                        if _kw_matches(kw_norm, t_norm):
                                matched_length += len(kw_norm)
                                matched_count += 1

                if matched_count > 0:
                        score = (matched_length, matched_count)
                        if score > best_score:
                                best_score = score
                                best_name = common_name

        return best_name


# ---------------------------------------------------------------------------
# Law grouping
# ---------------------------------------------------------------------------

def extract_law_group_key(votacion: dict) -> str:
    title = votacion.get("title", "")
    date = votacion.get("date", "")
    chamber = votacion.get("chamber", "")

    date_part = ""
    dm = re.search(r"(\d{2}/\d{2}/\d{4})", date)
    if dm:
        date_part = dm.group(1)

    # Extract O.D. / C.D. numbers
    od_match = re.search(
        r"(O\.?\s*D\.?\s*N?[°ºo]?\s*\d+(?:/\d+)?)", title, re.IGNORECASE
    )
    if od_match:
        od_num = re.sub(r"\s+", "", od_match.group(1).upper())
        return f"{chamber}|{od_num}|{date_part}"

    exp_match = re.search(
        r"(Exp(?:ediente)?\.?\s*N?[°ºo]?\s*[\d\-]+(?:/\d+)?)",
        title, re.IGNORECASE
    )
    if exp_match:
        exp_num = re.sub(r"\s+", "", exp_match.group(1).upper())
        return f"{chamber}|{exp_num}|{date_part}"

    # Strip vote-type markers to group related votes
    clean = title.upper()
    clean = re.sub(r"\s*-?\s*EN\s+(GENERAL|PARTICULAR)\s*", " ", clean)
    clean = re.sub(r"\s*-?\s*ART[IÍ]?CULO?\s+\d+.*", "", clean)
    clean = re.sub(r"\s*-?\s*ART\.?\s+\d+.*", "", clean)
    clean = re.sub(
        r"\s*-?\s*MODIFICACIONES?\s+(AL|DEL)\s+SENADO.*", "", clean
    )
    clean = re.sub(r"\s+", " ", clean).strip()

    if len(clean) > 15:
        return f"{chamber}|{clean[:100]}|{date_part}"

    return f"{chamber}|SINGLE|{votacion.get('id', '')}"


def build_law_groups(all_votaciones: list[dict]) -> dict:
    groups = defaultdict(lambda: {
        "votaciones": [], "title": "", "date": "",
        "common_name": None, "chamber": "",
    })

    for v in all_votaciones:
        key = extract_law_group_key(v)
        g = groups[key]
        g["votaciones"].append(v)
        if len(v.get("title", "")) > len(g["title"]):
            g["title"] = v.get("title", "")
        if not g["date"]:
            g["date"] = v.get("date", "")
        g["chamber"] = v.get("chamber", "")

    for key, g in groups.items():
        cn = get_common_name(g["title"])
        if cn:
            g["common_name"] = cn

    return dict(groups)


# ---------------------------------------------------------------------------
# Data loading (from consolidated JSON)
# ---------------------------------------------------------------------------

def load_all_votaciones_from_db(chamber: str) -> list[dict]:
    """Load all votaciones from a consolidated DB file, expanding them
    to the full format used by the rest of the pipeline."""
    db_path = DATA_DIR / f"{chamber}.json"
    if not db_path.exists():
        log.warning(f"Consolidated DB not found: {db_path}")
        return []

    db = ConsolidatedDB(db_path)
    db.load()
    log.info(f"Loaded {len(db.votaciones)} {chamber} votaciones from DB")
    return db.expand_all(chamber)


def clean_date(date_str: str) -> str:
    """Clean date string: extract DD/MM/YYYY and optional HH:MM."""
    m = re.search(r"(\d{2}/\d{2}/\d{4})", date_str)
    if not m:
        return date_str.strip()
    result = m.group(1)
    time_m = re.search(r"(\d{2}:\d{2})", date_str)
    if time_m:
        result += " - " + time_m.group(1)
    return result


def extract_year(date_str: str) -> int | None:
    match = re.search(r"(\d{4})", date_str)
    if match:
        return int(match.group(1))
    return None


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------

def compute_majority_vote(votes: list[dict], coalition: str) -> str:
    def _norm(s: str) -> str:
        return unicodedata.normalize('NFKD', (s or '')).encode('ascii', 'ignore').decode('ascii').upper()

    coalition_up = _norm(coalition) if coalition else coalition
    wanted = {coalition_up} if coalition_up else set()
    coalition_votes = []
    for v in votes:
        coal = v.get("coalition") or classify_bloc(v.get("bloc", ""))
        coal_up = _norm(coal)
        bloc_up = _norm(v.get("bloc", ""))
        if coal_up in wanted or any(w in bloc_up for w in wanted):
            coalition_votes.append(v)
    if not coalition_votes:
        return "N/A"

    counts = defaultdict(int)
    for v in coalition_votes:
        vote = v["vote"].upper()
        if "AFIRMATIV" in vote:
            counts["AFIRMATIVO"] += 1
        elif "NEGATIV" in vote:
            counts["NEGATIVO"] += 1
        elif "ABSTENCI" in vote or "ABSTENCION" in vote:
            counts["ABSTENCION"] += 1
        elif "AUSENT" in vote:
            counts["AUSENTE"] += 1

    active_counts = {k: v for k, v in counts.items() if k != "AUSENTE"}
    if not active_counts:
        return "AUSENTE"

    return max(active_counts, key=active_counts.get)


def compute_combined_majority(
    votes: list[dict], coalitions: list[str]
) -> str:
    """Compute majority vote across multiple coalitions combined.
    Used for 2023+ opposition bloc (LLA + PRO grouped together).
    """
    def _norm(s: str) -> str:
        return unicodedata.normalize('NFKD', (s or '')).encode('ascii', 'ignore').decode('ascii').upper()

    wanted = set(_norm(c) for c in coalitions)
    coalition_votes = []
    for v in votes:
        coal = v.get("coalition") or classify_bloc(v.get("bloc", ""))
        coal_up = _norm(coal)
        bloc_up = _norm(v.get("bloc", ""))
        if coal_up in wanted or any(w in bloc_up for w in wanted):
            coalition_votes.append(v)
    if not coalition_votes:
        return "N/A"

    counts = defaultdict(int)
    for v in coalition_votes:
        vote = v["vote"].upper()
        if "AFIRMATIV" in vote:
            counts["AFIRMATIVO"] += 1
        elif "NEGATIV" in vote:
            counts["NEGATIVO"] += 1
        elif "ABSTENCI" in vote or "ABSTENCION" in vote:
            counts["ABSTENCION"] += 1
        elif "AUSENT" in vote:
            counts["AUSENTE"] += 1

    active_counts = {k: v for k, v in counts.items() if k != "AUSENTE"}
    if not active_counts:
        return "AUSENTE"

    return max(active_counts, key=active_counts.get)


def is_contested(year: int | None, pj_majority: str, opp_majority: str) -> bool:
    """Return True when a votación is contested between PJ and the
    dynamically selected opposition majority for that year.

    Excludes cases where either side has no active majority ('N/A') or
    only 'AUSENTE'.
    """
    if year is None:
        return False
    if pj_majority in ("N/A", "AUSENTE"):
        return False
    if opp_majority in ("N/A", "AUSENTE"):
        return False
    return pj_majority != opp_majority


def normalize_vote(vote_str: str) -> str:
    v = vote_str.upper().strip()
    if "AFIRMATIV" in v:
        return "AFIRMATIVO"
    elif "NEGATIV" in v:
        return "NEGATIVO"
    elif "ABSTENCI" in v:
        return "ABSTENCION"
    elif "AUSENT" in v:
        return "AUSENTE"
    elif "PRESIDEN" in v:
        return "PRESIDENTE"
    return v


def normalize_name(name: str) -> str:
    name = re.sub(r"\s+", " ", name.strip().upper())
    replacements = {
        "Á": "A", "É": "E", "Í": "I", "Ó": "O", "Ú": "U",
        "Ü": "U", "Ñ": "N",
    }
    for old, new in replacements.items():
        name = name.replace(old, new)
    return name


def load_photo_maps() -> dict[str, str]:
    """Load photo name->filename mappings from scraper output.
    Returns a dict: normalized_name -> relative path (fotos/filename).
    """
    photo_map: dict[str, str] = {}

    # Diputados photos
    dip_photos_path = DATA_DIR / "diputados_photos.json"
    if dip_photos_path.exists():
        try:
            with open(dip_photos_path, "r", encoding="utf-8") as f:
                dip_photos = json.load(f)
            for name, filename in dip_photos.items():
                nk = normalize_name(name)
                photo_map[nk] = f"fotos/{filename}"
        except (json.JSONDecodeError, OSError):
            pass

    # Senadores photos
    sen_photos_path = DATA_DIR / "senadores_photos.json"
    if sen_photos_path.exists():
        try:
            with open(sen_photos_path, "r", encoding="utf-8") as f:
                sen_photos = json.load(f)
            for name, filename in sen_photos.items():
                nk = normalize_name(name)
                if nk not in photo_map:
                    photo_map[nk] = f"fotos/{filename}"
        except (json.JSONDecodeError, OSError):
            pass

    # Also scan the consolidated DB for photo_ids
    dip_db_path = DATA_DIR / "diputados.json"
    if dip_db_path.exists():
        db = ConsolidatedDB(dip_db_path)
        db.load()
        for ni_str, photo_id in db.photo_ids.items():
            ni = int(ni_str)
            if ni < len(db.names):
                name = db.names[ni]
                nk = normalize_name(name)
                filename = f"fotos/dip_{photo_id}.jpg"
                if nk not in photo_map:
                    photo_map[nk] = filename


    return photo_map


def attach_photos(legislators: dict, photo_map: dict[str, str]):
    """Attach photo paths to legislator records."""
    matched = 0
    for name_key, leg in legislators.items():
        photo = photo_map.get(name_key, "")
        if photo:
            full_path = DOCS_DIR / photo.replace("/", os.sep)
            if full_path.exists():
                leg["photo"] = photo
                matched += 1
            else:
                leg["photo"] = ""
        else:
            leg["photo"] = ""
    log.info(f"Attached photos to {matched}/{len(legislators)} legislators")


def build_legislator_data(
    all_votaciones: list[dict], law_groups: dict
) -> dict:
    legislators = {}

    votacion_to_group = {}
    for group_key, group_data in law_groups.items():
        for v in group_data["votaciones"]:
            vid = f"{v.get('chamber', '')}_{v.get('id', '')}"
            votacion_to_group[vid] = group_key

    for votacion in all_votaciones:
        year = extract_year(votacion.get("date", ""))
        votacion_id = votacion.get("id", "")
        chamber = votacion.get("chamber", "")
        title = votacion.get("title", "")
        date = clean_date(votacion.get("date", ""))
        vtype = votacion.get("type", "")

        vid_key = f"{chamber}_{votacion_id}"
        group_key = votacion_to_group.get(vid_key, "")
        group_data = law_groups.get(group_key, {})
        law_display_name = (
            group_data.get("common_name")
            or group_data.get("title", title)
        )

        # Coalition definitions (case-insensitive matching)
        PJ_COALITIONS = [
            "PJ", "JUSTICIALISTAS", "FRENTE PARA LA VICTORIA",
            "FRENTE DE TODOS", "UNION POR LA PATRIA", "UXP", "Unidad Ciudadana", "Frente Justicialista"
        ]
        UCR_COALITIONS = [
            "UCR", "UNION CIVICA RADICAL", "UNIÓN CÍVICA RADICAL", "RADICAL",
            "CC", "ACyS", "UDESO", "FPCyS", "Frente Progresista Cívico y Social",
            "Unión para el Desarrollo Social", "Acuerdo Cívico y Social",
            "Concertación para Una Nación Avanzada", "UNA", "ARI",
            "Argentinos por una República de Iguales", "Coalición Cívica",
            "Coalición Cívica ARI"
        ]
        JXC_COALITIONS = ["UCR", "Unión Cívica Radical", "Union Civica Radical","JxC", "Juntos por el Cambio", "CC", "Coalición Cívica" "Cambiemos", "PRO", "Propuesta Republicana", "Frente Pro", "Frente Cambiemos", "Frente Juntos por el Cambio", "ARI", "Coalición Cívica ARI"]
        LLA_PRO_COALITIONS = ["LLA", "PRO", "Juntos por el Cambio", "Alianza La Libertad Avanza"]

        # PJ majority across the PJ coalition variants
        pj_majority = compute_combined_majority(votacion.get("votes", []), PJ_COALITIONS)

        combined_ucr = compute_combined_majority(votacion.get("votes", []), UCR_COALITIONS)
        combined_jxc = compute_combined_majority(votacion.get("votes", []), JXC_COALITIONS)
        combined_lla_pro = compute_combined_majority(votacion.get("votes", []), LLA_PRO_COALITIONS)

        # choose opposition majority based on year
        if year is None:
            opp_majority = "N/A"
        elif year <= 2014:
            opp_majority = combined_ucr
        elif year <= 2023:
            opp_majority = combined_jxc
        else:
            opp_majority = combined_lla_pro

        # Exclude procedural votes
        title_low = title.lower() if title else ""
        if "moción de orden" in title_low or "mocion de orden" in title_low or "pedido de licencia" in title_low:
            contested = False
        else:
            contested = is_contested(year, pj_majority, opp_majority)

        for vote_record in votacion.get("votes", []):
            name = vote_record.get("name", "").strip()
            if not name:
                continue

            name_key = normalize_name(name)

            if name_key not in legislators:
                legislators[name_key] = {
                    "name": name,
                    "name_key": name_key,
                    "chambers": [chamber],
                    "chamber": chamber,
                    "bloc": vote_record.get("bloc", ""),
                    "province": normalize_province(vote_record.get("province", "")),

                    "coalition": vote_record.get("coalition",
                        classify_bloc(vote_record.get("bloc", ""))),
                    "votes": [],
                    "yearly_stats": {},
                    "alignment": {
                        "PJ": {"total": 0, "aligned": 0},
                        "PRO": {"total": 0, "aligned": 0},
                        "LLA": {"total": 0, "aligned": 0},
                        "UCR": {"total": 0, "aligned": 0},
                    },
                    "yearly_alignment": {},
                }

            leg = legislators[name_key]

            if chamber not in leg["chambers"]:
                leg["chambers"].append(chamber)

            leg["bloc"] = vote_record.get("bloc", leg["bloc"])
            leg["province"] = normalize_province(vote_record.get("province", leg["province"]))
            leg["coalition"] = vote_record.get(
                "coalition", leg["coalition"]
            )
            leg["chamber"] = chamber

            norm_vote = normalize_vote(vote_record.get("vote", ""))

            article_label = ""
            title_upper = title.upper()

            titulo_match = re.search(
                r"T[IÍ]TULO\s+([\dIVXLCDM]+)", title_upper
            )
            if titulo_match:
                article_label = f"Título {titulo_match.group(1)}"
            elif "EN GENERAL" in title_upper or \
                 vtype.upper() == "EN GENERAL":
                article_label = "En General"
            elif "EN PARTICULAR" in title_upper or \
                 vtype.upper() == "EN PARTICULAR":
                art_match = re.search(
                    r"ART[IÍ]?CULO?\s*\.?\s*(\d+)", title, re.IGNORECASE
                )
                if art_match:
                    article_label = f"Art. {art_match.group(1)}"
                else:
                    article_label = "En Particular"

            vote_entry = {
                "vid": votacion_id,
                "ch": chamber,
                "t": title[:200],
                "d": date,
                "yr": year,
                "v": norm_vote,
                "pj": pj_majority,
                # store combined coalition majorities for frontend compatibility
                "pro": combined_jxc,
                "lla": combined_lla_pro,
                "ucr": combined_ucr,
                "tp": vtype,
                "gk": group_key,
                "ln": law_display_name[:120] if law_display_name else "",
                "al": article_label,
            }
            # include link to original source when available
            vot_url = votacion.get("url")
            if vot_url:
                vote_entry["url"] = vot_url
            leg["votes"].append(vote_entry)

            if year:
                yr_key = str(year)
                if yr_key not in leg["yearly_stats"]:
                    leg["yearly_stats"][yr_key] = {
                        "AFIRMATIVO": 0, "NEGATIVO": 0,
                        "ABSTENCION": 0, "AUSENTE": 0,
                        "PRESIDENTE": 0, "total": 0,
                    }
                leg["yearly_stats"][yr_key][norm_vote] = \
                    leg["yearly_stats"][yr_key].get(norm_vote, 0) + 1
                leg["yearly_stats"][yr_key]["total"] += 1

                if yr_key not in leg["yearly_alignment"]:
                    leg["yearly_alignment"][yr_key] = {
                        "PJ": {"total": 0, "aligned": 0},
                        # Track multiple opposition groups for reporting
                        "PRO": {"total": 0, "aligned": 0},
                        "LLA": {"total": 0, "aligned": 0},
                        "UCR": {"total": 0, "aligned": 0},
                        "JxC": {"total": 0, "aligned": 0},
                    }

                if contested and norm_vote not in ("AUSENTE", "PRESIDENTE"):
                    # PJ alignment (combined PJ coalition)
                    if pj_majority not in ("N/A", "AUSENTE"):
                        leg["alignment"]["PJ"]["total"] += 1
                        leg["yearly_alignment"][yr_key]["PJ"]["total"] += 1
                        if norm_vote == pj_majority:
                            leg["alignment"]["PJ"]["aligned"] += 1
                            leg["yearly_alignment"][yr_key]["PJ"]["aligned"] += 1

                    # UCR combined majority
                    if combined_ucr not in ("N/A", "AUSENTE"):
                        leg["yearly_alignment"][yr_key]["UCR"]["total"] += 1
                        if norm_vote == combined_ucr:
                            leg["yearly_alignment"][yr_key]["UCR"]["aligned"] += 1
                        # increment global UCR totals only for years where UCR is the
                        # designated opposition (<= 2014)
                        if year is not None and year <= 2014:
                            leg["alignment"]["UCR"]["total"] += 1
                            if norm_vote == combined_ucr:
                                leg["alignment"]["UCR"]["aligned"] += 1

                    # JxC combined majority
                    if combined_jxc not in ("N/A", "AUSENTE"):
                        leg["yearly_alignment"][yr_key]["JxC"]["total"] += 1
                        if norm_vote == combined_jxc:
                            leg["yearly_alignment"][yr_key]["JxC"]["aligned"] += 1

                    # JxC / PRO combined majority (store under PRO for compatibility)
                    # Only count these votes toward overall PRO alignment for years
                    # where JxC/PRO was the designated opposition (2015-2023).
                    if combined_jxc not in ("N/A", "AUSENTE"):
                        leg["yearly_alignment"][yr_key]["PRO"]["total"] += 1
                        if norm_vote == combined_jxc:
                            leg["yearly_alignment"][yr_key]["PRO"]["aligned"] += 1
                        # increment global PRO totals only for 2015-2023
                        if year is not None and 2015 <= year <= 2023:
                            leg["alignment"]["PRO"]["total"] += 1
                            if norm_vote == combined_jxc:
                                leg["alignment"]["PRO"]["aligned"] += 1

                    # LLA + PRO combined majority (tracked under LLA field too)
                    # Only count these votes toward overall LLA alignment for years
                    # where LLA was the designated opposition (2024+).
                    if combined_lla_pro not in ("N/A", "AUSENTE"):
                        leg["yearly_alignment"][yr_key]["LLA"]["total"] += 1
                        if norm_vote == combined_lla_pro:
                            leg["yearly_alignment"][yr_key]["LLA"]["aligned"] += 1
                        if year is not None and year >= 2024:
                            leg["alignment"]["LLA"]["total"] += 1
                            if norm_vote == combined_lla_pro:
                                leg["alignment"]["LLA"]["aligned"] += 1

    return legislators


_PARTY_KEYS = ("pj", "ucr", "pro", "lla", "cc", "oth")


def build_law_detail_data(law_groups: dict) -> tuple[list[dict], dict[int, dict]]:
    """Build per-law, per-party vote breakdown for the law-search feature.

    Returns a tuple of:
      - list of law entries for laws_detail.json (tallies only, no names)
      - votes_by_year: {year: {"n": [name_table], "v": {vi: {pk: [[idx,...],...]}}}}
        One entry per year, each with a local name table for small file size.

    Each law entry contains display name, year, chamber and, for every
    votación in the group, compact vote tallies broken down by the five
    major parties (PJ / UCR / PRO / LLA / CC) plus OTHER.
    """
    laws: list[dict] = []
    # Collect all voter name lists keyed by a running votación index.
    # Using a sequential integer keeps the JSON keys short.
    all_vote_names: dict[int, dict[str, list[list[str]]]] = {}
    name_set: set[str] = set()
    votacion_counter = 0

    for gk, group in law_groups.items():
        votaciones_raw = group.get("votaciones", [])
        if not votaciones_raw:
            continue

        display_name = group.get("common_name") or group.get("title", "")
        if not display_name or len(display_name.strip()) < 3:
            continue

        # Determine year from first votación with a date
        year: int | None = None
        chamber = group.get("chamber", "")
        for v in votaciones_raw:
            y = extract_year(v.get("date", ""))
            if y:
                year = y
                break
            if not chamber:
                chamber = v.get("chamber", "")

        vs_out: list[dict] = []
        for v in votaciones_raw:
            votes_list = v.get("votes", [])
            if not votes_list:
                continue

            # Tally votes per real party (not coalition)
            tallies: dict[str, list[int]] = {
                "pj":  [0, 0, 0, 0],   # [afirm, neg, abst, aus]
                "ucr": [0, 0, 0, 0],
                "pro": [0, 0, 0, 0],
                "lla": [0, 0, 0, 0],
                "cc":  [0, 0, 0, 0],
                "oth": [0, 0, 0, 0],
            }
            total = [0, 0, 0, 0]

            VOTE_IDX = {
                "AFIRMATIVO": 0,
                "NEGATIVO": 1,
                "ABSTENCION": 2,
                "AUSENTE": 3,
            }

            # Per-legislator names for voter drill-down
            names: dict[str, list[list[str]]] = {
                pk: [[], [], [], []] for pk in _PARTY_KEYS
            }

            for vr in votes_list:
                vote_str = normalize_vote(vr.get("vote", ""))
                idx = VOTE_IDX.get(vote_str)
                if idx is None:
                    continue  # skip PRESIDENTE and unknowns

                party = classify_bloc_party(vr.get("bloc", ""))
                party_key = party.lower() if party != "OTROS" else "oth"

                tallies[party_key][idx] += 1
                total[idx] += 1

                leg_name = vr.get("name", "").strip()
                if leg_name:
                    names[party_key][idx].append(leg_name)
                    name_set.add(leg_name)

            # Skip votaciones with zero relevant votes
            if sum(total) == 0:
                continue

            vi = votacion_counter
            votacion_counter += 1
            all_vote_names[vi] = names

            # Determine vote type label
            title_str = v.get("title", "")
            vtype = v.get("type", "")
            tp_label = extract_section_label(title_str, vtype)

            entry: dict = {
                "t": title_str[:200],
                "tp": tp_label,
                "d": clean_date(v.get("date", "")),
                "r": v.get("result", ""),
                "vi": vi,
                "tot": total,
                "pj":  tallies["pj"],
                "ucr": tallies["ucr"],
                "pro": tallies["pro"],
                "lla": tallies["lla"],
                "cc":  tallies["cc"],
                "oth": tallies["oth"],
            }

            vid = v.get("id")
            if vid:
                entry["id"] = vid
            url = v.get("url", "")
            if url:
                entry["url"] = url

            vs_out.append(entry)

        if not vs_out:
            continue

        law_entry: dict = {
            "n": (display_name[:120]).strip(),
            "y": year,
            "ch": chamber,
            "vs": vs_out,
        }
        if group.get("common_name"):
            law_entry["cn"] = group["common_name"]

        laws.append(law_entry)

    # Sort: notable (common_name) first, then by year desc, then by name
    laws.sort(key=lambda x: (
        0 if x.get("cn") else 1,
        -(x.get("y") or 0),
        x.get("n", ""),
    ))

    # Build per-year votes indices.  Each year file gets its own compact
    # name table so files stay small (10-50 KB each instead of one 3.8 MB blob).

    # Map vi -> year so we can bucket votaciones.
    vi_to_year: dict[int, int] = {}
    for law in laws:
        y = law.get("y") or 0
        for v in law.get("vs", []):
            vi = v.get("vi")
            if vi is not None:
                vi_to_year[vi] = y

    # Group vi keys by year
    from collections import defaultdict
    year_vis: dict[int, list[int]] = defaultdict(list)
    for vi, y in vi_to_year.items():
        year_vis[y].append(vi)

    # Build one compact file per year with a local name table
    votes_by_year: dict[int, dict] = {}
    for y, vis in year_vis.items():
        # Collect all names used this year
        year_name_set: set[str] = set()
        for vi in vis:
            pn = all_vote_names.get(vi, {})
            for pk in _PARTY_KEYS:
                for vote_i in range(4):
                    for nm in pn.get(pk, [[], [], [], []])[vote_i]:
                        year_name_set.add(nm)

        year_name_list = sorted(year_name_set)
        local_idx = {n: i for i, n in enumerate(year_name_list)}

        compact_votes: dict[str, dict] = {}
        for vi in vis:
            pn = all_vote_names.get(vi, {})
            compact: dict[str, list[list[int]]] = {}
            for pk in _PARTY_KEYS:
                arrs: list[list[int]] = [[], [], [], []]
                for vote_i in range(4):
                    for nm in pn.get(pk, [[], [], [], []])[vote_i]:
                        arrs[vote_i].append(local_idx[nm])
                if any(arrs[i] for i in range(4)):
                    compact[pk] = arrs
            if compact:
                compact_votes[str(vi)] = compact

        votes_by_year[y] = {"n": year_name_list, "v": compact_votes}

    return laws, votes_by_year


def generate_site_data(legislators: dict, law_groups: dict):
    DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Legislators index
    leg_index = []
    for key, leg in sorted(legislators.items(), key=lambda x: x[0]):
        alignment_pj = (
            round(leg["alignment"]["PJ"]["aligned"]
                  / leg["alignment"]["PJ"]["total"] * 100, 1)
            if leg["alignment"]["PJ"]["total"] > 0 else None
        )
        alignment_pro = (
            round(leg["alignment"]["PRO"]["aligned"]
                  / leg["alignment"]["PRO"]["total"] * 100, 1)
            if leg["alignment"]["PRO"]["total"] > 0 else None
        )
        alignment_lla = (
            round(leg["alignment"]["LLA"]["aligned"]
                  / leg["alignment"]["LLA"]["total"] * 100, 1)
            if leg["alignment"]["LLA"]["total"] > 0 else None
        )

        total_votes = sum(
            s.get("total", 0) for s in leg["yearly_stats"].values()
        )

        chambers = sorted(set(leg["chambers"]))
        chamber_display = (
            "+".join(chambers) if len(chambers) > 1
            else chambers[0] if chambers
            else leg["chamber"]
        )

        leg_index.append({
            "k": key,
            "n": leg["name"],
            "c": chamber_display,
            "b": leg["bloc"],
            "p": leg["province"],
            "co": leg["coalition"],
            "apj": alignment_pj,
            "apro": alignment_pro,
            "alla": alignment_lla,
            "tv": total_votes,
            "ph": leg.get("photo", ""),
        })

    save_json(DOCS_DATA_DIR / "legislators.json", leg_index)
    log.info(f"Generated legislators index with {len(leg_index)} entries")

    # 2. Individual legislator detail files
    leg_details_dir = DOCS_DATA_DIR / "legislators"
    leg_details_dir.mkdir(parents=True, exist_ok=True)

    total_legs = len(legislators)
    count = 0
    start_time = time.time()

    for key, leg in legislators.items():
        yearly_alignment_pct = {}
        for yr, data in sorted(leg["yearly_alignment"].items()):
            yearly_alignment_pct[yr] = {}
            try:
                yint = int(yr)
            except Exception:
                yint = None
            for coalition in ["PJ", "UCR", "JxC", "PRO", "LLA"]:
                total = data.get(coalition, {}).get("total", 0)
                aligned = data.get(coalition, {}).get("aligned", 0)
                pct = None
                # Apply year masks for opposition coalitions
                if coalition == "UCR":
                    if yint is not None and yint <= 2014:
                        pct = round(aligned / total * 100, 1) if total > 5 else None
                    else:
                        pct = None
                elif coalition in ("JxC", "PRO"):
                    if yint is not None and 2015 <= yint <= 2023:
                        pct = round(aligned / total * 100, 1) if total > 5 else None
                    else:
                        pct = None
                elif coalition == "LLA":
                    if yint is not None and yint >= 2024:
                        pct = round(aligned / total * 100, 1) if total > 5 else None
                    else:
                        pct = None
                else:
                    # PJ: always compute when enough votes
                    pct = round(aligned / total * 100, 1) if total > 5 else None

                yearly_alignment_pct[yr][coalition] = pct

        # Build waffle groups — merge by common_name so that e.g. all
        # "Ley Bases" O.D. numbers/dates become a single law entry
        waffle_groups = defaultdict(
            lambda: {"name": "", "votes": [], "year": None,
                     "common_name": None}
        )
        for vote in leg["votes"]:
            ln = vote.get("ln", "")
            # Determine merge key: use common_name if available, else gk
            common_name = get_common_name(ln) if ln else None
            if common_name:
                merge_key = f"COMMON|{common_name}"
            else:
                gk = vote.get("gk", "")
                merge_key = gk if gk else f"SINGLE_{vote['vid']}"

            wg = waffle_groups[merge_key]
            if not wg["name"]:
                wg["name"] = ln or vote.get("t", "")
            if common_name:
                wg["common_name"] = common_name
                wg["name"] = common_name

            # Mark whether this is an "En General" vote
            al = vote.get("al", "")
            is_general = (al == "En General"
                          or "EN GENERAL" in vote.get("t", "").upper()
                          or "VOT. EN GRAL" in vote.get("t", "").upper())

            entry = {
                "v": vote["v"],
                "al": al,
                "t": vote.get("t", ""),
                "vid": vote.get("vid"),
                "ch": vote.get("ch"),
                "g": is_general,
            }
            if vote.get("url"):
                entry["url"] = vote["url"]
            wg["votes"].append(entry)
            if vote.get("yr") and not wg["year"]:
                wg["year"] = vote["yr"]

        waffle_list = []
        for gk, wg in waffle_groups.items():
            # pick first available vote URL to act as law link
            law_url = ""
            for vote in wg["votes"]:
                if vote.get("url"):
                    law_url = vote["url"]
                    break

            waffle_list.append({
                "gk": gk,
                "name": wg["name"][:120],
                "year": wg["year"],
                "url": law_url,
                "votes": wg["votes"],
                "notable": wg.get("common_name") is not None,
            })
        waffle_list.sort(key=lambda x: (-(x["year"] or 0), x["name"]))

        detail = {
            "name": leg["name"],
            "name_key": key,
            "photo": leg.get("photo", ""),
            "chambers": sorted(set(leg["chambers"])),
            "chamber": leg["chamber"],
            "bloc": leg["bloc"],
            "province": leg["province"],
            "coalition": leg["coalition"],
            "yearly_stats": leg["yearly_stats"],
            "yearly_alignment": yearly_alignment_pct,
            "alignment": {
                c: round(d["aligned"] / d["total"] * 100, 1)
                   if d["total"] > 0 else None
                for c, d in leg["alignment"].items()
            },
            "votes": leg["votes"],
            "laws": waffle_list,
        }

        safe_name = re.sub(r"[^A-Z0-9_]", "_", key)[:80]
        # time the JSON write to detect slow IO
        write_start = time.time()
        save_json(leg_details_dir / f"{safe_name}.json", detail)
        write_elapsed = time.time() - write_start
        count += 1
        # log progress periodically to help diagnose stalls
        if count % 100 == 0 or count == total_legs:
            total_elapsed = time.time() - start_time
            log.info(
                f"Wrote {count}/{total_legs} legislator files; last_write={write_elapsed:.2f}s; total_elapsed={total_elapsed:.2f}s"
            )

    log.info(f"Generated {len(legislators)} legislator detail files")

    # 3. Votaciones summary
    votaciones_summary = {"diputados": [], "senadores": []}

    for chamber in ["diputados", "senadores"]:
        votaciones = load_all_votaciones_from_db(chamber)
        for v in votaciones:
            votaciones_summary[chamber].append({
                "id": v.get("id"),
                "title": v.get("title", "")[:200],
                "date": clean_date(v.get("date", "")),
                "result": v.get("result", ""),
                "type": v.get("type", ""),
                "afirmativo": v.get("afirmativo", 0),
                "negativo": v.get("negativo", 0),
                "abstencion": v.get("abstencion", 0),
                "ausente": v.get("ausente", 0),
            })

    save_json(DOCS_DATA_DIR / "votaciones.json", votaciones_summary)
    log.info("Generated votaciones summary")

    # 3b. Law detail data (per-coalition vote breakdowns for law search)
    laws_detail, votes_by_year = build_law_detail_data(law_groups)
    save_json(DOCS_DATA_DIR / "laws_detail.json", laws_detail)
    log.info(f"Generated laws_detail.json with {len(laws_detail)} law entries")

    # 3c. Per-year voter-names indices (loaded on-demand on first bar-click)
    votes_dir = DOCS_DATA_DIR / "votes"
    votes_dir.mkdir(parents=True, exist_ok=True)
    total_votaciones = 0
    for year, ydata in sorted(votes_by_year.items()):
        save_json(votes_dir / f"votes_{year}.json", ydata)
        total_votaciones += len(ydata["v"])
    log.info(f"Generated {len(votes_by_year)} per-year vote files "
             f"({total_votaciones} votaciones total)")

    # 4. Law names list
    law_names_set = set()
    for g in law_groups.values():
        cn = g.get("common_name")
        if cn:
            law_names_set.add(cn)
        else:
            t = g.get("title", "").strip()
            if t and len(t) > 5:
                law_names_set.add(t[:120])
    save_json(DOCS_DATA_DIR / "law_names.json", sorted(law_names_set))
    log.info(f"Generated {len(law_names_set)} unique law names")

    # 5. Global stats
    stats = {
        "last_updated": datetime.now().isoformat(),
        "total_legislators": len(legislators),
        "total_diputados": sum(
            1 for l in legislators.values()
            if "diputados" in l.get("chambers", [l["chamber"]])
        ),
        "total_senadores": sum(
            1 for l in legislators.values()
            if "senadores" in l.get("chambers", [l["chamber"]])
        ),
        "total_votaciones_diputados": len(votaciones_summary["diputados"]),
        "total_votaciones_senadores": len(votaciones_summary["senadores"]),
        "years_covered": sorted(set(
            yr for leg in legislators.values()
            for yr in leg["yearly_stats"].keys()
        )),
        "total_laws": len(law_groups),
    }
    save_json(DOCS_DATA_DIR / "stats.json", stats)
    log.info("Generated global stats")


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix('.tmp')
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=None,
                      separators=(",", ":"))
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                # fsync may not be available on all platforms; ignore if it fails
                pass
        # Atomic replace to avoid truncated files if process is interrupted
        os.replace(tmp_path, path)
    except Exception as e:
        log.exception(f"Failed to write JSON to {path}: {e}")
        # Attempt best-effort write directly
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=None,
                          separators=(",", ":"))
        except Exception:
            log.exception(f"Fallback write also failed for {path}")


def main():
    log.info("Como Voto - Data Processor")
    log.info(f"Loading votaciones from {DATA_DIR}")

    # Load from consolidated DBs
    all_votaciones = []
    all_votaciones.extend(load_all_votaciones_from_db("diputados"))
    all_votaciones.extend(load_all_votaciones_from_db("senadores"))

    if not all_votaciones:
        log.warning("No votaciones found. Run scraper.py first.")
        generate_site_data({}, {})
        return

    log.info(f"Loaded {len(all_votaciones)} votaciones total")

    law_groups = build_law_groups(all_votaciones)
    log.info(f"Identified {len(law_groups)} law groups")

    photo_map = load_photo_maps()
    log.info(f"Loaded photo map: {len(photo_map)} entries")

    legislators = build_legislator_data(all_votaciones, law_groups)
    log.info(f"Found {len(legislators)} unique legislators")

    attach_photos(legislators, photo_map)

    generate_site_data(legislators, law_groups)

    log.info("Processing complete!")


if __name__ == "__main__":
    main()
