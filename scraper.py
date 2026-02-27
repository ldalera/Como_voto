#!/usr/bin/env python3
"""
Como Voto - Data Scraper
========================
Scrapes voting data from:
  - Diputados: https://votaciones.hcdn.gob.ar/
  - Senadores: https://www.senado.gob.ar/votaciones/actas

Also scrapes legislator photos:
  - Diputados photos from voting page tables
  - Senadores photos from the Senado open data JSON API

Stores results in data/ directory as JSON files.
Skips votaciones already present in the local database.
"""

import json
import os
import re
import sys
import time
import logging
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DIPUTADOS_DIR = DATA_DIR / "diputados"
SENADORES_DIR = DATA_DIR / "senadores"
FOTOS_DIR = BASE_DIR / "docs" / "fotos"

HCDN_BASE = "https://votaciones.hcdn.gob.ar"
SENADO_BASE = "https://www.senado.gob.ar"

# Rate-limiting: seconds between requests
REQUEST_DELAY = 1.0

# HCDN votacion IDs range from 1 to ~6000 (non-contiguous, gaps up to ~1500).
# We simply iterate through all IDs and check each page.
HCDN_MAX_ID = 6500          # upper bound to scan (with margin)
HCDN_STOP_AFTER_MISS = 500  # stop after this many consecutive misses past the last hit

# Senado periods to scrape
SENADO_YEARS = list(range(2015, datetime.now().year + 1))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("scraper")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "ComoVoto-Scraper/1.0 (https://github.com/como-voto; educational project)",
    "Accept-Language": "es-AR,es;q=0.9",
})

# ---------------------------------------------------------------------------
# Party classification helpers
# ---------------------------------------------------------------------------
PJ_KEYWORDS = [
    "justicialista", "frente de todos", "frente para la victoria",
    "unión por la patria", "union por la patria",
    "frente renovador", "peronismo", "peronista",
    "frente cívico por santiago", "frente civico por santiago",
    "movimiento popular neuquino",
    "bloque justicialista", "pj ",
]

PRO_KEYWORDS = [
    "pro ", "propuesta republicana",
    "cambiemos", "juntos por el cambio", "juntos por el cambio federal",
    "ucr", "unión cívica radical", "union civica radical",
    "coalición cívica", "coalicion civica",
    "evolución radical", "evolucion radical",
]

LLA_KEYWORDS = [
    "la libertad avanza",
]


def classify_bloc(bloc_name: str) -> str:
    """Classify a bloc name into PJ, PRO, LLA or OTHER."""
    name = bloc_name.lower().strip()
    for kw in PJ_KEYWORDS:
        if kw in name:
            return "PJ"
    for kw in PRO_KEYWORDS:
        if kw in name:
            return "PRO"
    for kw in LLA_KEYWORDS:
        if kw in name:
            return "LLA"
    return "OTHER"


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------

def ensure_dirs():
    """Create data directories if they don't exist."""
    for d in [DATA_DIR, DIPUTADOS_DIR, SENADORES_DIR, FOTOS_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def load_json(path: Path) -> dict | list:
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_json(path: Path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_index(chamber: str) -> dict:
    """Load the votaciones index for a chamber. Returns {id: metadata}."""
    path = DATA_DIR / f"{chamber}_index.json"
    return load_json(path) if path.exists() else {}


def save_index(chamber: str, index: dict):
    save_json(DATA_DIR / f"{chamber}_index.json", index)


def votacion_exists(chamber: str, votacion_id: str) -> bool:
    """Check if a votacion detail file already exists."""
    chamber_dir = DIPUTADOS_DIR if chamber == "diputados" else SENADORES_DIR
    return (chamber_dir / f"{votacion_id}.json").exists()


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def fetch(url: str, delay: float = REQUEST_DELAY, raise_for_status: bool = True) -> requests.Response | None:
    """Fetch a URL with rate limiting and error handling."""
    time.sleep(delay)
    try:
        resp = SESSION.get(url, timeout=30)
        if raise_for_status:
            resp.raise_for_status()
        return resp
    except requests.RequestException as e:
        if not isinstance(e, requests.HTTPError):
            log.warning(f"Failed to fetch {url}: {e}")
        return None


def fetch_soup(url: str, delay: float = REQUEST_DELAY) -> BeautifulSoup | None:
    resp = fetch(url, delay)
    if resp is None:
        return None
    return BeautifulSoup(resp.text, "lxml")


# ---------------------------------------------------------------------------
# Photo helpers
# ---------------------------------------------------------------------------

def download_photo(url: str, filename: str) -> bool:
    """Download a photo to docs/fotos/. Returns True on success."""
    dest = FOTOS_DIR / filename
    if dest.exists() and dest.stat().st_size > 500:
        return True  # already downloaded
    try:
        time.sleep(0.15)
        resp = SESSION.get(url, timeout=15)
        if resp.status_code == 200 and len(resp.content) > 500:
            with open(dest, "wb") as f:
                f.write(resp.content)
            return True
    except requests.RequestException:
        pass
    return False


# ===========================================================================
#  DIPUTADOS SCRAPER
# ===========================================================================

def scrape_hcdn_votacion(votacion_id: str) -> dict | None:
    """Scrape a single HCDN votacion detail page.

    Simply checks if the page at votaciones.hcdn.gob.ar/votacion/{id} exists
    and has voting data. Returns the data dict or None.
    """
    url = f"{HCDN_BASE}/votacion/{votacion_id}"
    resp = fetch(url, delay=0.3, raise_for_status=False)
    if resp is None or resp.status_code != 200:
        return None
    soup = BeautifulSoup(resp.text, "lxml")

    # Check if page has actual voting content
    if not soup.find(string=re.compile("¿CÓMO VOTÓ?")):
        return None

    result = {
        "id": votacion_id,
        "chamber": "diputados",
        "url": url,
        "title": "",
        "date": "",
        "result": "",
        "period": "",
        "type": "",
        "afirmativo": 0,
        "negativo": 0,
        "abstencion": 0,
        "ausente": 0,
        "votes": [],
    }

    # Title - in h4 element, may include date appended
    title_el = soup.find("h4")
    if title_el:
        raw_title = title_el.get_text(strip=True)
        date_match = re.search(r"(\d{2}/\d{2}/\d{4}\s*-?\s*\d{2}:\d{2})", raw_title)
        if date_match:
            result["date"] = date_match.group(1).strip()
            result["title"] = raw_title[:date_match.start()].strip()
        else:
            result["title"] = raw_title

    # Period info
    period_el = soup.find("h5", string=re.compile(r"Período"))
    if period_el:
        result["period"] = period_el.get_text(strip=True)

    # Date fallback
    if not result["date"]:
        for h5 in soup.find_all("h5"):
            text = h5.get_text(strip=True)
            dm = re.search(r"\d{2}/\d{2}/\d{4}", text)
            if dm:
                result["date"] = text
                break

    # Result (AFIRMATIVO/NEGATIVO)
    result_h3 = soup.find("h3")
    if result_h3:
        result["result"] = result_h3.get_text(strip=True)

    # Vote counts
    count_sections = soup.find_all("h3")
    labels = soup.find_all("h4")
    for h3, h4 in zip(count_sections, labels):
        try:
            count = int(h3.get_text(strip=True))
            label = h4.get_text(strip=True).upper()
            if "AFIRMATIVO" in label:
                result["afirmativo"] = count
            elif "NEGATIVO" in label:
                result["negativo"] = count
            elif "ABSTENCI" in label:
                result["abstencion"] = count
            elif "AUSENTE" in label:
                result["ausente"] = count
        except (ValueError, AttributeError):
            continue

    # Individual votes from the table
    # Columns: [photo, NAME, BLOQUE, PROVINCIA, VOTE, optional]
    table = soup.find("table")
    if table:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 5:
                # Cell 0 = photo — extract photo ID for later download
                photo_id = ""
                photo_link = cells[0].find("a", href=True)
                if photo_link:
                    photo_id = photo_link["href"].rstrip("/").split("/")[-1]

                name = cells[1].get_text(strip=True)
                bloc = cells[2].get_text(strip=True)
                province = cells[3].get_text(strip=True)
                vote = cells[4].get_text(strip=True)

                if name and vote:
                    vote_entry = {
                        "name": name,
                        "bloc": bloc,
                        "province": province,
                        "vote": vote.upper(),
                        "coalition": classify_bloc(bloc),
                    }
                    if photo_id:
                        vote_entry["photo_id"] = photo_id
                    result["votes"].append(vote_entry)

    return result


def scrape_diputados():
    """Scrape all new Diputados votaciones.

    Simple sequential approach: iterate IDs from 1 to HCDN_MAX_ID.
    For each ID:
      - skip if the file already exists on disk
      - check if the page https://votaciones.hcdn.gob.ar/votacion/{id} exists
      - if yes, scrape and save; if no, move to the next ID
    Stop after HCDN_STOP_AFTER_MISS consecutive misses past the highest hit.
    """
    log.info("=" * 60)
    log.info("SCRAPING DIPUTADOS")
    log.info("=" * 60)

    existing_index = load_index("diputados")

    # IDs already on disk
    existing_ids: set[int] = set()
    for f in DIPUTADOS_DIR.glob("*.json"):
        try:
            existing_ids.add(int(f.stem))
        except ValueError:
            pass

    log.info(f"Found {len(existing_ids)} existing diputados votaciones on disk")

    new_count = 0
    consecutive_misses = 0
    highest_hit = max(existing_ids) if existing_ids else 0
    checked = 0

    for vid_int in range(1, HCDN_MAX_ID + 1):
        # Skip if already on disk
        if vid_int in existing_ids:
            consecutive_misses = 0  # reset — we know there's data here
            continue

        checked += 1
        vid = str(vid_int)

        data = scrape_hcdn_votacion(vid)
        if data and data.get("votes"):
            save_json(DIPUTADOS_DIR / f"{vid}.json", data)
            existing_index[vid] = {
                "title": data.get("title", ""),
                "date": data.get("date", ""),
                "result": data.get("result", ""),
            }
            new_count += 1
            consecutive_misses = 0
            highest_hit = vid_int

            if new_count % 25 == 0:
                save_index("diputados", existing_index)

            log.info(f"  [{vid}] Saved ({new_count} new, {checked} checked): "
                     f"{data.get('title', 'Unknown')[:80]}")
        else:
            consecutive_misses += 1

        # Log progress periodically
        if checked % 200 == 0:
            log.info(f"  Progress: checked {checked}, saved {new_count} new, "
                     f"at ID {vid_int}, consecutive misses: {consecutive_misses}")

        # Stop condition: many consecutive misses past the highest known hit
        if vid_int > highest_hit and consecutive_misses >= HCDN_STOP_AFTER_MISS:
            log.info(f"  Stopping: {consecutive_misses} consecutive misses "
                     f"past highest hit ({highest_hit})")
            break

    save_index("diputados", existing_index)
    log.info(f"Diputados: scraped {new_count} new votaciones "
             f"(checked {checked} IDs, highest hit: {highest_hit})")


# ===========================================================================
#  SENADO SCRAPER
# ===========================================================================

def scrape_senado_actas_list(year: int) -> list[dict]:
    """Scrape the list of actas from the Senado for a given year."""
    log.info(f"=== Scraping Senado actas list for {year} ===")
    existing_index = load_index("senadores")

    url = f"{SENADO_BASE}/votaciones/actas"
    actas = []

    page = 1
    while True:
        resp = fetch(f"{url}?periodo={year}&page={page}")
        if resp is None:
            break

        soup = BeautifulSoup(resp.text, "lxml")
        detail_links = soup.find_all("a", href=re.compile(r"/votaciones/detalleActa/(\d+)"))

        if not detail_links:
            break

        for link in detail_links:
            match = re.search(r"/votaciones/detalleActa/(\d+)", link["href"])
            if match:
                acta_id = match.group(1)
                if acta_id not in existing_index:
                    actas.append({
                        "id": acta_id,
                        "text": link.get_text(strip=True),
                    })

        next_link = soup.find("a", string=re.compile("Siguiente"))
        if next_link and "href" in next_link.attrs:
            page += 1
        else:
            break

    log.info(f"Found {len(actas)} new Senado actas for {year}")
    return actas


def scrape_senado_votacion(acta_id: str) -> dict | None:
    """Scrape a single Senado votacion detail page."""
    url = f"{SENADO_BASE}/votaciones/detalleActa/{acta_id}"
    soup = fetch_soup(url, delay=0.5)
    if soup is None:
        return None

    result = {
        "id": acta_id,
        "chamber": "senadores",
        "url": url,
        "title": "",
        "date": "",
        "result": "",
        "type": "",
        "afirmativo": 0,
        "negativo": 0,
        "abstencion": 0,
        "ausente": 0,
        "votes": [],
    }

    content = soup.find("div", class_=re.compile("content|main|votacion", re.I))
    if not content:
        content = soup

    # Title: find <p> immediately after "Acta Nro:" <p>
    acta_nro_p = content.find("p", string=re.compile(r"Acta Nro", re.I))
    if acta_nro_p:
        for sib in acta_nro_p.find_next_siblings():
            if sib.name == "p":
                t = sib.get_text(strip=True)
                if t and "Secretaría" not in t and "Honorable" not in t:
                    result["title"] = t[:300]
                    break

    # Fallback: keyword search
    if not result["title"]:
        for text_node in content.find_all(string=True):
            text = text_node.strip()
            if len(text) > 20 and any(kw in text.lower() for kw in
                    ["ley", "proyecto", "pliego", "acuerdo", "modificación",
                     "régimen", "designación", "modernización"]):
                result["title"] = text[:300]
                break

    if not result["title"]:
        for tag in ["h2", "h3", "h1"]:
            el = content.find(tag)
            if el and len(el.get_text(strip=True)) > 10:
                result["title"] = el.get_text(strip=True)
                break

    # Date
    date_match = re.search(
        r"(\d{2}/\d{2}/\d{4})\s*-?\s*(\d{2}:\d{2})?",
        content.get_text()
    )
    if date_match:
        result["date"] = date_match.group(0).strip()

    # Result
    for text in content.find_all(string=re.compile(r"AFIRMATIVO|NEGATIVO", re.I)):
        result["result"] = text.strip()
        break

    # Type
    for text in content.find_all(string=re.compile(r"EN GENERAL|EN PARTICULAR", re.I)):
        result["type"] = text.strip()
        break

    # Vote counts
    count_headers = content.find_all("h3")
    count_labels = content.find_all("h4")
    for h3, h4 in zip(count_headers, count_labels):
        try:
            count = int(h3.get_text(strip=True))
            label = h4.get_text(strip=True).upper()
            if "AFIRMATIVO" in label:
                result["afirmativo"] = count
            elif "NEGATIVO" in label:
                result["negativo"] = count
            elif "ABSTENCI" in label:
                result["abstencion"] = count
            elif "AUSENTE" in label:
                result["ausente"] = count
        except (ValueError, AttributeError):
            continue

    # Individual votes from the table
    table = content.find("table")
    if table:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 5:
                name = cells[1].get_text(strip=True)
                bloc = cells[2].get_text(strip=True)
                province = cells[3].get_text(strip=True)
                vote = cells[4].get_text(strip=True)

                name = re.sub(r"^Foto de.*?Nacional\s*", "", name).strip()

                if name and vote:
                    result["votes"].append({
                        "name": name,
                        "bloc": bloc,
                        "province": province,
                        "vote": vote.upper(),
                        "coalition": classify_bloc(bloc),
                    })

    return result


# ===========================================================================
#  PHOTO SCRAPERS
# ===========================================================================

def _patch_photo_ids(fpath: Path) -> bool:
    """Re-fetch an HCDN votacion page to extract photo_ids into an existing file.

    Returns True if the file was updated.
    """
    try:
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return False

    # Check if any vote already has a photo_id
    if any(v.get("photo_id") for v in data.get("votes", [])):
        return False  # already patched

    vid = data.get("id", fpath.stem)
    url = f"{HCDN_BASE}/votacion/{vid}"
    resp = fetch(url, delay=0.3, raise_for_status=False)
    if resp is None or resp.status_code != 200:
        return False

    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.find("table")
    if not table:
        return False

    # Build a name -> photo_id map from the page
    page_photos: dict[str, str] = {}
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) >= 5:
            photo_link = cells[0].find("a", href=True)
            if photo_link:
                pid = photo_link["href"].rstrip("/").split("/")[-1]
                name = cells[1].get_text(strip=True)
                if pid and name:
                    page_photos[name] = pid

    if not page_photos:
        return False

    # Patch the votes
    changed = False
    for v in data.get("votes", []):
        name = v.get("name", "").strip()
        if name in page_photos and "photo_id" not in v:
            v["photo_id"] = page_photos[name]
            changed = True

    if changed:
        save_json(fpath, data)

    return changed


def scrape_diputados_photos():
    """Download diputado photos from HCDN voting pages.

    Reads the already-scraped votacion JSON files to collect photo_ids,
    then downloads each unique photo. If existing files lack photo_ids,
    re-fetches the page to extract them first.
    """
    log.info("=" * 60)
    log.info("DOWNLOADING DIPUTADOS PHOTOS")
    log.info("=" * 60)

    # First pass: patch files missing photo_ids
    files_to_patch = []
    for fpath in sorted(DIPUTADOS_DIR.glob("*.json")):
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
            has_photos = any(v.get("photo_id") for v in data.get("votes", []))
            if not has_photos and data.get("votes"):
                files_to_patch.append(fpath)
        except (json.JSONDecodeError, OSError):
            continue

    if files_to_patch:
        log.info(f"Patching {len(files_to_patch)} files missing photo_ids...")
        patched = 0
        for fpath in files_to_patch:
            if _patch_photo_ids(fpath):
                patched += 1
                if patched % 25 == 0:
                    log.info(f"  Patched {patched}/{len(files_to_patch)}")
        log.info(f"  Patched {patched} files with photo_ids")

    # Collect unique (name -> photo_id) from scraped data
    photo_map: dict[str, str] = {}  # name -> photo_id
    for fpath in sorted(DIPUTADOS_DIR.glob("*.json")):
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
            for v in data.get("votes", []):
                pid = v.get("photo_id", "")
                name = v.get("name", "").strip()
                if pid and name:
                    photo_map[name] = pid
        except (json.JSONDecodeError, OSError):
            continue

    log.info(f"Found {len(photo_map)} unique diputados with photo IDs")

    downloaded = 0
    for name, photo_id in photo_map.items():
        url = f"{HCDN_BASE}/assets/diputados/{photo_id}"
        filename = f"dip_{photo_id}.jpg"
        if download_photo(url, filename):
            downloaded += 1

    log.info(f"Downloaded {downloaded} diputado photos")

    # Save the name->filename mapping
    name_to_file = {}
    for name, photo_id in photo_map.items():
        filename = f"dip_{photo_id}.jpg"
        if (FOTOS_DIR / filename).exists():
            name_to_file[name] = filename
    save_json(DATA_DIR / "diputados_photos.json", name_to_file)
    log.info(f"Saved diputados photo mapping ({len(name_to_file)} entries)")


def scrape_senadores_photos():
    """Download senator photos from the Senado open data JSON API."""
    log.info("=" * 60)
    log.info("DOWNLOADING SENADORES PHOTOS")
    log.info("=" * 60)

    url = f"{SENADO_BASE}/micrositios/DatosAbiertos/ExportarListadoSenadores/json"
    resp = fetch(url, delay=0.5)
    if resp is None:
        log.warning("Could not fetch Senado open data JSON")
        return

    try:
        data = resp.json()
    except (json.JSONDecodeError, ValueError):
        log.warning("Invalid JSON from Senado open data")
        return

    rows = data.get("table", {}).get("rows", [])
    log.info(f"Found {len(rows)} senators in open data")

    name_to_file: dict[str, str] = {}
    downloaded = 0

    for row in rows:
        sen_id = row.get("ID", "")
        apellido = row.get("APELLIDO", "").strip()
        nombre = row.get("NOMBRE", "").strip()
        foto_url = row.get("FOTO", "")

        if not sen_id or not foto_url:
            continue

        # Build name in format "APELLIDO, Nombre" to match voting data
        full_name = f"{apellido}, {nombre}".strip()

        filename = f"sen_{sen_id}.gif"
        if download_photo(foto_url, filename):
            downloaded += 1
            name_to_file[full_name] = filename

    log.info(f"Downloaded {downloaded} senator photos")
    save_json(DATA_DIR / "senadores_photos.json", name_to_file)
    log.info(f"Saved senadores photo mapping ({len(name_to_file)} entries)")


# ===========================================================================
#  SENADO ORCHESTRATION
# ===========================================================================

def scrape_senadores():
    """Scrape all new Senado votaciones."""
    log.info("=" * 60)
    log.info("SCRAPING SENADORES")
    log.info("=" * 60)

    existing_index = load_index("senadores")

    new_count = 0
    for year in SENADO_YEARS:
        actas = scrape_senado_actas_list(year)

        for acta in actas:
            aid = acta["id"]
            if votacion_exists("senadores", aid):
                log.info(f"  Skipping senado votacion {aid} (already exists)")
                continue

            log.info(f"  Scraping senado votacion {aid}...")
            data = scrape_senado_votacion(aid)
            if data and data.get("votes"):
                save_json(SENADORES_DIR / f"{aid}.json", data)
                existing_index[aid] = {
                    "title": data.get("title", ""),
                    "date": data.get("date", ""),
                    "result": data.get("result", ""),
                }
                new_count += 1
                log.info(f"    Saved: {data.get('title', 'Unknown')[:80]}")
            else:
                log.warning(f"    No vote data found for senado votacion {aid}")

    save_index("senadores", existing_index)
    log.info(f"Senadores: scraped {new_count} new votaciones")


# ===========================================================================
#  MAIN
# ===========================================================================

def main():
    ensure_dirs()

    log.info("Como Voto - Data Scraper")
    log.info(f"Data directory: {DATA_DIR}")

    # Parse CLI args
    # Usage: python scraper.py [diputados] [senadores] [fotos]
    # Default: all three
    args = [a.lower() for a in sys.argv[1:]] if len(sys.argv) > 1 else ["diputados", "senadores", "fotos"]

    if "diputados" in args:
        scrape_diputados()

    if "senadores" in args:
        scrape_senadores()

    if "fotos" in args:
        scrape_diputados_photos()
        scrape_senadores_photos()

    log.info("Scraping complete!")


if __name__ == "__main__":
    main()
