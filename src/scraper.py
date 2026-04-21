#!/usr/bin/env python3
"""
Harris County (Houston, TX) — Motivated Seller Lead Scraper v4
==============================================================
Public sources (no login required):
  1. Harris County District Clerk — civil foreclosure / lien filings
  2. Harris County Tax Office     — delinquent property tax accounts
  3. Harris County Appraisal (HCAD) — property owner enrichment

Distress scoring (max 100 pts):
  Tax delinquency    +30
  Foreclosure filing +30
  Code violation     +25
  Probate filing     +20
  Multiple liens     +15
  Divorce/bankruptcy +10
"""

import json, csv, re, sys, argparse, logging
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent
DATA = ROOT / "data"
DASH = ROOT / "dashboard"
DATA.mkdir(exist_ok=True)
DASH.mkdir(exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
})

def get(url, **kw):
    try:
        r = SESSION.get(url, timeout=30, **kw)
        r.raise_for_status()
        log.info("GET %s → %d (%d bytes)", url, r.status_code, len(r.content))
        return r
    except Exception as e:
        log.warning("GET %s → %s", url, e)
        return None

def post(url, data, **kw):
    try:
        r = SESSION.post(url, data=data, timeout=30, **kw)
        r.raise_for_status()
        log.info("POST %s → %d (%d bytes)", url, r.status_code, len(r.content))
        return r
    except Exception as e:
        log.warning("POST %s → %s", url, e)
        return None

def clean(s):
    return " ".join(str(s).split()) if s else ""

# ── 1. Harris County District Clerk — Foreclosure filings ─────────────────

CLERK_BASE = "https://www.hcdistrictclerk.com"
CLERK_SEARCH = "https://www.hcdistrictclerk.com/edocs/public/CaseListFiling.aspx"

def scrape_clerk_foreclosures(days_back=7):
    """Scrape recent foreclosure / lien filings from Harris County District Clerk."""
    log.info("=== Harris County District Clerk ===")

    # First load the search page to get ASP.NET tokens
    r = get(CLERK_SEARCH)
    if not r:
        log.warning("District Clerk: could not load search page")
        return _try_clerk_alternate(days_back)

    soup = BeautifulSoup(r.text, "lxml")
    log.info("Clerk page title: %s", soup.title.string if soup.title else "n/a")
    log.debug("Clerk HTML[:1500]: %s", r.text[:1500])

    # Grab ASP.NET hidden fields
    vs  = soup.find("input", {"id": "__VIEWSTATE"})
    ev  = soup.find("input", {"id": "__EVENTVALIDATION"})
    vsg = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})

    date_from = (datetime.now() - timedelta(days=days_back)).strftime("%m/%d/%Y")
    date_to   = datetime.now().strftime("%m/%d/%Y")

    payload = {
        "__VIEWSTATE":            vs["value"]  if vs  else "",
        "__EVENTVALIDATION":      ev["value"]  if ev  else "",
        "__VIEWSTATEGENERATOR":   vsg["value"] if vsg else "",
        "ctl00$ctl00$cphMain$cphMain$txtFilingDateFrom": date_from,
        "ctl00$ctl00$cphMain$cphMain$txtFilingDateTo":   date_to,
        "ctl00$ctl00$cphMain$cphMain$ddCaseType":        "T",   # Tax / Foreclosure
        "ctl00$ctl00$cphMain$cphMain$btnSearch":         "Search",
    }

    r2 = post(CLERK_SEARCH, payload)
    if not r2:
        return _try_clerk_alternate(days_back)

    return _parse_clerk_results(r2.text, "Foreclosure")

def _try_clerk_alternate(days_back):
    """Fallback: try the newer Harris County portal."""
    log.info("Trying alternate clerk URL...")
    url = "https://www.hcdistrictclerk.com/edocs/public/search.aspx"
    r = get(url)
    if r:
        return _parse_clerk_results(r.text, "Clerk Filing")
    return []

def _parse_clerk_results(html, doc_type):
    soup = BeautifulSoup(html, "lxml")
    records = []
    tables = soup.find_all("table")
    log.info("Clerk results: %d table(s) found", len(tables))

    for table in tables:
        rows = table.find_all("tr")
        headers = [clean(th.get_text()) for th in rows[0].find_all(["th","td"])] if rows else []
        log.debug("Table headers: %s", headers)

        for row in rows[1:]:
            cells = [clean(td.get_text()) for td in row.find_all("td")]
            if len(cells) < 2:
                continue
            rec = {
                "source":       "Harris Co. District Clerk",
                "doc_type":     doc_type,
                "case_number":  cells[0] if cells else "",
                "owner":        cells[1] if len(cells) > 1 else "",
                "address":      cells[2] if len(cells) > 2 else "",
                "filing_date":  cells[3] if len(cells) > 3 else "",
                "score":        0,
                "signals":      ["foreclosure"],
            }
            if rec["case_number"] or rec["owner"]:
                records.append(rec)

    log.info("District Clerk → %d record(s)", len(records))
    return records

# ── 2. Harris County Tax Office — Delinquent accounts ─────────────────────

TAX_URL = "https://www.hctax.net/Property/PropertyTax"

def scrape_tax_delinquent():
    """Scrape delinquent tax accounts from Harris County Tax Office."""
    log.info("=== Harris County Tax Office ===")
    r = get(TAX_URL)
    if not r:
        log.warning("Tax Office: no response")
        return []

    soup = BeautifulSoup(r.text, "lxml")
    log.info("Tax page title: %s", soup.title.string if soup.title else "n/a")
    log.debug("Tax HTML[:1500]: %s", r.text[:1500])

    records = []
    tables = soup.find_all("table")
    log.info("Tax Office: %d table(s) found", len(tables))

    for table in tables:
        rows = table.find_all("tr")
        for row in rows[1:]:
            cells = [clean(td.get_text()) for td in row.find_all("td")]
            if len(cells) < 2:
                continue
            rec = {
                "source":      "Harris Co. Tax Office",
                "doc_type":    "TAX_DELINQUENT",
                "case_number": cells[0] if cells else "",
                "owner":       cells[1] if len(cells) > 1 else "",
                "address":     cells[2] if len(cells) > 2 else "",
                "filing_date": cells[3] if len(cells) > 3 else "",
                "score":       0,
                "signals":     ["tax_delinquency"],
            }
            if rec["owner"] or rec["address"]:
                records.append(rec)

    log.info("Tax Office → %d record(s)", len(records))
    return records

# ── 3. HCAD Property Search — Recent deed activity ─────────────────────────

HCAD_URL = "https://hcad.org/property-search/real-property/"

def scrape_hcad():
    """Scrape HCAD for recent property transfers / distressed indicators."""
    log.info("=== HCAD Property Search ===")
    r = get(HCAD_URL)
    if not r:
        log.warning("HCAD: no response")
        return []

    soup = BeautifulSoup(r.text, "lxml")
    log.info("HCAD page title: %s", soup.title.string if soup.title else "n/a")
    log.debug("HCAD HTML[:1500]: %s", r.text[:1500])

    records = []
    tables = soup.find_all("table")
    log.info("HCAD: %d table(s) found", len(tables))

    for table in tables:
        rows = table.find_all("tr")
        for row in rows[1:]:
            cells = [clean(td.get_text()) for td in row.find_all("td")]
            if len(cells) < 2:
                continue
            rec = {
                "source":      "HCAD",
                "doc_type":    "PROPERTY",
                "case_number": cells[0] if cells else "",
                "owner":       cells[1] if len(cells) > 1 else "",
                "address":     cells[2] if len(cells) > 2 else "",
                "filing_date": "",
                "score":       0,
                "signals":     [],
            }
            if rec["owner"] or rec["address"]:
                records.append(rec)

    log.info("HCAD → %d record(s)", len(records))
    return records

# ── Scoring ────────────────────────────────────────────────────────────────

WEIGHTS = {
    "tax_delinquency": 30,
    "foreclosure":     30,
    "code_violation":  25,
    "probate":         20,
    "multiple_liens":  15,
    "divorce":         10,
}

def score_lead(rec):
    pts = sum(WEIGHTS.get(s, 0) for s in rec.get("signals", []))
    rec["score"] = min(pts, 100)
    return rec

# ── Output writers ─────────────────────────────────────────────────────────

def write_json(records):
    path = DATA / "output.json"
    payload = {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "source": "Harris County District Clerk / Tax Office / HCAD",
        "total": len(records),
        "records": records,
    }
    path.write_text(json.dumps(payload, indent=2, default=str))
    log.info("Wrote %s (%d records)", path, len(records))

def write_ghl_csv(records):
    path = DATA / "ghl_export.csv"
    fields = ["owner", "address", "source", "doc_type",
              "filing_date", "case_number", "score", "signals"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in records:
            row = dict(r)
            row["signals"] = ", ".join(r.get("signals", []))
            w.writerow(row)
    log.info("Wrote %s", path)

def write_dashboard(records):
    path = DASH / "index.html"
    now  = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    rows = ""
    for r in sorted(records, key=lambda x: x.get("score", 0), reverse=True):
        sig   = ", ".join(r.get("signals", []))
        score = r.get("score", 0)
        color = "#c0392b" if score >= 50 else "#e67e22" if score >= 25 else "#27ae60"
        rows += (
            f"<tr>"
            f"<td style='color:{color};font-weight:bold'>{score}</td>"
            f"<td>{r.get('owner','')}</td>"
            f"<td>{r.get('address','')}</td>"
            f"<td>{r.get('source','')}</td>"
            f"<td>{r.get('doc_type','')}</td>"
            f"<td>{r.get('filing_date','')}</td>"
            f"<td>{r.get('case_number','')}</td>"
            f"<td>{sig}</td>"
            f"</tr>\n"
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Harris County — Motivated Seller Leads</title>
<style>
  body{{font-family:Arial,sans-serif;margin:20px;background:#f5f5f5}}
  h1{{color:#2c3e50}}
  .meta{{color:#666;margin-bottom:16px}}
  table{{border-collapse:collapse;width:100%;background:#fff;box-shadow:0 1px 4px rgba(0,0,0,.15)}}
  th{{background:#2c3e50;color:#fff;padding:10px 12px;text-align:left;cursor:pointer}}
  td{{padding:9px 12px;border-bottom:1px solid #eee;font-size:.9em}}
  tr:hover td{{background:#f0f8ff}}
</style>
</head>
<body>
<h1>🏘 Harris County (Houston, TX) — Motivated Seller Leads</h1>
<p class="meta">Last updated: {now} &nbsp;|&nbsp; Total leads: {len(records)}</p>
<table id="t">
<thead>
<tr>
  <th onclick="sortTable(0)">Score ▼</th>
  <th onclick="sortTable(1)">Owner</th>
  <th onclick="sortTable(2)">Address</th>
  <th onclick="sortTable(3)">Source</th>
  <th onclick="sortTable(4)">Type</th>
  <th onclick="sortTable(5)">Date</th>
  <th onclick="sortTable(6)">Case #</th>
  <th onclick="sortTable(7)">Signals</th>
</tr>
</thead>
<tbody>
{rows if rows else '<tr><td colspan="8" style="text-align:center;padding:30px;color:#999">No leads found — check Actions logs</td></tr>'}
</tbody>
</table>
<script>
function sortTable(col){{
  const t=document.getElementById('t'),rows=[...t.tBodies[0].rows];
  const asc=t.dataset.sort==col;t.dataset.sort=asc?'':col;
  rows.sort((a,b)=>{{
    const x=a.cells[col].innerText,y=b.cells[col].innerText;
    return asc?(isNaN(x)?x>y?1:-1:+x-+y):(isNaN(y)?y>x?1:-1:+y-+x);
  }});
  rows.forEach(r=>t.tBodies[0].append(r));
}}
</script>
</body>
</html>"""
    path.write_text(html, encoding="utf-8")
    log.info("Wrote %s", path)

# ── Main ───────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days",  type=int, default=7)
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("=== Harris County Scraper v4 starting ===")
    log.info("Days back: %d | Limit: %d", args.days, args.limit)

    all_records = []
    all_records += scrape_clerk_foreclosures(days_back=args.days)
    all_records += scrape_tax_delinquent()
    all_records += scrape_hcad()

    for r in all_records:
        score_lead(r)

    all_records.sort(key=lambda x: x.get("score", 0), reverse=True)
    all_records = all_records[:args.limit]

    log.info("=== Total leads: %d ===", len(all_records))

    write_json(all_records)
    write_ghl_csv(all_records)
    write_dashboard(all_records)

    log.info("=== Done ===")

if __name__ == "__main__":
    main()
