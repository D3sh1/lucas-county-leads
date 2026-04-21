#!/usr/bin/env python3
"""
Lucas County Motivated Seller Lead Scraper — v3
Public sources only (no login required):
  1. Sheriff Sales (foreclosures)
  2. Domestic Relations (divorce)
  3. iCare (code violations)
"""

import argparse
import csv
import json
import logging
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

ROOT          = Path(__file__).resolve().parent.parent
DATA_DIR      = ROOT / "data"
DASHBOARD_DIR = ROOT / "dashboard"
OUTPUT_JSON   = DATA_DIR / "output.json"
GHL_CSV       = DATA_DIR / "ghl_export.csv"
DASHBOARD_HTML= DASHBOARD_DIR / "index.html"

SHERIFF_URL   = "http://lcapps.co.lucas.oh.us/foreclosure/"
DOMESTIC_URL  = "https://lucapps.co.lucas.oh.us/onlinedockets/Default.aspx"
ICARE_URL     = "http://icare.co.lucas.oh.us/LucasCare/search/commonsearch.aspx"

REQ_TIMEOUT   = 45
REQ_DELAY     = 2.0
RETRY_COUNT   = 2

GUIDE_WEIGHTS = {
    "tax_delinquent":       30,
    "code_violation":       25,
    "probate_filing":       20,
    "multiple_liens":       15,
    "divorce_or_bankruptcy":10,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("lucas")


@dataclass
class DistressSignals:
    tax_delinquent:        bool = False
    code_violation:        bool = False
    probate_filing:        bool = False
    multiple_liens:        bool = False
    divorce_or_bankruptcy: bool = False


@dataclass
class Lead:
    document_number:   str = ""
    doc_type_code:     str = ""
    doc_type_label:    str = ""
    doc_category:      str = ""
    file_date:         str = ""
    grantor:           str = ""
    grantee:           str = ""
    legal_description: str = ""
    amount:            str = ""
    clerk_url:         str = ""
    property_address:  str = ""
    property_city:     str = ""
    property_state:    str = "OH"
    property_zip:      str = ""
    mail_address:      str = ""
    mail_city:         str = ""
    mail_state:        str = ""
    mail_zip:          str = ""
    code_violation_ids: List[str] = field(default_factory=list)
    signals:     DistressSignals = field(default_factory=DistressSignals)
    flags:       List[str]       = field(default_factory=list)
    seller_score: int            = 0
    sources:    List[str] = field(default_factory=list)
    scraped_at: str       = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def _get(url, **kwargs):
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            time.sleep(REQ_DELAY)
            r = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT, verify=False, **kwargs)
            r.raise_for_status()
            log.info("[GET %d] %s → %d bytes", r.status_code, url, len(r.content))
            return r
        except Exception as exc:
            log.warning("GET %d/%d %s: %s", attempt, RETRY_COUNT, url, exc)
    return None


def _post(url, data, **kwargs):
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            time.sleep(REQ_DELAY)
            r = requests.post(url, headers=HEADERS, data=data, timeout=REQ_TIMEOUT, verify=False, **kwargs)
            r.raise_for_status()
            log.info("[POST %d] %s → %d bytes", r.status_code, url, len(r.content))
            return r
        except Exception as exc:
            log.warning("POST %d/%d %s: %s", attempt, RETRY_COUNT, url, exc)
    return None


def _soup(resp):
    try:
        return BeautifulSoup(resp.text, "lxml")
    except Exception:
        return BeautifulSoup(resp.text, "html.parser")


def _txt(tag):
    return tag.get_text(" ", strip=True) if tag else ""


class SheriffSalesScraper:
    def scrape(self):
        log.info("[Sheriff] Fetching %s", SHERIFF_URL)
        resp = _get(SHERIFF_URL)
        if not resp:
            return []
        bs = _soup(resp)
        log.info("[Sheriff] Title: %s | tables=%d | links=%d | forms=%d",
                 _txt(bs.title), len(bs.find_all("table")), len(bs.find_all("a")), len(bs.find_all("form")))
        leads = []
        for tbl in bs.find_all("table"):
            rows = tbl.find_all("tr")
            if len(rows) < 2:
                continue
            headers = [_txt(c).lower() for c in rows[0].find_all(["th","td"])]
            if not any(kw in " ".join(headers) for kw in ("case","address","sale","property","parcel","defendant")):
                continue
            log.info("[Sheriff] Table headers: %s", headers)
            for row in rows[1:]:
                cells = [_txt(c) for c in row.find_all(["td","th"])]
                if len(cells) < 2:
                    continue
                def _col(*kw):
                    for i,h in enumerate(headers):
                        if any(k in h for k in kw):
                            return cells[i] if i<len(cells) else ""
                    return ""
                case_num = _col("case")
                address  = _col("address","property","location")
                party    = _col("defendant","owner","debtor","party")
                sale     = _col("sale","date")
                amount   = _col("appraise","amount","minimum","judgment")
                if not (address or party or case_num):
                    continue
                leads.append(Lead(
                    document_number=case_num, doc_type_code="FORECLOSURE",
                    doc_type_label="Sheriff Sale / Foreclosure", doc_category="foreclosure",
                    file_date=sale, grantor=party, property_address=address,
                    amount=amount, clerk_url=SHERIFF_URL,
                    sources=["Lucas County Sheriff Sales"],
                ))
        log.info("[Sheriff] Parsed %d leads.", len(leads))
        return leads


class DomesticScraper:
    def scrape(self):
        log.info("[Domestic] Fetching %s", DOMESTIC_URL)
        resp = _get(DOMESTIC_URL)
        if not resp:
            return []
        bs = _soup(resp)
        log.info("[Domestic] Title: %s | tables=%d | forms=%d",
                 _txt(bs.title), len(bs.find_all("table")), len(bs.find_all("form")))
        for form in bs.find_all("form"):
            inputs = [inp.get("name","") for inp in form.find_all(["input","select"])]
            log.info("[Domestic] Form fields: %s", inputs[:20])
        leads = []
        for tbl in bs.find_all("table"):
            headers = [_txt(c).lower() for c in tbl.find_all("th")]
            if any(kw in " ".join(headers) for kw in ("case","party","filed","divorce")):
                log.info("[Domestic] Results table: %s", headers)
                for row in tbl.find_all("tr")[1:]:
                    cells = [_txt(c) for c in row.find_all("td")]
                    if len(cells) < 2:
                        continue
                    leads.append(Lead(
                        document_number=cells[0] if cells else "",
                        doc_type_code="DRJUD", doc_type_label="Divorce / Domestic Relations",
                        doc_category="divorce",
                        file_date=cells[2] if len(cells)>2 else "",
                        grantor=cells[1] if len(cells)>1 else "",
                        clerk_url=DOMESTIC_URL,
                        sources=["Lucas County Domestic Dockets"],
                    ))
        log.info("[Domestic] Parsed %d leads.", len(leads))
        return leads


class ICareScraper:
    def get_violations(self, address):
        if not address:
            return []
        resp = _get(ICARE_URL, params={"mode":"address"})
        if not resp:
            return []
        bs = _soup(resp)
        tokens = {}
        for sel,key in [("input#__VIEWSTATE","__VIEWSTATE"),
                        ("input#__EVENTVALIDATION","__EVENTVALIDATION"),
                        ("input#__VIEWSTATEGENERATOR","__VIEWSTATEGENERATOR")]:
            tag = bs.select_one(sel)
            if tag:
                tokens[key] = tag.get("value","")
        if not tokens:
            return []
        data = {**tokens,
                "ctl00$ContentPlaceHolder1$txtAddress": address,
                "ctl00$ContentPlaceHolder1$btnSearch": "Search"}
        r2 = _post(ICARE_URL, data=data, params={"mode":"address"})
        if not r2:
            return []
        ids = []
        for row in _soup(r2).select("table#GridView1 tbody tr, table.rgMasterTable tbody tr"):
            cells = row.find_all("td")
            if cells:
                cid = _txt(cells[0])
                if cid and cid.lower() not in ("case id","case number"):
                    ids.append(cid)
        return ids


def score_lead(lead):
    sig = lead.signals
    if lead.doc_type_code == "FORECLOSURE":
        sig.tax_delinquent = True
        lead.flags.append("Foreclosure / Sheriff Sale")
    if lead.doc_type_code == "DRJUD":
        sig.divorce_or_bankruptcy = True
        lead.flags.append("Divorce filing")
    if lead.code_violation_ids:
        sig.code_violation = True
        lead.flags.append(f"Code violation ({len(lead.code_violation_ids)})")
    s = 0
    s += GUIDE_WEIGHTS["tax_delinquent"]        if sig.tax_delinquent        else 0
    s += GUIDE_WEIGHTS["code_violation"]        if sig.code_violation        else 0
    s += GUIDE_WEIGHTS["probate_filing"]        if sig.probate_filing        else 0
    s += GUIDE_WEIGHTS["multiple_liens"]        if sig.multiple_liens        else 0
    s += GUIDE_WEIGHTS["divorce_or_bankruptcy"] if sig.divorce_or_bankruptcy else 0
    lead.seller_score = min(s, 100)


def flag_multiple_liens(leads):
    counts = {}
    for l in leads:
        key = l.grantor.upper().strip()
        if key:
            counts[key] = counts.get(key,0)+1
    for l in leads:
        if counts.get(l.grantor.upper().strip(),0) >= 2:
            l.signals.multiple_liens = True


def _to_dict(l):
    d = asdict(l)
    d["signals"] = asdict(l.signals)
    return d


def write_json(leads):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps([_to_dict(l) for l in leads], indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("[Output] JSON → %s (%d records)", OUTPUT_JSON, len(leads))


def write_ghl_csv(leads):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    cols = ["First Name","Last Name","Property Address","Lead Type","Document Type",
            "Date Filed","Document Number","Amount","Seller Score","Flags","Source","URL"]
    def _split(n):
        p = n.strip().split()
        return (p[0]," ".join(p[1:])) if len(p)>=2 else (n,"")
    with open(GHL_CSV,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for l in leads:
            first,last = _split(l.grantor)
            w.writerow({"First Name":first,"Last Name":last,
                "Property Address":l.property_address,"Lead Type":l.doc_category,
                "Document Type":l.doc_type_label,"Date Filed":l.file_date,
                "Document Number":l.document_number,"Amount":l.amount,
                "Seller Score":l.seller_score,"Flags":" | ".join(l.flags),
                "Source":" / ".join(l.sources),"URL":l.clerk_url})
    log.info("[Output] GHL CSV → %s", GHL_CSV)


def write_dashboard(leads):
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    recs = sorted([_to_dict(l) for l in leads], key=lambda r: r["seller_score"], reverse=True)
    data = json.dumps(recs)
    t,h,m,lo = len(recs), sum(1 for r in recs if r["seller_score"]>=70), \
               sum(1 for r in recs if 40<=r["seller_score"]<70), \
               sum(1 for r in recs if r["seller_score"]<40)
    gen = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"/>
<title>Lucas County Leads</title><style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:system-ui;background:#0f172a;color:#e2e8f0}}
header{{padding:2rem;background:linear-gradient(135deg,#1e3a5f,#0f172a);border-bottom:1px solid #1e40af}}
h1{{color:#60a5fa;font-size:1.6rem}}
.stats{{display:flex;gap:1rem;padding:1.25rem 2rem;background:#1e293b;flex-wrap:wrap}}
.stat{{flex:1;min-width:110px;background:#0f172a;padding:.75rem 1rem;border-radius:8px;border:1px solid #334155}}
.stat-v{{font-size:1.6rem;font-weight:700;margin-top:.2rem}}
.wrap{{padding:1.5rem 2rem;overflow-x:auto}}
table{{width:100%;border-collapse:collapse;font-size:.85rem}}
th{{background:#1e293b;color:#94a3b8;text-align:left;padding:.65rem;font-size:.7rem;text-transform:uppercase}}
td{{padding:.6rem;border-bottom:1px solid #1e293b}}
.b{{display:inline-block;padding:.2rem .6rem;border-radius:10px;font-weight:700;font-size:.75rem}}
.h{{background:#7f1d1d;color:#fca5a5}}.m{{background:#78350f;color:#fde68a}}.l{{background:#14532d;color:#86efac}}
.p{{display:inline-block;padding:.15rem .45rem;border-radius:9999px;font-size:.65rem;font-weight:600;margin-right:.25rem}}
.p-t{{background:#7f1d1d;color:#fca5a5}}.p-c{{background:#78350f;color:#fde68a}}
.p-p{{background:#312e81;color:#a5b4fc}}.p-l{{background:#1e3a5f;color:#93c5fd}}
.p-d{{background:#4a1d96;color:#ddd6fe}}
.empty{{text-align:center;padding:4rem;color:#475569}}
</style></head><body>
<header><h1>Lucas County — Motivated Seller Leads</h1>
<p style="color:#94a3b8;font-size:.85rem;margin-top:.25rem">{gen} | {t} leads</p></header>
<div class="stats">
<div class="stat">Total<div class="stat-v" style="color:#60a5fa">{t}</div></div>
<div class="stat">High ≥70<div class="stat-v" style="color:#ef4444">{h}</div></div>
<div class="stat">Med 40-69<div class="stat-v" style="color:#f59e0b">{m}</div></div>
<div class="stat">Low<div class="stat-v" style="color:#22c55e">{lo}</div></div>
</div>
<div class="wrap">
<table><thead><tr><th>Score</th><th>Address</th><th>Owner</th><th>Doc Type</th><th>Filed</th><th>Amount</th><th>Signals</th></tr></thead>
<tbody id="b"></tbody></table>
<div id="e" class="empty" style="display:none"><h2>No leads yet</h2><p>Check Actions logs.</p></div>
</div>
<script>
const D={data};
function pill(r){{const s=r.signals||{{}};const p=[];
if(s.tax_delinquent)p.push('<span class="p p-t">Tax</span>');
if(s.code_violation)p.push('<span class="p p-c">Code</span>');
if(s.probate_filing)p.push('<span class="p p-p">Probate</span>');
if(s.multiple_liens)p.push('<span class="p p-l">Multi-Lien</span>');
if(s.divorce_or_bankruptcy)p.push('<span class="p p-d">Divorce</span>');
return p.join('')||'—';}}
document.getElementById('b').innerHTML=D.map(r=>{{
const s=r.seller_score||0,c=s>=70?'h':s>=40?'m':'l';
return `<tr><td><span class="b ${{c}}">${{s}}</span></td><td>${{r.property_address||'—'}}</td>
<td>${{r.grantor||'—'}}</td><td>${{r.doc_type_label||'—'}}</td>
<td>${{r.file_date||'—'}}</td><td>${{r.amount||'—'}}</td><td>${{pill(r)}}</td></tr>`;}}).join('');
if(!D.length)document.getElementById('e').style.display='block';
</script></body></html>"""
    DASHBOARD_HTML.write_text(html, encoding="utf-8")
    log.info("[Output] Dashboard → %s", DASHBOARD_HTML)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-icare", action="store_true")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("Lucas County Lead Scraper v3 — public sources only")
    log.info("=" * 60)

    leads = []
    try:
        leads.extend(SheriffSalesScraper().scrape())
    except Exception as exc:
        log.error("[Sheriff] Fatal: %s", exc)
    try:
        leads.extend(DomesticScraper().scrape())
    except Exception as exc:
        log.error("[Domestic] Fatal: %s", exc)

    log.info("Total raw leads: %d", len(leads))

    if not args.skip_icare and leads:
        icare = ICareScraper()
        for i, lead in enumerate(leads, 1):
            if lead.property_address:
                try:
                    vids = icare.get_violations(lead.property_address)
                    if vids:
                        lead.code_violation_ids.extend(vids)
                        if "iCare" not in lead.sources:
                            lead.sources.append("iCare")
                except Exception:
                    pass
            if i % 10 == 0:
                log.info("iCare: %d/%d", i, len(leads))

    flag_multiple_liens(leads)
    for lead in leads:
        score_lead(lead)
    leads.sort(key=lambda l: l.seller_score, reverse=True)

    write_json(leads)
    write_ghl_csv(leads)
    write_dashboard(leads)

    log.info("=" * 60)
    log.info("Done. %d leads. Top score: %d",
             len(leads), leads[0].seller_score if leads else 0)
    log.info("=" * 60)


if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    main()
