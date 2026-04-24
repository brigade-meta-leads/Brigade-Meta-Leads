"""
Brigade Meta Leads — Dashboard Generator
Fetches all leads from Meta API and generates a self-contained HTML dashboard.
Features: date range filter, Excel export, all form question fields.
"""

import requests, os, json, re
from datetime import datetime, timezone, timedelta
from collections import defaultdict

META_TOKEN   = os.environ['META_SYSTEM_USER_TOKEN']
IST          = timezone(timedelta(hours=5, minutes=30))
HIST_FILE    = os.path.join(os.path.dirname(__file__), 'historical_leads.json')
RAW_DIR      = os.path.join(os.path.dirname(__file__), 'raw_leads')

FORMS = [
    {"property": "Signature",  "sheet": "Signature - Club Membership", "form_id": "1958383678398355", "type": "Club Membership"},
    {"property": "Signature",  "sheet": "Signature - Day Out",         "form_id": "2087614135361752", "type": "Day Out"},
    {"property": "Woodrose",   "sheet": "Woodrose - Club Membership",  "form_id": "2373227253178965", "type": "Club Membership"},
    {"property": "Woodrose",   "sheet": "Woodrose - Social Events",    "form_id": "979047675186445",  "type": "Social Events"},
    {"property": "Regent",     "sheet": "Regent - Club Membership",    "form_id": "1345460640739280", "type": "Club Membership"},
    {"property": "Regent",     "sheet": "Regent - Social Events",      "form_id": "1256247030047629", "type": "Social Events"},
    {"property": "Augusta",    "sheet": "Augusta - Club Membership",   "form_id": "1985356756189741", "type": "Club Membership"},
    {"property": "Augusta",    "sheet": "Augusta - Social Events",     "form_id": "4400562656882365", "type": "Social Events"},
    {"property": "Galaxy",     "sheet": "Galaxy - Club Membership",    "form_id": "1652253566011112", "type": "Club Membership"},
    {"property": "Galaxy",     "sheet": "Galaxy - Social Events",      "form_id": "830344982769325",  "type": "Social Events"},
]

PROPERTIES = ["Signature", "Woodrose", "Regent", "Augusta", "Galaxy"]
COLORS     = {
    "Signature": "#1a3c6e",
    "Woodrose":  "#6e1a3c",
    "Regent":    "#1a6e3c",
    "Augusta":   "#6e4a1a",
    "Galaxy":    "#3c1a6e",
}


def fmt_date(iso_str):
    try:
        dt = datetime.fromisoformat(iso_str.replace('+0000', '+00:00'))
        return dt.astimezone(IST).strftime("%d %b %Y, %I:%M %p")
    except Exception:
        return iso_str


def iso_to_ts(iso_str):
    try:
        dt = datetime.fromisoformat(iso_str.replace('+0000', '+00:00'))
        return dt.astimezone(IST).strftime("%Y-%m-%d")
    except Exception:
        return ""


def fetch_all_leads(form_id):
    leads = []
    url   = f"https://graph.facebook.com/v21.0/{form_id}/leads"

    # Get field labels + option maps
    r         = requests.get(f"https://graph.facebook.com/v21.0/{form_id}",
                             params={"access_token": META_TOKEN, "fields": "questions"})
    label_map = {"full_name": "Name", "phone_number": "Phone", "email": "Email"}
    value_map = {}
    q_order   = ["Name", "Phone", "Email"]   # preserve question order

    for q in r.json().get("questions", []):
        key   = q.get("key", "")
        label = label_map.get(key, q.get("label", key))
        if key not in label_map:
            label_map[key] = label
        if label not in q_order:
            q_order.append(label)
        if q.get("options"):
            value_map[key] = {o["key"]: o["value"] for o in q["options"]}

    params = {"access_token": META_TOKEN, "fields": "created_time,field_data,id", "limit": 100}

    while url:
        resp = requests.get(url, params=params)
        data = resp.json()
        if "error" in data:
            print(f"  Error {form_id}: {data['error']['message']}", flush=True)
            return [], q_order
        for lead in data.get("data", []):
            row = {
                "id":      lead["id"],
                "date":    fmt_date(lead["created_time"]),
                "date_ts": iso_to_ts(lead["created_time"]),
            }
            for field in lead.get("field_data", []):
                col = label_map.get(field["name"], field["name"])
                raw = field["values"][0] if field.get("values") else ""
                row[col] = value_map.get(field["name"], {}).get(raw, raw)
            leads.append(row)
        url    = data.get("paging", {}).get("next")
        params = {}

    return leads, q_order


def load_historical():
    """Load older leads from historical_leads.json (leads no longer in Meta API)."""
    if os.path.exists(HIST_FILE):
        with open(HIST_FILE) as f:
            return json.load(f)
    return []

def load_from_raw_csvs():
    """Load leads from local raw CSVs — used when running locally to capture older leads."""
    if not os.path.exists(RAW_DIR):
        return []
    import csv
    leads = []
    sheet_to_form = {f["sheet"]: f for f in FORMS}
    for fname in os.listdir(RAW_DIR):
        if not fname.endswith('.csv'):
            continue
        sheet = fname[:-4]
        form  = sheet_to_form.get(sheet)
        if not form:
            continue
        with open(os.path.join(RAW_DIR, fname), newline='', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                lead = dict(row)
                lead["id"]         = lead.pop("_id", lead.get("id", ""))
                lead["date"]       = lead.pop("Submitted At", "")
                lead["date_ts"]    = ""
                lead["_property"]  = form["property"]
                lead["_form_type"] = form["type"]
                lead["_sheet"]     = sheet
                # Parse date_ts from date string e.g. "8 Apr 2026, 6:28 PM"
                try:
                    dt = datetime.strptime(lead["date"], "%d %b %Y, %I:%M %p")
                    lead["date_ts"] = dt.strftime("%Y-%m-%d")
                except Exception:
                    pass
                leads.append(lead)
    return leads

def fetch_all_data():
    all_leads    = []
    form_schemas = {}
    seen_ids     = set()

    # 1. Load historical leads (committed to repo)
    hist = load_historical()
    for l in hist:
        if l.get("id") and l["id"] not in seen_ids:
            seen_ids.add(l["id"])
            all_leads.append(l)

    # 2. Fetch fresh leads from Meta API
    for form in FORMS:
        print(f"  Fetching {form['sheet']}...", flush=True)
        leads, q_order = fetch_all_leads(form["form_id"])
        print(f"    → {len(leads)} leads", flush=True)
        form_schemas[form["sheet"]] = q_order
        for lead in leads:
            lead["_property"]  = form["property"]
            lead["_form_type"] = form["type"]
            lead["_sheet"]     = form["sheet"]
            if lead.get("id") and lead["id"] not in seen_ids:
                seen_ids.add(lead["id"])
                all_leads.append(lead)

    return all_leads, form_schemas

def update_historical_from_csvs():
    """Run locally to pull older leads from raw CSVs into historical_leads.json."""
    existing = load_historical()
    seen_ids = {l.get("id") for l in existing if l.get("id")}
    csv_leads = load_from_raw_csvs()
    new_hist  = [l for l in csv_leads if l.get("id") and l["id"] not in seen_ids]
    if new_hist:
        all_hist = existing + new_hist
        with open(HIST_FILE, 'w') as f:
            json.dump(all_hist, f, ensure_ascii=False, indent=2)
        print(f"Added {len(new_hist)} historical leads to {HIST_FILE}", flush=True)
    else:
        print("No new historical leads to add.", flush=True)
    return existing + new_hist


def generate(all_leads, form_schemas):
    now_str = datetime.now(IST).strftime("%d %b %Y, %I:%M %p IST")

    # Fix phone formatting
    for lead in all_leads:
        p = lead.get("Phone", "")
        if p and not p.startswith("+") and str(p).isdigit() and len(str(p)) >= 10:
            lead["Phone"] = "+" + str(p)

    # Sort newest first
    all_leads.sort(key=lambda l: l.get("date_ts", ""), reverse=True)

    # Date bounds for the date picker defaults
    dates     = [l["date_ts"] for l in all_leads if l.get("date_ts")]
    min_date  = min(dates) if dates else ""
    max_date  = datetime.now(IST).strftime("%Y-%m-%d")  # always allow selecting today

    # Build per-property schemas (union of all question columns)
    prop_schemas = defaultdict(list)
    for sheet, cols in form_schemas.items():
        prop = next((f["property"] for f in FORMS if f["sheet"] == sheet), None)
        if prop:
            for c in cols:
                if c not in prop_schemas[prop]:
                    prop_schemas[prop].append(c)

    # Stats
    total     = len(all_leads)
    week_cut  = (datetime.now(IST) - timedelta(days=7)).strftime("%Y-%m-%d")
    today_cut = (datetime.now(IST) - timedelta(days=1)).strftime("%Y-%m-%d")
    this_week = sum(1 for l in all_leads if l.get("date_ts","") >= week_cut)
    today     = sum(1 for l in all_leads if l.get("date_ts","") >= today_cut)
    prop_counts = {p: sum(1 for l in all_leads if l["_property"] == p) for p in PROPERTIES}

    # Pre-build all dynamic HTML/JS fragments before f-string
    leads_json   = json.dumps(all_leads, ensure_ascii=False)
    schemas_json = json.dumps({p: prop_schemas[p] for p in PROPERTIES}, ensure_ascii=False)
    colors_json  = json.dumps(COLORS)
    props_json   = json.dumps(PROPERTIES)

    prop_tabs = "".join(
        '<button class="tab" onclick="showTab(\'' + p.lower() + '\',this)">' + p + '</button>'
        for p in PROPERTIES
    )
    prop_counts_str = ", ".join(
        str(sum(1 for l in all_leads if l["_property"] == p)) for p in PROPERTIES
    )
    active_props = sum(1 for p in PROPERTIES if any(l["_property"] == p for l in all_leads))

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Brigade Hospitality — Leads Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/xlsx@0.18.5/dist/xlsx.full.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f0f2f5;color:#1a1a2e}}

.header{{background:linear-gradient(135deg,#1a1a2e 0%,#16213e 60%,#0f3460 100%);color:white;padding:24px 36px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px}}
.header h1{{font-size:1.6rem;font-weight:700}}
.header p{{opacity:.6;font-size:.82rem;margin-top:3px}}
.updated{{background:rgba(255,255,255,.12);padding:5px 14px;border-radius:20px;font-size:.72rem}}

.container{{max-width:1280px;margin:0 auto;padding:24px 20px}}

.summary{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:22px}}
.card{{background:white;border-radius:12px;padding:18px 22px;box-shadow:0 2px 8px rgba(0,0,0,.06)}}
.card-val{{font-size:1.9rem;font-weight:700;color:#0f3460}}
.card-lbl{{font-size:.7rem;text-transform:uppercase;letter-spacing:.8px;color:#9ca3af;margin-top:3px;font-weight:600}}

.toolbar{{background:white;border-radius:12px;padding:16px 20px;box-shadow:0 2px 8px rgba(0,0,0,.06);margin-bottom:22px;display:flex;flex-wrap:wrap;gap:14px;align-items:center}}
.toolbar label{{font-size:.78rem;font-weight:600;color:#6b7280;text-transform:uppercase;letter-spacing:.5px}}
.toolbar input[type=date]{{padding:7px 12px;border:1.5px solid #e5e7eb;border-radius:8px;font-size:.84rem;color:#1a1a2e;outline:none}}
.toolbar input[type=date]:focus{{border-color:#0f3460}}
.toolbar-sep{{flex:1}}
.btn{{padding:8px 18px;border-radius:8px;font-size:.82rem;font-weight:600;cursor:pointer;border:none;transition:all .18s}}
.btn-download{{background:#16a34a;color:white}}
.btn-download:hover{{background:#15803d}}
.btn-reset{{background:#f3f4f6;color:#374151}}
.btn-reset:hover{{background:#e5e7eb}}
.result-count{{font-size:.8rem;color:#9ca3af;font-weight:500}}

.nav{{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:20px}}
.tab{{padding:8px 18px;border-radius:10px;border:2px solid #e5e7eb;background:white;color:#6b7280;font-size:.82rem;font-weight:600;cursor:pointer;transition:all .18s}}
.tab.active{{background:#0f3460;color:white;border-color:#0f3460}}
.tab:hover:not(.active){{border-color:#0f3460;color:#0f3460}}

.panel{{display:none}}
.panel.visible{{display:block}}

.prop-header{{border-radius:12px 12px 0 0;padding:18px 24px;display:flex;justify-content:space-between;align-items:center;color:white}}
.prop-name{{font-size:1.1rem;font-weight:700}}
.prop-sub{{font-size:.78rem;opacity:.7;margin-top:2px}}
.prop-stats{{display:flex;gap:24px}}
.ps-val{{font-size:1.3rem;font-weight:700;text-align:center}}
.ps-lbl{{font-size:.62rem;opacity:.72;text-transform:uppercase;letter-spacing:.4px;text-align:center}}

.table-card{{background:white;border-radius:0 0 12px 12px;box-shadow:0 2px 10px rgba(0,0,0,.07);margin-bottom:20px;overflow:hidden}}
.table-wrap{{overflow-x:auto}}
table{{width:100%;border-collapse:collapse;font-size:.82rem}}
th{{background:#1a1a2e;color:white;padding:10px 13px;text-align:left;font-size:.72rem;letter-spacing:.4px;white-space:nowrap;position:sticky;top:0}}
td{{padding:9px 13px;border-bottom:1px solid #f3f4f6;white-space:nowrap;max-width:260px;overflow:hidden;text-overflow:ellipsis}}
tr:last-child td{{border-bottom:none}}
tr:hover td{{background:#f8f9ff}}
.empty{{padding:36px;text-align:center;color:#9ca3af;font-size:.88rem}}

.remark-td{{white-space:normal!important;min-width:200px;max-width:320px}}
.remark-input{{width:100%;padding:5px 8px;border:1.5px solid #e5e7eb;border-radius:6px;font-size:.8rem;font-family:inherit;color:#1a1a2e;background:#fffdf0;resize:none;outline:none;transition:border-color .15s}}
.remark-input:focus{{border-color:#0f3460;background:#fff}}
.remark-input::placeholder{{color:#c4b8a0}}

.footer{{text-align:center;padding:22px;color:#9ca3af;font-size:.7rem}}

@media(max-width:700px){{
  .summary{{grid-template-columns:repeat(2,1fr)}}
  .prop-stats{{gap:14px}}
  .toolbar{{flex-direction:column;align-items:flex-start}}
}}
</style>
</head>
<body>

<div class="header">
  <div>
    <h1>Brigade Hospitality</h1>
    <p>Meta Ads — Leads Dashboard</p>
  </div>
  <div class="updated">Updated {now_str}</div>
</div>

<div class="container">

  <!-- Summary cards -->
  <div class="summary">
    <div class="card"><div class="card-val" id="stat-total">{total}</div><div class="card-lbl">Total Leads</div></div>
    <div class="card"><div class="card-val" id="stat-week">{this_week}</div><div class="card-lbl">This Week</div></div>
    <div class="card"><div class="card-val" id="stat-today">{today}</div><div class="card-lbl">Last 24 Hours</div></div>
    <div class="card"><div class="card-val">{active_props}</div><div class="card-lbl">Properties Active</div></div>
  </div>

  <!-- Toolbar -->
  <div class="toolbar">
    <label>From</label>
    <input type="date" id="date-from" value="{min_date}" min="{min_date}" max="{max_date}">
    <label>To</label>
    <input type="date" id="date-to" value="{max_date}" min="{min_date}" max="{max_date}">
    <button class="btn btn-reset" onclick="resetDates()">Reset</button>
    <div class="toolbar-sep"></div>
    <span class="result-count" id="result-count"></span>
    <button class="btn btn-download" onclick="downloadExcel()">⬇ Download Excel</button>
  </div>

  <!-- Tabs -->
  <div class="nav" id="tab-nav">
    <button class="tab active" onclick="showTab('all',this)">All Properties</button>
    {prop_tabs}
  </div>

  <!-- Panels populated by JS -->
  <div id="panels"></div>

</div>

<div class="footer">Brigade Hospitality x Gautami &nbsp;|&nbsp; Meta Ads only &nbsp;|&nbsp; Auto-updated every 15 min</div>

<script>
const ALL_LEADS   = {leads_json};
const SCHEMAS     = {schemas_json};
const COLORS      = {colors_json};
const PROPERTIES  = {props_json};

const FIXED_COLS  = ['Name','Phone','Email'];
const META_COLS   = ['_property','_form_type','_sheet','id','date','date_ts'];

// ── Remarks (localStorage, keyed by lead ID) ───────────────────────────────
function getRemark(id)       {{ return localStorage.getItem('remark_' + id) || ''; }}
function saveRemark(id, val) {{ val ? localStorage.setItem('remark_' + id, val) : localStorage.removeItem('remark_' + id); }}
function loadRemarks() {{
  document.querySelectorAll('.remark-input').forEach(el => {{
    el.value = getRemark(el.dataset.id);
  }});
}}

let currentTab    = 'all';
let filteredLeads = [...ALL_LEADS];

// ── Date filter ────────────────────────────────────────────────────────────────
function getFiltered() {{
  const from = document.getElementById('date-from').value;
  const to   = document.getElementById('date-to').value;
  return ALL_LEADS.filter(l => {{
    const d = l.date_ts || '';
    return (!from || d >= from) && (!to || d <= to);
  }});
}}

function applyFilter() {{
  filteredLeads = getFiltered();
  document.getElementById('result-count').textContent = filteredLeads.length + ' leads';
  renderPanels();
}}

function resetDates() {{
  const dates = ALL_LEADS.map(l=>l.date_ts).filter(Boolean).sort();
  document.getElementById('date-from').value = dates[0] || '';
  document.getElementById('date-to').value   = dates[dates.length-1] || '';
  applyFilter();
}}

document.getElementById('date-from').addEventListener('change', applyFilter);
document.getElementById('date-to').addEventListener('change', applyFilter);

// ── Tab switching ──────────────────────────────────────────────────────────────
function showTab(id, el) {{
  currentTab = id;
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  el.classList.add('active');
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('visible'));
  const panel = document.getElementById('panel-' + id);
  if (panel) panel.classList.add('visible');
}}

// ── Build table ────────────────────────────────────────────────────────────────
function extraCols(leads) {{
  const skip = new Set([...META_COLS, ...FIXED_COLS]);
  const cols = [];
  leads.forEach(l => Object.keys(l).forEach(k => {{ if (!skip.has(k) && !cols.includes(k)) cols.push(k); }}));
  return cols;
}}

function buildTable(leads, showProp) {{
  if (!leads.length) return '<div class="empty">No leads in this date range.</div>';

  const extra = extraCols(leads);
  const cols  = showProp
    ? ['Name','Phone','Email','Property','Form','Submitted At',...extra]
    : ['Name','Phone','Email','Form','Submitted At',...extra];

  const allCols = [...cols, 'Remarks'];
  const header = allCols.map(c=>`<th>${{c}}</th>`).join('');
  const rows   = leads.map(l => {{
    const cells = cols.map(c => {{
      if (c === 'Property')     return `<td>${{l._property||'—'}}</td>`;
      if (c === 'Form')         return `<td>${{l._form_type||'—'}}</td>`;
      if (c === 'Submitted At') return `<td>${{l.date||'—'}}</td>`;
      return `<td title="${{(l[c]||'').toString().replace(/"/g,"'")}}">${{l[c]||'—'}}</td>`;
    }}).join('');
    const remarkCell = `<td class="remark-td"><textarea class="remark-input" rows="2" data-id="${{l.id}}" placeholder="Add remarks…" oninput="saveRemark('${{l.id}}',this.value)"></textarea></td>`;
    return `<tr>${{cells}}${{remarkCell}}</tr>`;
  }}).join('');

  return `<div class="table-wrap"><table><thead><tr>${{header}}</tr></thead><tbody>${{rows}}</tbody></table></div>`;
}}

// ── Render panels ──────────────────────────────────────────────────────────────
function renderPanels() {{
  const container = document.getElementById('panels');
  let html = '';

  // All panel
  const today    = new Date().toISOString().slice(0,10);
  const weekAgo  = new Date(Date.now()-7*86400000).toISOString().slice(0,10);
  const allTotal = filteredLeads.length;
  const allWeek  = filteredLeads.filter(l=>(l.date_ts||'')>=weekAgo).length;
  const allToday = filteredLeads.filter(l=>(l.date_ts||'')>=today).length;

  html += `<div class="panel ${{currentTab==='all'?'visible':''}}" id="panel-all">
    <div class="prop-header" style="background:#1a1a2e;border-radius:12px 12px 0 0">
      <div><div class="prop-name">All Properties</div><div class="prop-sub">${{allTotal}} leads in range</div></div>
      <div class="prop-stats">
        <div><div class="ps-val">${{allTotal}}</div><div class="ps-lbl">Total</div></div>
        <div><div class="ps-val">${{allWeek}}</div><div class="ps-lbl">This week</div></div>
        <div><div class="ps-val">${{allToday}}</div><div class="ps-lbl">Today</div></div>
      </div>
    </div>
    <div class="table-card">${{buildTable(filteredLeads, true)}}</div>
  </div>`;

  // Per-property panels
  PROPERTIES.forEach(prop => {{
    const color  = COLORS[prop] || '#0f3460';
    const leads  = filteredLeads.filter(l => l._property === prop);
    const pWeek  = leads.filter(l=>(l.date_ts||'')>=weekAgo).length;
    const pToday = leads.filter(l=>(l.date_ts||'')>=today).length;

    html += `<div class="panel ${{currentTab===prop.toLowerCase()?'visible':''}}" id="panel-${{prop.toLowerCase()}}">
      <div class="prop-header" style="background:${{color}};border-radius:12px 12px 0 0">
        <div><div class="prop-name">${{prop}}</div><div class="prop-sub">${{leads.length}} leads in range</div></div>
        <div class="prop-stats">
          <div><div class="ps-val">${{leads.length}}</div><div class="ps-lbl">Total</div></div>
          <div><div class="ps-val">${{pWeek}}</div><div class="ps-lbl">This week</div></div>
          <div><div class="ps-val">${{pToday}}</div><div class="ps-lbl">Today</div></div>
        </div>
      </div>
      <div class="table-card">${{buildTable(leads, false)}}</div>
    </div>`;
  }});

  container.innerHTML = html;
  document.getElementById('result-count').textContent = filteredLeads.length + ' leads';
  loadRemarks();
}}

// ── Excel download ─────────────────────────────────────────────────────────────
function downloadExcel() {{
  const wb   = XLSX.utils.book_new();
  const skip = new Set(META_COLS.filter(c=>c!=='_property'&&c!=='_form_type'));

  // Sheet per property + one "All" sheet
  const groups = {{'All Properties': filteredLeads}};
  PROPERTIES.forEach(p => {{ groups[p] = filteredLeads.filter(l=>l._property===p); }});

  Object.entries(groups).forEach(([name, leads]) => {{
    if (!leads.length) return;
    const extra = extraCols(leads);
    const cols  = ['Name','Phone','Email','Property','Form','Submitted At',...extra,'Remarks'];

    const rows = leads.map(l => {{
      const row = {{}};
      cols.forEach(c => {{
        if (c==='Property')     row[c] = l._property||'';
        else if (c==='Form')    row[c] = l._form_type||'';
        else if (c==='Submitted At') row[c] = l.date||'';
        else if (c==='Remarks') row[c] = getRemark(l.id);
        else row[c] = l[c]||'';
      }});
      return row;
    }});

    const ws = XLSX.utils.json_to_sheet(rows, {{header: cols}});

    // Column widths
    ws['!cols'] = cols.map(c => ({{wch: Math.min(Math.max(c.length+2, 12), 40)}}));

    const sheetName = name.slice(0,31);
    XLSX.utils.book_append_sheet(wb, ws, sheetName);
  }});

  const from = document.getElementById('date-from').value || 'all';
  const to   = document.getElementById('date-to').value   || 'time';
  XLSX.writeFile(wb, `Brigade_Leads_${{from}}_to_${{to}}.xlsx`);
}}

// ── Init ───────────────────────────────────────────────────────────────────────
applyFilter();
</script>
</body>
</html>"""
    return html


if __name__ == '__main__':
    # Pull older leads from local raw CSVs into historical_leads.json
    if os.path.exists(RAW_DIR):
        print("Updating historical leads from local CSVs...", flush=True)
        update_historical_from_csvs()

    print("Fetching all leads for dashboard...", flush=True)
    all_leads, form_schemas = fetch_all_data()
    total = len(all_leads)
    print(f"Total leads: {total}", flush=True)
    html = generate(all_leads, form_schemas)
    out  = os.path.join(os.path.dirname(__file__), 'dashboard.html')
    with open(out, 'w') as f:
        f.write(html)
    print(f"Dashboard saved → {out}", flush=True)
