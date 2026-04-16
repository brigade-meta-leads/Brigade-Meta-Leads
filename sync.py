"""
Brigade Meta Leads — GitHub Actions Sync
Runs every 15 minutes via GitHub Actions cron.
Fetches last 48h of leads from Meta → appends new ones to OneDrive Excel.
State tracked in .pushed_ids.json (committed back to repo after each run).
"""

import requests
import os, json, re
from datetime import datetime, timezone, timedelta
import msal

IST         = timezone(timedelta(hours=5, minutes=30))
PUSHED_FILE = os.path.join(os.path.dirname(__file__), '.pushed_ids.json')

META_TOKEN    = os.environ['META_SYSTEM_USER_TOKEN']
MS_CLIENT_ID  = os.environ.get('MS_CLIENT_ID', '14d82eec-204b-4c2f-b7e8-296a70dab67e')
MS_FILE_ID    = os.environ.get('MS_FILE_ID', '8CA35C33-8198-4AF1-83DF-726B315CD656')
MS_GRAPH_BASE = f"https://graph.microsoft.com/v1.0/me/drive/items/{MS_FILE_ID}/workbook/worksheets"

FORMS = [
    {"sheet": "Woodrose - Social Events",    "form_id": "979047675186445",  "campaign_start": "25 Mar 2026"},
    {"sheet": "Woodrose - Club Membership",  "form_id": "2373227253178965", "campaign_start": "25 Mar 2026"},
    {"sheet": "Regent - Club Membership",    "form_id": "1345460640739280", "campaign_start": "25 Mar 2026"},
    {"sheet": "Regent - Social Events",      "form_id": "1256247030047629", "campaign_start": "25 Mar 2026"},
    {"sheet": "Augusta - Club Membership",   "form_id": "1985356756189741", "campaign_start": "25 Mar 2026"},
    {"sheet": "Augusta - Social Events",     "form_id": "4400562656882365", "campaign_start": "25 Mar 2026"},
    {"sheet": "Galaxy - Club Membership",    "form_id": "1652253566011112", "campaign_start": "25 Mar 2026"},
    {"sheet": "Galaxy - Social Events",      "form_id": "830344982769325",  "campaign_start": "25 Mar 2026"},
    {"sheet": "Signature - Club Membership", "form_id": "1958383678398355", "campaign_start": "25 Mar 2026"},
    {"sheet": "Signature - Day Out",         "form_id": "2087614135361752", "campaign_start": "25 Mar 2026"},
]

STANDARD_FIELDS = {"full_name": "Name", "phone_number": "Phone", "email": "Email"}
BASE_COLS       = ["Name", "Phone", "Email", "Submitted At", "Campaign Start"]


# ── Pushed IDs ─────────────────────────────────────────────────────────────────

def load_pushed():
    if os.path.exists(PUSHED_FILE):
        with open(PUSHED_FILE) as f:
            return set(json.load(f))
    return set()

def save_pushed(pushed_ids):
    with open(PUSHED_FILE, 'w') as f:
        json.dump(sorted(pushed_ids), f, indent=2)


# ── Microsoft Token ────────────────────────────────────────────────────────────

def get_ms_token():
    refresh_token = os.environ['MS_REFRESH_TOKEN']
    app    = msal.PublicClientApplication(MS_CLIENT_ID, authority='https://login.microsoftonline.com/common')
    result = app.acquire_token_by_refresh_token(refresh_token, scopes=['Files.ReadWrite', 'User.Read'])
    if 'access_token' not in result:
        raise RuntimeError(f"MS token refresh failed: {result.get('error_description')}")
    return result['access_token']


# ── Meta API ───────────────────────────────────────────────────────────────────

def fetch_field_labels(form_id):
    r = requests.get(
        f"https://graph.facebook.com/v21.0/{form_id}",
        params={"access_token": META_TOKEN, "fields": "questions"}
    )
    label_map = dict(STANDARD_FIELDS)
    value_map = {}
    for q in r.json().get("questions", []):
        key = q.get("key", "")
        if key not in STANDARD_FIELDS:
            label_map[key] = q.get("label", key)
        options = q.get("options", [])
        if options:
            value_map[key] = {opt["key"]: opt["value"] for opt in options}
    return label_map, value_map

def fmt_date(iso_str):
    try:
        dt = datetime.fromisoformat(iso_str.replace('+0000', '+00:00'))
        return dt.astimezone(IST).strftime("%-d %b %Y, %-I:%M %p")
    except Exception:
        return iso_str

def fetch_leads(form_id, since_ts):
    label_map, value_map = fetch_field_labels(form_id)
    leads, url = [], f"https://graph.facebook.com/v21.0/{form_id}/leads"
    params = {
        "access_token": META_TOKEN,
        "fields": "created_time,field_data,id",
        "limit": 100,
        "filtering": json.dumps([{"field": "time_created", "operator": "GREATER_THAN", "value": since_ts}])
    }
    while url:
        r    = requests.get(url, params=params)
        data = r.json()
        if "error" in data:
            print(f"  [Meta] Error {form_id}: {data['error']['message']}", flush=True)
            return []
        for lead in data.get("data", []):
            row = {"_id": lead["id"], "Submitted At": fmt_date(lead["created_time"])}
            for field in lead.get("field_data", []):
                col = label_map.get(field["name"], field["name"])
                raw = field["values"][0] if field.get("values") else ""
                row[col] = value_map.get(field["name"], {}).get(raw, raw)
            leads.append(row)
        url    = data.get("paging", {}).get("next")
        params = {}
    return leads


# ── OneDrive Append ────────────────────────────────────────────────────────────

def fix_phone(p):
    p = str(p).strip()
    if p and not p.startswith('+') and p.isdigit() and len(p) >= 10:
        return '+' + p
    return p

def append_to_onedrive(sheet, rows, ms_token):
    enc     = requests.utils.quote(sheet, safe='')
    headers = {'Authorization': f'Bearer {ms_token}', 'Content-Type': 'application/json'}

    sr = requests.get(f"{MS_GRAPH_BASE}('{enc}')/usedRange", headers=headers, params={'select': 'rowCount,values'})
    if sr.status_code == 200:
        sheet_data = sr.json()
        all_rows   = sheet_data.get('values', [])
        col_order  = [c for c in (all_rows[1] if len(all_rows) > 1 else []) if c]
        next_row   = sheet_data.get('rowCount', 2) + 1
    else:
        col_order = BASE_COLS
        next_row  = 3

    if not col_order:
        col_order = BASE_COLS

    # Fix phone formatting
    for row in rows:
        if 'Phone' in row:
            row['Phone'] = fix_phone(row['Phone'])

    values     = [[str(row.get(c, '') or '') for c in col_order] for row in rows]
    col_letter = chr(64 + len(col_order)) if len(col_order) <= 26 else 'Z'
    range_addr = f"A{next_row}:{col_letter}{next_row + len(values) - 1}"

    # Format Phone and Campaign Start as text
    for col_name in ('Phone', 'Campaign Start'):
        if col_name in col_order:
            cl         = chr(65 + col_order.index(col_name))
            col_range  = f"{cl}{next_row}:{cl}{next_row + len(values) - 1}"
            requests.patch(
                f"{MS_GRAPH_BASE}('{enc}')/range(address='{col_range}')/format",
                headers=headers, json={'numberFormat': '@'}
            )

    patch = requests.patch(
        f"{MS_GRAPH_BASE}('{enc}')/range(address='{range_addr}')",
        headers=headers, json={'values': values}
    )
    if patch.status_code in (200, 201):
        print(f"  [OneDrive] {sheet}: +{len(values)} row(s) ✅", flush=True)
        return True
    else:
        print(f"  [OneDrive] {sheet}: FAILED {patch.status_code} — {patch.text[:200]}", flush=True)
        return False


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    now      = datetime.now(timezone.utc)
    since_ts = int((now - timedelta(hours=48)).timestamp())
    print(f"[{now.strftime('%Y-%m-%d %H:%M UTC')}] Starting Brigade leads sync...", flush=True)

    pushed_ids = load_pushed()
    ms_token   = None
    any_new    = False

    for form in FORMS:
        sheet = form["sheet"]
        leads = fetch_leads(form["form_id"], since_ts)

        # Filter: only leads not already pushed to OneDrive
        new_leads = [l for l in leads if l['_id'] not in pushed_ids]

        if not new_leads:
            continue

        any_new = True
        print(f"  {sheet}: {len(new_leads)} new lead(s)", flush=True)

        # Add campaign_start field
        for l in new_leads:
            l['Campaign Start'] = form["campaign_start"]

        if ms_token is None:
            ms_token = get_ms_token()

        ok = append_to_onedrive(sheet, new_leads, ms_token)
        if ok:
            for l in new_leads:
                pushed_ids.add(l['_id'])
            save_pushed(pushed_ids)

    if not any_new:
        print("  No new leads.", flush=True)

    print("Done.", flush=True)


if __name__ == '__main__':
    main()
