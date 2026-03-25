"""Generate the CSS Workforce Tool notebook with hidden code cells for Colab."""
import json

cells = []

def md(source):
    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": [line + "\n" for line in source.strip().split("\n")]
    })

def code(source, title=None):
    lines = source.strip().split("\n")
    if title:
        lines = [f"#@title {title}"] + lines
    cell = {
        "cell_type": "code",
        "metadata": {},
        "source": [line + "\n" for line in lines],
        "outputs": [],
        "execution_count": None
    }
    if title:
        cell["metadata"]["cellView"] = "form"
    cells.append(cell)


# ── Cell 0: Title ──────────────────────────────────────────────────────────
md("""[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/Impact-Map/opm_ehri_data/blob/main/css_workforce_tool.ipynb)

# Federal Workforce Data Tool

**No coding required.**

This tool loads federal workforce data from [OPM/EHRI](https://data.opm.gov) and gives you menus to explore it. To get started:

1. Click the **▶▶ (Run all)** button in the toolbar, or use **Runtime → Run all**
2. Wait for the setup cells to finish running (~30 seconds) — you'll see output under each one
3. Scroll down to the tools — check the boxes for the organizations and breakdowns you want
4. Click **Generate Report** to see a table (each query takes ~10–30 seconds), then **Download CSV** to save it

**Four tools included:**
- **Workforce Size Comparison** — Compare total employees at two points in time (e.g., Jan 2025 vs. latest)
- **Separations Analysis** — Departures by mechanism (RIF, quit, retirement, DRP, etc.)
- **Accessions Analysis** — New hires by agency and category
- **Monthly Employment Trends** — Track headcount month by month since December 2024

**Note on agencies vs. subagencies:** The selector includes both full agencies (e.g., Department of Justice) and specific subagencies (e.g., FBI, IRS, CFPB) indented underneath. Selecting an agency gives you the *whole* agency; selecting a subagency gives you *just* that component.""")


# ── Cell 1: Install ────────────────────────────────────────────────────────
code("""
!pip install -q 'duckdb>=1.5,<2' 'pandas>=2,<4' 'ipywidgets>=8.1,<9' 'huggingface_hub>=1.4,<2'

import re, base64, warnings
import duckdb
import pandas as pd
import ipywidgets as widgets
from IPython.display import HTML, display, clear_output
from huggingface_hub import list_repo_files
warnings.filterwarnings('ignore')

print("Packages ready ✓")
""", title="Step 1: Install packages")


# ── Cell 2: Load data ──────────────────────────────────────────────────────
code(r"""
HF_REPO = "impactproject/opm-ehri-data"
BASE_URL = f"https://huggingface.co/datasets/{HF_REPO}/resolve/main"

all_files = list(list_repo_files(HF_REPO, repo_type="dataset"))
parquet_files = [f for f in all_files if f.endswith(".parquet")]

def friendly_name(data_type, yyyymm, version):
    # e.g. employment, 202505, 2 -> Employment May 2025 Version 2
    label = pd.to_datetime(yyyymm, format='%Y%m').strftime('%B %Y')
    return f"{data_type.capitalize()} {label} Version {version}"

def get_best_urls(data_type):
    pattern = re.compile(rf"^{data_type}/{data_type}_(\d{{6}})_v(\d+)\.parquet$")
    best = {}
    for f in parquet_files:
        m = pattern.match(f)
        if m:
            yyyymm, ver = m.group(1), int(m.group(2))
            if yyyymm not in best or ver > best[yyyymm][0]:
                best[yyyymm] = (ver, f)
    return {m: (f"{BASE_URL}/{best[m][1]}", best[m][0], friendly_name(data_type, m, best[m][0]))
            for m in sorted(best)}

emp_all = get_best_urls("employment")
sep_all = get_best_urls("separations")
acc_all = get_best_urls("accessions")

MIN_MONTH = "202412"
emp_info = {m: v for m, v in emp_all.items() if m >= MIN_MONTH}
sep_info = {m: v for m, v in sep_all.items() if m >= MIN_MONTH}
acc_info = {m: v for m, v in acc_all.items() if m >= MIN_MONTH}

print("Found data since December 2024:")
print(f"  Employment:  {len(emp_info)} months")
for m in sorted(emp_info):
    print(f"    • {emp_info[m][2]}")
print(f"  Separations: {len(sep_info)} months")
for m in sorted(sep_info):
    print(f"    • {sep_info[m][2]}")
print(f"  Accessions:  {len(acc_info)} months")
for m in sorted(acc_info):
    print(f"    • {acc_info[m][2]}")

db = duckdb.connect()

# Create VIEWS over remote parquet — nothing is downloaded yet.
# Data is fetched on demand when you click "Generate Report".
print("\nSetting up data connections (no download yet — data loads on demand)...", flush=True)
if sep_info:
    sep_list = ", ".join(f"'{v[0]}'" for v in sep_info.values())
    db.execute(f"CREATE VIEW separations AS SELECT * FROM read_parquet([{sep_list}])")
if acc_info:
    acc_list = ", ".join(f"'{v[0]}'" for v in acc_info.values())
    db.execute(f"CREATE VIEW accessions AS SELECT * FROM read_parquet([{acc_list}])")
if emp_info:
    emp_list = ", ".join(f"'{v[0]}'" for v in emp_info.values())
    db.execute(f"CREATE VIEW employment AS SELECT * FROM read_parquet([{emp_list}])")
print("  Done ✓")

# Month lists come from the file names (no download needed)
emp_month_list = sorted(emp_info.keys())
sep_month_list = sorted(sep_info.keys())
acc_month_list = sorted(acc_info.keys())

# Fetch agency list + separation categories from a single recent file (small query)
print("Fetching agency list and separation categories...", flush=True)
latest_emp_url = emp_info[emp_month_list[-1]][0]
all_agencies = sorted(db.execute(
    f"SELECT DISTINCT agency FROM read_parquet('{latest_emp_url}') WHERE agency IS NOT NULL ORDER BY agency"
).df()['agency'].tolist())

latest_sep_url = sep_info[sep_month_list[-1]][0]
sep_categories = sorted(db.execute(
    f"SELECT DISTINCT separation_category FROM read_parquet('{latest_sep_url}') WHERE separation_category IS NOT NULL ORDER BY 1"
).df()['separation_category'].tolist())
print("  Done ✓")

def month_label(m):
    return pd.to_datetime(m, format='%Y%m').strftime('%B %Y')

def make_download_link(df, filename):
    csv_str = df.to_csv(index=False)
    b64 = base64.b64encode(csv_str.encode()).decode()
    return HTML(
        f'<a href="data:file/csv;base64,{b64}" download="{filename}" '
        f'style="background:#4CAF50;color:white;padding:12px 24px;text-decoration:none;'
        f'border-radius:4px;display:inline-block;margin:10px 0;font-size:16px;">'
        f'⬇ Download {filename}</a>'
    )

# ── Unified organization picker (agencies + subagencies) ──
# Each org maps to (agency_name, subelement_name_or_None)
# When subelement is None, we select the whole agency.

# Build "all orgs" from data (query only the latest file, not all 14)
all_orgs = {}
for a in all_agencies:
    all_orgs[a] = (a, None)
all_subs = db.execute(
    f"SELECT DISTINCT agency, agency_subelement FROM read_parquet('{latest_emp_url}') "
    "WHERE agency_subelement IS NOT NULL AND agency_subelement != '' "
    "ORDER BY agency, agency_subelement"
).df()
for _, row in all_subs.iterrows():
    a, s = row['agency'], row['agency_subelement']
    if s != a:
        label = f"    ↳ {s}"
        all_orgs[label] = (a, s)

# Key organizations of interest (order matters for display)
KEY_ORG_DEFS = [
    ("DEPARTMENT OF AGRICULTURE", "DEPARTMENT OF AGRICULTURE", None),
    ("    ↳ FOOD AND NUTRITION SERVICE", "DEPARTMENT OF AGRICULTURE", "FOOD AND NUTRITION SERVICE"),
    ("    ↳ FOREST SERVICE", "DEPARTMENT OF AGRICULTURE", "FOREST SERVICE"),
    ("DEPARTMENT OF COMMERCE", "DEPARTMENT OF COMMERCE", None),
    ("DEPARTMENT OF DEFENSE", "DEPARTMENT OF DEFENSE", None),
    ("DEPARTMENT OF EDUCATION", "DEPARTMENT OF EDUCATION", None),
    ("DEPARTMENT OF ENERGY", "DEPARTMENT OF ENERGY", None),
    ("DEPARTMENT OF HEALTH AND HUMAN SERVICES", "DEPARTMENT OF HEALTH AND HUMAN SERVICES", None),
    ("    ↳ ADMINISTRATION FOR CHILDREN AND FAMILIES", "DEPARTMENT OF HEALTH AND HUMAN SERVICES", "ADMINISTRATION FOR CHILDREN AND FAMILIES"),
    ("    ↳ CENTERS FOR DISEASE CONTROL AND PREVENTION", "DEPARTMENT OF HEALTH AND HUMAN SERVICES", "CENTERS FOR DISEASE CONTROL AND PREVENTION"),
    ("    ↳ FOOD AND DRUG ADMINISTRATION", "DEPARTMENT OF HEALTH AND HUMAN SERVICES", "FOOD AND DRUG ADMINISTRATION"),
    ("    ↳ NATIONAL INSTITUTES OF HEALTH", "DEPARTMENT OF HEALTH AND HUMAN SERVICES", "NATIONAL INSTITUTES OF HEALTH"),
    ("DEPARTMENT OF HOMELAND SECURITY", "DEPARTMENT OF HOMELAND SECURITY", None),
    ("    ↳ CUSTOMS AND BORDER PROTECTION", "DEPARTMENT OF HOMELAND SECURITY", "CUSTOMS AND BORDER PROTECTION"),
    ("    ↳ FEDERAL EMERGENCY MANAGEMENT AGENCY", "DEPARTMENT OF HOMELAND SECURITY", "FEDERAL EMERGENCY MANAGEMENT AGENCY"),
    ("    ↳ IMMIGRATION AND CUSTOMS ENFORCEMENT", "DEPARTMENT OF HOMELAND SECURITY", "IMMIGRATION AND CUSTOMS ENFORCEMENT"),
    ("DEPARTMENT OF HOUSING AND URBAN DEVELOPM", "DEPARTMENT OF HOUSING AND URBAN DEVELOPM", None),
    ("DEPARTMENT OF INTERIOR", "DEPARTMENT OF INTERIOR", None),
    ("DEPARTMENT OF JUSTICE", "DEPARTMENT OF JUSTICE", None),
    ("    ↳ BUREAU OF ALCOHOL, TOBACCO, FIREARMS, AND EXPLOSIVES", "DEPARTMENT OF JUSTICE", "BUREAU OF ALCOHOL, TOBACCO, FIREARMS, AND EXPLOSIVES"),
    ("    ↳ DRUG ENFORCEMENT ADMINISTRATION", "DEPARTMENT OF JUSTICE", "DRUG ENFORCEMENT ADMINISTRATION"),
    ("    ↳ FEDERAL BUREAU OF INVESTIGATION", "DEPARTMENT OF JUSTICE", "FEDERAL BUREAU OF INVESTIGATION"),
    ("DEPARTMENT OF LABOR", "DEPARTMENT OF LABOR", None),
    ("DEPARTMENT OF STATE", "DEPARTMENT OF STATE", None),
    ("DEPARTMENT OF TRANSPORTATION", "DEPARTMENT OF TRANSPORTATION", None),
    ("DEPARTMENT OF TREASURY", "DEPARTMENT OF TREASURY", None),
    ("    ↳ INTERNAL REVENUE SERVICE", "DEPARTMENT OF TREASURY", "INTERNAL REVENUE SERVICE"),
    ("DEPARTMENT OF VETERANS AFFAIRS", "DEPARTMENT OF VETERANS AFFAIRS", None),
    ("ENVIRONMENTAL PROTECTION AGENCY", "ENVIRONMENTAL PROTECTION AGENCY", None),
    ("FEDERAL RESERVE SYSTEM", "FEDERAL RESERVE SYSTEM", None),
    ("    ↳ CONSUMER FINANCIAL PROTECTION BUREAU", "FEDERAL RESERVE SYSTEM", "CONSUMER FINANCIAL PROTECTION BUREAU"),
    ("NATIONAL LABOR RELATIONS BOARD", "NATIONAL LABOR RELATIONS BOARD", None),
    ("NATIONAL SCIENCE FOUNDATION", "NATIONAL SCIENCE FOUNDATION", None),
    ("SMALL BUSINESS ADMINISTRATION", "SMALL BUSINESS ADMINISTRATION", None),
    ("SOCIAL SECURITY ADMINISTRATION", "SOCIAL SECURITY ADMINISTRATION", None),
    ("U.S. AGENCY FOR INTERNATIONAL DEV", "U.S. AGENCY FOR INTERNATIONAL DEV", None),
]

# Validate against data (check latest file only)
key_orgs = {}
for label, agency, sub in KEY_ORG_DEFS:
    esc_a = agency.replace("'", "''")
    if sub:
        esc_s = sub.replace("'", "''")
        exists = db.execute(f"SELECT 1 FROM read_parquet('{latest_emp_url}') WHERE agency='{esc_a}' AND agency_subelement='{esc_s}' LIMIT 1").df()
    else:
        exists = db.execute(f"SELECT 1 FROM read_parquet('{latest_emp_url}') WHERE agency='{esc_a}' LIMIT 1").df()
    if len(exists) > 0:
        key_orgs[label] = (agency, sub)
    else:
        print(f"  ⚠ Not found in data: {label}")

def build_org_filter(selected_labels, org_dict):
    clauses = []
    for label in selected_labels:
        if label not in org_dict:
            continue
        agency, sub = org_dict[label]
        esc_a = agency.replace("'", "''")
        if sub:
            esc_s = sub.replace("'", "''")
            clauses.append(f"(agency = '{esc_a}' AND agency_subelement = '{esc_s}')")
        else:
            clauses.append(f"(agency = '{esc_a}')")
    return ' OR '.join(clauses) if clauses else '1=0'

def make_checkbox_group(labels, checked_labels=None, max_height='300px'):
    # Create a scrollable group of checkboxes with Check All / Uncheck All buttons
    if checked_labels is None:
        checked_labels = labels
    boxes = []
    for label in labels:
        cb = widgets.Checkbox(
            value=(label in checked_labels),
            description=label,
            indent=False,
            layout=widgets.Layout(width='auto')
        )
        boxes.append(cb)
    btn_all = widgets.Button(description='Check All', layout=widgets.Layout(width='100px', height='28px'))
    btn_none = widgets.Button(description='Uncheck All', layout=widgets.Layout(width='100px', height='28px'))
    def _check_all(_):
        for cb in boxes:
            cb.value = True
    def _uncheck_all(_):
        for cb in boxes:
            cb.value = False
    btn_all.on_click(_check_all)
    btn_none.on_click(_uncheck_all)
    scroll_box = widgets.VBox(
        boxes,
        layout=widgets.Layout(
            max_height=max_height,
            overflow_y='auto',
            border='1px solid #ddd',
            padding='5px'
        )
    )
    container = widgets.VBox([
        widgets.HBox([btn_all, btn_none]),
        scroll_box
    ])
    return container, boxes

def get_checked(boxes):
    # Return list of labels for checked boxes
    return [cb.description for cb in boxes if cb.value]

def make_org_picker(prefix, max_height='300px'):
    toggle = widgets.ToggleButtons(
        options=['Key Organizations', 'All Organizations'],
        value='Key Organizations',
        style={'description_width': '50px'}
    )
    container, boxes = make_checkbox_group(list(key_orgs.keys()), max_height=max_height)
    container._boxes = boxes

    def _update(change):
        if change['new'] == 'Key Organizations':
            new_container, new_boxes = make_checkbox_group(
                list(key_orgs.keys()), max_height=max_height
            )
        else:
            new_container, new_boxes = make_checkbox_group(
                list(all_orgs.keys()),
                checked_labels=list(key_orgs.keys()),
                max_height=max_height
            )
        container.children = new_container.children
        container._boxes = new_boxes
    toggle.observe(_update, names='value')
    return toggle, container

print(f"\nReady ✓")
print(f"  {len(all_agencies)} agencies, {len(key_orgs)} key organizations pre-selected")
print(f"  Employment: {month_label(emp_month_list[0])} – {month_label(emp_month_list[-1])}")
print(f"  Separations: {month_label(sep_month_list[0])} – {month_label(sep_month_list[-1])}")
print(f"  Accessions: {month_label(acc_month_list[0])} – {month_label(acc_month_list[-1])}")
""", title="Step 2: Connect to data on HuggingFace (~30 seconds)")


# ── Cell 3: Workforce comparison ───────────────────────────────────────────
md("""---
## Tool 1: Workforce Size Comparison

Compare total federal employees between two months.""")

code(r"""
BREAKDOWN_OPTIONS = {
    'Agency Subelement (subagency)': 'agency_subelement',
    'Grade': 'grade',
    'Pay Plan (GS, SES, etc.)': 'pay_plan',
    'Length of Service (years)': 'length_of_service_years',
    'Duty Station State': 'duty_station_state',
    'Occupational Category': 'occupational_category',
    'Occupational Group': 'occupational_group',
    'Supervisory Status': 'supervisory_status',
    'Education Level': 'education_level',
    'Work Schedule': 'work_schedule',
    'Appointment Type': 'appointment_type',
    'Bargaining Unit Status': 'bargaining_unit_status',
    'STEM Occupation': 'stem_occupation',
    'Veteran Indicator': 'veteran_indicator',
}

w1_baseline = widgets.Dropdown(
    options=[(month_label(m), m) for m in emp_month_list],
    value='202501' if '202501' in emp_month_list else emp_month_list[0],
    description='Baseline:',
    style={'description_width': '100px'},
    layout=widgets.Layout(width='300px')
)
w1_comparison = widgets.Dropdown(
    options=[(month_label(m), m) for m in emp_month_list],
    value=emp_month_list[-1],
    description='Compare to:',
    style={'description_width': '100px'},
    layout=widgets.Layout(width='300px')
)

w1_toggle, w1_picker = make_org_picker('w1')

w1_bd_container, w1_bd_boxes = make_checkbox_group(
    list(BREAKDOWN_OPTIONS.keys()), checked_labels=[], max_height='200px'
)
w1_button = widgets.Button(
    description='  Generate Report', button_style='primary', icon='table',
    layout=widgets.Layout(width='220px', height='40px')
)
w1_output = widgets.Output()

def run_comparison(_):
    with w1_output:
        clear_output(wait=True)
        selected = get_checked(w1_picker._boxes)
        if not selected:
            print("⚠ Check at least one organization.")
            return
        baseline = w1_baseline.value
        comparison = w1_comparison.value
        if baseline == comparison:
            print("⚠ Baseline and comparison months must be different.")
            return

        current_orgs = key_orgs if w1_toggle.value == 'Key Organizations' else all_orgs
        org_filter = build_org_filter(selected, current_orgs)

        breakdown_cols = [BREAKDOWN_OPTIONS[k] for k in get_checked(w1_bd_boxes)]
        group_cols = ['agency', 'agency_subelement'] + breakdown_cols
        group_sql = ', '.join(group_cols)

        print(f"Comparing {month_label(baseline)} vs {month_label(comparison)} for {len(selected)} organizations...")

        q = f'''
            SELECT snapshot_yyyymm AS month, {group_sql},
                   SUM(CAST(count AS INTEGER)) AS employees
            FROM employment
            WHERE ({org_filter})
              AND snapshot_yyyymm IN ('{baseline}', '{comparison}')
            GROUP BY snapshot_yyyymm, {group_sql}
        '''
        df = db.execute(q).df()

        base_col = month_label(baseline)
        comp_col = month_label(comparison)
        base_df = df[df['month'] == baseline].drop(columns=['month']).rename(columns={'employees': base_col})
        comp_df = df[df['month'] == comparison].drop(columns=['month']).rename(columns={'employees': comp_col})

        result = base_df.merge(comp_df, on=group_cols, how='outer').fillna(0)
        result[base_col] = result[base_col].astype(int)
        result[comp_col] = result[comp_col].astype(int)
        result['Change'] = result[comp_col] - result[base_col]
        result['% Change'] = (result['Change'] / result[base_col].replace(0, float('nan')) * 100).round(1)
        result = result.sort_values('Change')

        rename = {c: c.replace('_', ' ').title() for c in group_cols}
        rename['agency'] = 'Agency'
        rename['agency_subelement'] = 'Subagency'
        result = result.rename(columns=rename)

        print(f"✓ {len(result)} rows\n")
        display(result.style.format({base_col: '{:,.0f}', comp_col: '{:,.0f}', 'Change': '{:+,.0f}', '% Change': '{:+.1f}%'}).hide(axis='index'))

        fname = f"workforce_comparison_{baseline}_vs_{comparison}.csv"
        display(make_download_link(result, fname))

w1_button.on_click(run_comparison)

display(widgets.VBox([
    widgets.HTML('<h3>Pick two months to compare:</h3>'),
    widgets.HBox([w1_baseline, w1_comparison]),
    widgets.HTML('<h3>Select organizations:</h3>'),
    w1_toggle,
    w1_picker,
    widgets.HTML('<h3>Optional: add breakdown dimensions</h3><p style="color:#666">Check any boxes to add columns to the report.</p>'),
    w1_bd_container,
    widgets.HTML('<br>'),
    w1_button,
    w1_output,
]))
""", title="Tool 1: Workforce Size Comparison")


# ── Cell 4: Separations analysis ──────────────────────────────────────────
md("""---
## Tool 2: Separations Analysis (RIF, Quit, Retirement, DRP, etc.)

See how many federal employees left, broken down by **separation mechanism**.

| Category | What it means |
|----------|--------------|
| Reduction in Force (RIF) | Involuntary layoff |
| Quit | Voluntary resignation |
| Retirement (Voluntary / Early Out / Other) | Includes VERA/VSIP buyouts |
| Termination | Expired appointments, probationary terms, etc. |
| Transfer Out | Moved to another agency |

**DRP flag:** The Deferred Resignation Program is tracked as a separate Yes/No indicator — check the box below to include it as a column.""")

code(r"""
SEP_BREAKDOWN_OPTIONS = {
    'Agency Subelement (subagency)': 'agency_subelement',
    'Grade': 'grade',
    'Pay Plan (GS, SES, etc.)': 'pay_plan',
    'Length of Service (years)': 'length_of_service_years',
    'Duty Station State': 'duty_station_state',
    'Occupational Category': 'occupational_category',
    'Occupational Group': 'occupational_group',
    'Supervisory Status': 'supervisory_status',
    'Education Level': 'education_level',
    'Age Bracket': 'age_bracket',
}

w2_months_container, w2_months_boxes = make_checkbox_group(
    [month_label(m) for m in sep_month_list], max_height='150px'
)
# Store the raw month values for lookup
_sep_month_map = {month_label(m): m for m in sep_month_list}

w2_toggle, w2_picker = make_org_picker('w2')

w2_cat_container, w2_cat_boxes = make_checkbox_group(sep_categories, max_height='160px')
w2_drp = widgets.Checkbox(value=True, description='Include DRP (Deferred Resignation) flag as a column', indent=False)
w2_bd_container, w2_bd_boxes = make_checkbox_group(
    list(SEP_BREAKDOWN_OPTIONS.keys()), checked_labels=[], max_height='160px'
)
w2_button = widgets.Button(
    description='  Generate Report', button_style='primary', icon='table',
    layout=widgets.Layout(width='220px', height='40px')
)
w2_output = widgets.Output()

def run_separations(_):
    with w2_output:
        clear_output(wait=True)
        selected = get_checked(w2_picker._boxes)
        month_labels = get_checked(w2_months_boxes)
        months = [_sep_month_map[ml] for ml in month_labels]
        categories = get_checked(w2_cat_boxes)
        if not selected or not months or not categories:
            print("⚠ Check at least one organization, month, and separation category.")
            return

        current_orgs = key_orgs if w2_toggle.value == 'Key Organizations' else all_orgs
        org_filter = build_org_filter(selected, current_orgs)

        breakdown_cols = [SEP_BREAKDOWN_OPTIONS[k] for k in get_checked(w2_bd_boxes)]
        group_cols = ['personnel_action_effective_date_yyyymm', 'agency', 'agency_subelement', 'separation_category']
        if w2_drp.value:
            group_cols.append('drp_indicator')
        group_cols += breakdown_cols

        group_sql = ', '.join(group_cols)
        month_sql = ', '.join(f"'{m}'" for m in months)
        cat_sql = ', '.join(f"'{c}'" for c in categories)

        print(f"Querying separations for {len(selected)} organizations, {len(months)} months...")

        q = f'''
            SELECT {group_sql},
                   SUM(CAST(count AS INTEGER)) AS separations
            FROM separations
            WHERE ({org_filter})
              AND personnel_action_effective_date_yyyymm IN ({month_sql})
              AND separation_category IN ({cat_sql})
            GROUP BY {group_sql}
            ORDER BY agency, agency_subelement, personnel_action_effective_date_yyyymm, separation_category
        '''
        result = db.execute(q).df()

        rename = {
            'personnel_action_effective_date_yyyymm': 'Month',
            'agency': 'Agency',
            'agency_subelement': 'Subagency',
            'separation_category': 'Separation Category',
            'drp_indicator': 'DRP (Deferred Resignation)',
        }
        for c in breakdown_cols:
            rename[c] = c.replace('_', ' ').title()
        result = result.rename(columns=rename)
        result['Month'] = result['Month'].apply(month_label)
        if 'DRP (Deferred Resignation)' in result.columns:
            result['DRP (Deferred Resignation)'] = result['DRP (Deferred Resignation)'].map({'Y': 'Yes', 'N': 'No'})

        print(f"✓ {len(result)} rows\n")
        display(result.style.format({'separations': '{:,.0f}'}).hide(axis='index'))

        month_str = f"{months[0]}_{months[-1]}" if len(months) > 1 else months[0]
        display(make_download_link(result, f"separations_{month_str}.csv"))

w2_button.on_click(run_separations)

display(widgets.VBox([
    widgets.HTML('<h3>Select months:</h3>'),
    w2_months_container,
    widgets.HTML('<h3>Select organizations:</h3>'),
    w2_toggle,
    w2_picker,
    widgets.HTML('<h3>Separation mechanisms:</h3><p style="color:#666">All checked by default. Uncheck to exclude.</p>'),
    w2_cat_container,
    w2_drp,
    widgets.HTML('<h3>Optional: add breakdown dimensions</h3>'),
    w2_bd_container,
    widgets.HTML('<br>'),
    w2_button,
    w2_output,
]))
""", title="Tool 2: Separations Analysis")


# ── Cell 5: Accessions analysis ───────────────────────────────────────────
md("""---
## Tool 3: Accessions (New Hires) Analysis

See how many new federal employees were hired, by agency and category.""")

code(r"""
ACC_BREAKDOWN_OPTIONS = {
    'Agency Subelement (subagency)': 'agency_subelement',
    'Accession Category': 'accession_category',
    'Grade': 'grade',
    'Pay Plan (GS, SES, etc.)': 'pay_plan',
    'Length of Service (years)': 'length_of_service_years',
    'Duty Station State': 'duty_station_state',
    'Occupational Category': 'occupational_category',
    'Occupational Group': 'occupational_group',
    'Education Level': 'education_level',
}

w3_months_container, w3_months_boxes = make_checkbox_group(
    [month_label(m) for m in acc_month_list], max_height='150px'
)
_acc_month_map = {month_label(m): m for m in acc_month_list}

w3_toggle, w3_picker = make_org_picker('w3')

w3_bd_container, w3_bd_boxes = make_checkbox_group(
    list(ACC_BREAKDOWN_OPTIONS.keys()), checked_labels=[], max_height='160px'
)
w3_button = widgets.Button(
    description='  Generate Report', button_style='primary', icon='table',
    layout=widgets.Layout(width='220px', height='40px')
)
w3_output = widgets.Output()

def run_accessions(_):
    with w3_output:
        clear_output(wait=True)
        selected = get_checked(w3_picker._boxes)
        month_labels = get_checked(w3_months_boxes)
        months = [_acc_month_map[ml] for ml in month_labels]
        if not selected or not months:
            print("⚠ Check at least one organization and month.")
            return

        current_orgs = key_orgs if w3_toggle.value == 'Key Organizations' else all_orgs
        org_filter = build_org_filter(selected, current_orgs)

        breakdown_cols = [ACC_BREAKDOWN_OPTIONS[k] for k in get_checked(w3_bd_boxes)]
        group_cols = ['personnel_action_effective_date_yyyymm', 'agency', 'agency_subelement'] + breakdown_cols
        group_sql = ', '.join(group_cols)
        month_sql = ', '.join(f"'{m}'" for m in months)

        print(f"Querying accessions for {len(selected)} organizations, {len(months)} months...")

        q = f'''
            SELECT {group_sql},
                   SUM(CAST(count AS INTEGER)) AS accessions
            FROM accessions
            WHERE ({org_filter})
              AND personnel_action_effective_date_yyyymm IN ({month_sql})
            GROUP BY {group_sql}
            ORDER BY agency, agency_subelement, personnel_action_effective_date_yyyymm
        '''
        result = db.execute(q).df()

        rename = {
            'personnel_action_effective_date_yyyymm': 'Month',
            'agency': 'Agency',
            'agency_subelement': 'Subagency',
        }
        for c in breakdown_cols:
            rename[c] = c.replace('_', ' ').title()
        result = result.rename(columns=rename)
        result['Month'] = result['Month'].apply(month_label)

        print(f"✓ {len(result)} rows\n")
        display(result.style.format({'accessions': '{:,.0f}'}).hide(axis='index'))

        month_str = f"{months[0]}_{months[-1]}" if len(months) > 1 else months[0]
        display(make_download_link(result, f"accessions_{month_str}.csv"))

w3_button.on_click(run_accessions)

display(widgets.VBox([
    widgets.HTML('<h3>Select months:</h3>'),
    w3_months_container,
    widgets.HTML('<h3>Select organizations:</h3>'),
    w3_toggle,
    w3_picker,
    widgets.HTML('<h3>Optional: add breakdown dimensions</h3>'),
    w3_bd_container,
    widgets.HTML('<br>'),
    w3_button,
    w3_output,
]))
""", title="Tool 3: Accessions (New Hires)")


# ── Cell 6: Monthly employment trends ─────────────────────────────────────
md("""---
## Tool 4: Monthly Employment Trends

Track total headcount **month by month** since December 2024. The "Change from Baseline" shows the difference from the earliest available month.""")

code(r"""
TREND_BREAKDOWN_OPTIONS = {
    'Agency Subelement (subagency)': 'agency_subelement',
    'Grade': 'grade',
    'Pay Plan (GS, SES, etc.)': 'pay_plan',
    'Length of Service (years)': 'length_of_service_years',
    'Duty Station State': 'duty_station_state',
    'Occupational Category': 'occupational_category',
    'Supervisory Status': 'supervisory_status',
}

w4_toggle, w4_picker = make_org_picker('w4')

w4_bd_container, w4_bd_boxes = make_checkbox_group(
    list(TREND_BREAKDOWN_OPTIONS.keys()), checked_labels=[], max_height='140px'
)
w4_button = widgets.Button(
    description='  Generate Report', button_style='primary', icon='table',
    layout=widgets.Layout(width='220px', height='40px')
)
w4_output = widgets.Output()

def run_trends(_):
    with w4_output:
        clear_output(wait=True)
        selected = get_checked(w4_picker._boxes)
        if not selected:
            print("⚠ Check at least one organization.")
            return

        current_orgs = key_orgs if w4_toggle.value == 'Key Organizations' else all_orgs
        org_filter = build_org_filter(selected, current_orgs)

        breakdown_cols = [TREND_BREAKDOWN_OPTIONS[k] for k in get_checked(w4_bd_boxes)]
        group_cols = ['snapshot_yyyymm', 'agency', 'agency_subelement'] + breakdown_cols
        group_sql = ', '.join(group_cols)

        print(f"Querying monthly trends for {len(selected)} organizations...")

        q = f'''
            SELECT {group_sql},
                   SUM(CAST(count AS INTEGER)) AS employees
            FROM employment
            WHERE ({org_filter})
            GROUP BY {group_sql}
            ORDER BY agency, agency_subelement, snapshot_yyyymm
        '''
        result = db.execute(q).df()

        rename = {
            'snapshot_yyyymm': 'Month',
            'agency': 'Agency',
            'agency_subelement': 'Subagency',
        }
        for c in breakdown_cols:
            rename[c] = c.replace('_', ' ').title()
        result = result.rename(columns=rename)

        id_cols = ['Agency', 'Subagency'] + [rename.get(c, c) for c in breakdown_cols]
        first_vals = result.groupby(id_cols)['employees'].first().rename('baseline')
        result = result.merge(first_vals, on=id_cols)
        result['Change from Baseline'] = result['employees'] - result['baseline']
        result['% Change from Baseline'] = (result['Change from Baseline'] / result['baseline'].replace(0, float('nan')) * 100).round(1)
        result = result.drop(columns=['baseline'])

        result['Month'] = result['Month'].apply(month_label)

        print(f"✓ {len(result)} rows\n")
        fmt = {'employees': '{:,.0f}', 'Change from Baseline': '{:+,.0f}', '% Change from Baseline': '{:+.1f}%'}
        display(result.style.format(fmt).hide(axis='index'))

        display(make_download_link(result, "monthly_employment_trends.csv"))

w4_button.on_click(run_trends)

display(widgets.VBox([
    widgets.HTML('<h3>Select organizations:</h3>'),
    w4_toggle,
    w4_picker,
    widgets.HTML('<h3>Optional: add breakdown dimensions</h3>'),
    w4_bd_container,
    widgets.HTML('<br>'),
    w4_button,
    w4_output,
]))
""", title="Tool 4: Monthly Employment Trends")


# ── Cell 7: Data dictionary ───────────────────────────────────────────────
md("""---
## Reference: Data Notes

**Data source:** [OPM EHRI data](https://data.opm.gov/explore-data/data/data-downloads), republished to [HuggingFace](https://huggingface.co/datasets/impactproject/opm-ehri-data) for easy access.

**Employment data** is a monthly snapshot — total headcount on the last day of each month. Compare two snapshots to see net change.

**Separations data** shows who left federal service and why. Key separation categories:
| Category | What it means |
|----------|--------------|
| Reduction in Force (RIF) | Involuntary layoff |
| Quit | Voluntary resignation (check DRP flag for Deferred Resignation Program) |
| Retirement - Voluntary | Standard retirement |
| Retirement - Early Out | VERA/VSIP early retirement (may include buyouts) |
| Retirement - Other | Other retirement types |
| Termination | Expired appointments, probationary terminations, etc. |
| Transfer Out | Moved to another agency (not a true loss to federal workforce) |

**DRP (Deferred Resignation Program):** A separate Yes/No indicator in the separations data. When someone accepted the DRP offer, this flag is "Yes" regardless of their separation category.

**Buyouts (VSIP):** OPM data does not have a separate "buyout" category. Employees who took a Voluntary Separation Incentive Payment typically appear under "Retirement - Early Out" or "Quit."

**Accessions data** shows new hires entering federal service each month.

**Agencies vs. subagencies:** Some organizations in the selector are subagencies nested under a parent agency. For example:
- **CFPB** is a subagency of Federal Reserve System
- **FBI**, **DEA**, **ATF** are subagencies of Department of Justice
- **ICE**, **CBP**, **FEMA** are subagencies of Department of Homeland Security
- **IRS** is a subagency of Department of Treasury
- **NIH**, **CDC**, **FDA**, **ACF** are subagencies of Department of Health and Human Services
- **Forest Service**, **Food and Nutrition Service** are subagencies of Department of Agriculture
- **USAID** appears as "U.S. Agency for International Dev" (name truncated in OPM data)
- **HUD** appears as "Department of Housing and Urban Developm" (name truncated in OPM data)

Selecting a parent agency gives you the *entire* agency including all subagencies. Selecting a specific subagency gives you *only* that component.""")


# ── Assemble notebook ──────────────────────────────────────────────────────
nb = {
    "nbformat": 4,
    "nbformat_minor": 5,
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3"
        },
        "language_info": {
            "name": "python",
            "version": "3.10.0"
        },
        "colab": {
            "provenance": [],
            "name": "CSS Federal Workforce Data Tool"
        }
    },
    "cells": cells
}

# Fix: remove trailing newline from last line of each cell source
for cell in nb["cells"]:
    if cell["source"] and cell["source"][-1].endswith("\n"):
        cell["source"][-1] = cell["source"][-1].rstrip("\n")

with open("css_workforce_tool.ipynb", "w") as f:
    json.dump(nb, f, indent=1)

print("Generated css_workforce_tool.ipynb")
