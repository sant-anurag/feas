# settings/views.py
import re
import json
import datetime
from typing import List, Tuple, Dict, Any, Optional

from django.shortcuts import render, redirect
from django.contrib import messages
from django.urls import reverse
from django.db import connection
from django.views.decorators.http import require_http_methods
from django.http import HttpResponseBadRequest, JsonResponse
from django.views.decorators.http import require_GET, require_POST

import pandas as pd

# ---------- Configuration ----------
MASTER_TABLE = "prism_master_wor"
META_TABLE = "prism_master_wor_meta"
IMPORT_HISTORY = "import_history"
BATCH_SIZE = 500
DROP_IF_EXISTS = True

# reserved internal names we won't allow as sanitized columns
RESERVED_COLS = {"id", "created_at"}


# ---------- Helpers ----------
def _sanitize_column(name: str, used: set, idx: int) -> str:
    """Return a safe DB column name derived from header name."""
    name = "" if name is None else str(name)
    base = re.sub(r'[^0-9a-zA-Z]+', '_', name.strip()).strip('_').lower()
    if not base:
        base = f"col_{idx}"
    if re.match(r'^\d', base):
        base = f"c_{base}"
    out = base
    i = 1
    while out in used or out in RESERVED_COLS:
        out = f"{base}_{i}"
        i += 1
    used.add(out)
    return out


def _param_safe(v: Any) -> Any:
    """
    Convert python value to DB-friendly native types:
    - None stays None
    - pandas NA -> None
    - datetime -> formatted string
    - numeric-like -> numeric
    - otherwise str(v)
    """
    try:
        import pandas as _pd
        if v is _pd.NA:
            return None
    except Exception:
        pass

    if v is None:
        return None

    # pandas NaN
    try:
        if isinstance(v, float) and (v != v):  # NaN check
            return None
    except Exception:
        pass

    # datetime/date
    try:
        if isinstance(v, (datetime.datetime, datetime.date)):
            if isinstance(v, datetime.datetime):
                return v.strftime("%Y-%m-%d %H:%M:%S")
            else:
                return v.strftime("%Y-%m-%d")
    except Exception:
        pass

    # pandas Timestamp
    try:
        import pandas as _pd
        if isinstance(v, _pd.Timestamp):
            dt = v.to_pydatetime()
            return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass

    # numeric types -> return as-is for driver (int/float)
    try:
        if isinstance(v, (int, float)):
            return v
    except Exception:
        pass

    # fallback: string
    return str(v)


# ---------- Ensure meta & history tables ----------
def _ensure_meta_table(cursor):
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS `{META_TABLE}` (
            `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
            `table_name` VARCHAR(128) NOT NULL,
            `col_order` INT NOT NULL,
            `col_name` VARCHAR(255) NOT NULL,
            `orig_header` VARCHAR(1024),
            `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY `uq_prism_master_meta` (`table_name`,`col_name`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)


def _ensure_import_history_table(cursor):
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS `{IMPORT_HISTORY}` (
            `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
            `imported_by` VARCHAR(255),
            `filename` VARCHAR(512),
            `started_at` DATETIME,
            `finished_at` DATETIME,
            `total_rows` INT,
            `master_inserted` INT,
            `master_failed` INT,
            `projects_created` INT,
            `wbs_inserted` INT,
            `wbs_failed` INT,
            `errors` LONGTEXT,
            `meta_map` LONGTEXT,
            `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)


# ---------- Create application tables ----------
def _create_projects_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS `projects` (
        `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
        `name` VARCHAR(255) NOT NULL,
        `oem_name` VARCHAR(255),
        `pdl_user_id` VARCHAR(255),
        `pdl_name` VARCHAR(255),
        `pm_user_id` VARCHAR(255),
        `pm_name` VARCHAR(255),
        `start_date` DATE,
        `end_date` DATE,
        `description` TEXT,
        `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY `uq_project_name` (`name`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)


def _create_project_contacts_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS `project_contacts` (
        `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
        `project_id` BIGINT NOT NULL,
        `contact_type` VARCHAR(16) NOT NULL,
        `contact_name` VARCHAR(512),
        `user_id` BIGINT NULL,
        `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY `uq_proj_contact` (`project_id`,`contact_type`,`contact_name`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)


def _create_prism_wbs_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS `prism_wbs` (
        `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
        `iom_id` VARCHAR(255) NOT NULL,
        `status` VARCHAR(64),
        `project_id` BIGINT,
        `bg_code` VARCHAR(128),
        `year` VARCHAR(16),
        `seller_country` VARCHAR(128),
        `creator` VARCHAR(255),
        `date_created` DATETIME,
        `comment_of_creator` TEXT,
        `buyer_bau` VARCHAR(255),
        `buyer_wbs_cc` VARCHAR(255),
        `seller_bau` VARCHAR(255),
        `seller_wbs_cc` VARCHAR(255),
        `site` VARCHAR(255),
        `function` VARCHAR(255),
        `department` VARCHAR(255),
        `jan_hours` DECIMAL(14,2) DEFAULT 0,
        `feb_hours` DECIMAL(14,2) DEFAULT 0,
        `mar_hours` DECIMAL(14,2) DEFAULT 0,
        `apr_hours` DECIMAL(14,2) DEFAULT 0,
        `may_hours` DECIMAL(14,2) DEFAULT 0,
        `jun_hours` DECIMAL(14,2) DEFAULT 0,
        `jul_hours` DECIMAL(14,2) DEFAULT 0,
        `aug_hours` DECIMAL(14,2) DEFAULT 0,
        `sep_hours` DECIMAL(14,2) DEFAULT 0,
        `oct_hours` DECIMAL(14,2) DEFAULT 0,
        `nov_hours` DECIMAL(14,2) DEFAULT 0,
        `dec_hours` DECIMAL(14,2) DEFAULT 0,
        `total_hours` DECIMAL(16,2) DEFAULT 0,
        `jan_fte` DECIMAL(10,4) DEFAULT 0,
        `feb_fte` DECIMAL(10,4) DEFAULT 0,
        `mar_fte` DECIMAL(10,4) DEFAULT 0,
        `apr_fte` DECIMAL(10,4) DEFAULT 0,
        `may_fte` DECIMAL(10,4) DEFAULT 0,
        `jun_fte` DECIMAL(10,4) DEFAULT 0,
        `jul_fte` DECIMAL(10,4) DEFAULT 0,
        `aug_fte` DECIMAL(10,4) DEFAULT 0,
        `sep_fte` DECIMAL(10,4) DEFAULT 0,
        `oct_fte` DECIMAL(10,4) DEFAULT 0,
        `nov_fte` DECIMAL(10,4) DEFAULT 0,
        `dec_fte` DECIMAL(10,4) DEFAULT 0,
        `total_fte` DECIMAL(16,4) DEFAULT 0,
        `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY `uq_prism_wbs_iom` (`iom_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)


# ---------- Main import view ----------
@require_http_methods(["GET", "POST"])
def import_master(request):
    if request.method == "GET":
        return render(request, "settings/import_master.html", {"preview_rows": None})

    if "reset" in request.POST:
        messages.info(request, "Import reset.")
        return redirect(reverse("settings:import_master"))

    uploaded_file = request.FILES.get("file")
    if not uploaded_file:
        messages.error(request, "No file uploaded.")
        return redirect(reverse("settings:import_master"))

    started_at = datetime.datetime.now()
    importer = getattr(request.user, "username", None) or "anonymous"
    filename = getattr(uploaded_file, "name", "uploaded.xlsx")

    # Read first sheet into pandas
    try:
        xls = pd.ExcelFile(uploaded_file)
        sheet_name = xls.sheet_names[0]
        df = pd.read_excel(xls, sheet_name=sheet_name, dtype=object)
    except Exception as e:
        messages.error(request, f"Failed to read Excel first sheet: {e}")
        return redirect(reverse("settings:import_master"))

    if df.shape[0] == 0:
        messages.error(request, "Uploaded sheet is empty.")
        return redirect(reverse("settings:import_master"))

    orig_headers = list(df.columns)
    used = set()
    mapping: List[Tuple[str, str]] = []
    for i, h in enumerate(orig_headers):
        col = _sanitize_column(h, used, i)
        mapping.append((h, col))
    sanitized_cols = [col for (_orig, col) in mapping]

    # Step A: create master table (all TEXT) and persist mapping
    ddl_warnings: List[str] = []
    master_inserted = 0
    master_failed = 0
    try:
        with connection.cursor() as cursor:
            _ensure_meta_table(cursor)
            _ensure_import_history_table(cursor)

            if DROP_IF_EXISTS:
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS `{MASTER_TABLE}`;")
                except Exception as e:
                    ddl_warnings.append(f"DROP master table warning: {e}")

            cols_def = ",\n  ".join([f"`{c}` TEXT NULL" for _, c in mapping])
            create_sql = f"""
                CREATE TABLE `{MASTER_TABLE}` (
                    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    {cols_def}
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
            cursor.execute(create_sql)

            # Save mapping
            cursor.execute(f"DELETE FROM `{META_TABLE}` WHERE table_name = %s", [MASTER_TABLE])
            for ord_idx, (orig, col) in enumerate(mapping, start=1):
                cursor.execute(
                    f"INSERT INTO `{META_TABLE}` (table_name, col_order, col_name, orig_header) VALUES (%s,%s,%s,%s)",
                    [MASTER_TABLE, ord_idx, col, str(orig)]
                )

            # Insert rows in batches
            cols_clause = ", ".join([f"`{c}`" for c in sanitized_cols])
            placeholders = ", ".join(["%s"] * len(sanitized_cols))
            insert_sql = f"INSERT INTO `{MASTER_TABLE}` ({cols_clause}) VALUES ({placeholders})"
            rows_values = []
            for _, row in df.iterrows():
                vals = []
                for orig, col in mapping:
                    raw = row.get(orig, None)
                    safe_val = _param_safe(raw)
                    vals.append(safe_val)
                rows_values.append(tuple(vals))

            # batch insert
            for i in range(0, len(rows_values), BATCH_SIZE):
                batch = rows_values[i:i + BATCH_SIZE]
                try:
                    cursor.executemany(insert_sql, batch)
                    master_inserted += len(batch)
                except Exception:
                    # fallback to row-by-row to capture faults precisely
                    for j, r in enumerate(batch):
                        try:
                            cursor.execute(insert_sql, r)
                            master_inserted += 1
                        except Exception as e:
                            master_failed += 1
                            messages.warning(request, f"Master insert row {i + j + 1} failed: {e}")
    except Exception as e:
        messages.error(request, f"Failed to create/populate master table: {e}")
        return redirect(reverse("settings:import_master"))

    messages.success(request, f"Master import: {master_inserted} rows inserted, {master_failed} failed.")
    for w in ddl_warnings:
        messages.warning(request, w)

    # Step B: create application tables
    try:
        with connection.cursor() as cursor:
            _create_projects_table(cursor)
            _create_project_contacts_table(cursor)
            _create_prism_wbs_table(cursor)
    except Exception as e:
        messages.error(request, f"Failed to create application tables: {e}")
        return redirect(reverse("settings:import_master"))

    # Step C: populate projects and prism_wbs (kept same as previously)
    mapping_lookup: Dict[str, str] = {}
    col_to_orig = {}
    for orig, col in mapping:
        if orig is None:
            continue
        mapping_lookup[str(orig).strip().lower()] = col
        col_to_orig[col] = orig

    def find_col_by_variants(variants: List[str]) -> Optional[str]:
        for v in variants:
            c = mapping_lookup.get(v.strip().lower())
            if c:
                return c
        return None

    prog_col = find_col_by_variants(["Program", "program", "Program "])
    buyer_oem_col = find_col_by_variants(["Buyer OEM", "Buyer_OEM", "BuyerOEM"])
    id_col = find_col_by_variants(["ID", "Id", "id"])
    buyer_wbs_col = find_col_by_variants(["Buyer WBS/CC", "Buyer WBS", "Buyer_WBS_CC"])
    seller_wbs_col = find_col_by_variants(["Seller WBS/CC", "Seller WBS", "Seller_WBS_CC"])
    total_hours_col = find_col_by_variants(["Total Hours", "TotalHours"])
    total_fte_col = find_col_by_variants(["Total FTE", "TotalFTE"])

    projects_created = 0
    wbs_inserted = 0
    wbs_failed = 0
    errors: List[str] = []

    try:
        with connection.cursor() as cursor:
            # Build a local cache of existing projects for faster lookup
            cursor.execute("SELECT id, name FROM projects")
            existing_projects = {row[1]: row[0] for row in cursor.fetchall()}

            # Populate projects (unique by Program)
            if prog_col:
                programs: Dict[str, Dict[str, Optional[str]]] = {}
                for _, row in df.iterrows():
                    orig_prog = col_to_orig.get(prog_col)
                    prog_val = row.get(orig_prog) if orig_prog else None
                    if prog_val is None or (isinstance(prog_val, float) and pd.isna(prog_val)):
                        for orig_h, dbc in mapping:
                            if "program" in str(orig_h).strip().lower():
                                prog_val = row.get(orig_h)
                                break
                    if prog_val is None:
                        continue
                    prog_name = str(prog_val).strip()
                    if prog_name == "":
                        continue
                    oem_val = None
                    if buyer_oem_col:
                        orig_oem = col_to_orig.get(buyer_oem_col)
                        if orig_oem:
                            v = row.get(orig_oem)
                            if v is not None and not (isinstance(v, float) and pd.isna(v)):
                                oem_val = str(v).strip()
                    if prog_name not in programs:
                        programs[prog_name] = {"oem_name": oem_val}
                    else:
                        if programs[prog_name].get("oem_name") in (None, "") and oem_val:
                            programs[prog_name]["oem_name"] = oem_val

                for p in sorted(programs.keys()):
                    if p == "":
                        continue
                    if p in existing_projects:
                        continue
                    try:
                        oem_for_p = programs[p].get("oem_name")
                        cursor.execute("INSERT INTO projects (name, oem_name) VALUES (%s,%s)", [p, oem_for_p])
                        cursor.execute("SELECT id FROM projects WHERE name=%s LIMIT 1", [p])
                        res = cursor.fetchone()
                        if res:
                            existing_projects[p] = res[0]
                        projects_created += 1
                    except Exception as e:
                        errors.append(f"Failed to insert project '{p}': {e}")

            # Populate prism_wbs using ON DUPLICATE KEY UPDATE
            def _find(orig_variants):
                return find_col_by_variants(orig_variants)

            status_col = _find(["Status", "status", "Request Status"])
            bg_code_col = _find(["BG Code", "BG_Code", "bg_code", "bg code"])
            year_col = _find(["Year", "year"])
            seller_country_col = _find(["Seller Country", "seller_country", "seller country", "Country"])
            creator_col = _find(["Creator", "creator", "Requesting Manager", "Requested By"])
            date_created_col = _find(["Date Created", "date_created", "datecreated", "Created At"])
            comment_col = _find(["Comment of Creator", "comment_of_creator", "Comment", "Comments", "comment"])
            buyer_bau_col = _find(["Buyer BAU", "buyer_bau", "Buyer_BAU"])
            seller_bau_col = _find(["Seller BAU", "seller_bau", "Seller_BAU"])
            site_col = _find(["Site", "site", "Location"])
            function_col = _find(["Function", "function"])
            department_col = _find(["Department", "department"])
            buyer_wbs_col = buyer_wbs_col or _find(["Buyer WBS/CC", "Buyer WBS", "Buyer_WBS_CC", "Buyer_WBS"])
            seller_wbs_col = seller_wbs_col or _find(["Seller WBS/CC", "Seller WBS", "Seller_WBS_CC", "Seller_WBS"])

            months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
            month_hours_cols = {}
            month_fte_cols = {}
            for m in months:
                month_hours_cols[m] = _find(
                    [f"{m}_hours", f"{m.title()} Hours", f"{m.title()}_Hours", f"{m}", f"{m.upper()}"])
                month_fte_cols[m] = _find([f"{m}_fte", f"{m.title()} FTE", f"{m.title()}_FTE"])

            # iterate rows and upsert
            for _, row in df.iterrows():
                iom_val = None
                if id_col:
                    orig = col_to_orig.get(id_col)
                    iom_val = row.get(orig) if orig else None
                if iom_val is None:
                    for orig_header, dbcol in mapping:
                        if str(orig_header).strip().lower() == "id":
                            iom_val = row.get(orig_header)
                            break
                if iom_val is None:
                    continue
                iom_val = str(iom_val).strip()

                project_id = None
                if prog_col:
                    orig = col_to_orig.get(prog_col)
                    progname = row.get(orig) if orig else None
                    if progname:
                        project_id = existing_projects.get(str(progname).strip())

                def read_val(col_sanitized):
                    if not col_sanitized:
                        return None
                    orig_h = col_to_orig.get(col_sanitized)
                    if not orig_h:
                        return None
                    return _param_safe(row.get(orig_h))

                status_val = read_val(status_col)
                bg_code_val = read_val(bg_code_col)
                year_val = read_val(year_col)
                seller_country_val = read_val(seller_country_col)
                creator_val = read_val(creator_col)
                date_created_val = read_val(date_created_col)
                comment_val = read_val(comment_col)
                buyer_bau_val = read_val(buyer_bau_col)
                buyer_wbs_val = read_val(buyer_wbs_col)
                seller_bau_val = read_val(seller_bau_col)
                seller_wbs_val = read_val(seller_wbs_col)
                site_val = read_val(site_col)
                function_val = read_val(function_col)
                department_val = read_val(department_col)
                total_hours_val = read_val(total_hours_col)
                total_fte_val = read_val(total_fte_col)

                months_hours_vals = {m: (read_val(month_hours_cols[m]) or 0) for m in months}
                months_fte_vals = {m: (read_val(month_fte_cols[m]) or 0) for m in months}

                insert_cols = [
                    "iom_id", "status", "project_id", "bg_code", "year", "seller_country",
                    "creator", "date_created", "comment_of_creator",
                    "buyer_bau", "buyer_wbs_cc", "seller_bau", "seller_wbs_cc",
                    "site", "function", "department"
                ]
                insert_cols += [f"{m}_hours" for m in months]
                insert_cols += ["total_hours"]
                insert_cols += [f"{m}_fte" for m in months]
                insert_cols += ["total_fte"]

                params = []
                params.append(_param_safe(iom_val))
                params.append(status_val)
                params.append(_param_safe(project_id))
                params.append(bg_code_val)
                params.append(year_val)
                params.append(seller_country_val)
                params.append(creator_val)
                params.append(date_created_val)
                params.append(comment_val)
                params.append(buyer_bau_val)
                params.append(buyer_wbs_val)
                params.append(seller_bau_val)
                params.append(seller_wbs_val)
                params.append(site_val)
                params.append(function_val)
                params.append(department_val)

                for m in months:
                    params.append(months_hours_vals.get(m, 0))
                params.append(total_hours_val or 0)
                for m in months:
                    params.append(months_fte_vals.get(m, 0))
                params.append(total_fte_val or 0)

                cols_clause = ", ".join([f"`{c}`" for c in insert_cols])
                placeholders = ", ".join(["%s"] * len(insert_cols))
                update_clause = ",\n                          ".join(
                    [f"`{c}`=VALUES(`{c}`)" for c in insert_cols if c != "iom_id"])

                try:
                    cursor.execute(f"""
                        INSERT INTO prism_wbs ({cols_clause})
                        VALUES ({placeholders})
                        ON DUPLICATE KEY UPDATE
                          {update_clause}
                    """, params)
                    wbs_inserted += 1
                except Exception as e:
                    wbs_failed += 1
                    errors.append(f"IOM {iom_val} upsert failed: {e}")


    except Exception as e:
        messages.error(request, f"Failed during projects/WBS population: {e}")
        return redirect(reverse("settings:import_master"))

    finished_at = datetime.datetime.now()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""
                INSERT INTO `{IMPORT_HISTORY}`
                (`imported_by`,`filename`,`started_at`,`finished_at`,`total_rows`,
                 `master_inserted`,`master_failed`,`projects_created`,`wbs_inserted`,`wbs_failed`,`errors`,`meta_map`)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, [
                importer,
                filename,
                started_at.strftime("%Y-%m-%d %H:%M:%S"),
                finished_at.strftime("%Y-%m-%d %H:%M:%S"),
                int(len(df)),
                int(master_inserted),
                int(master_failed),
                int(projects_created),
                int(wbs_inserted),
                int(wbs_failed),
                json.dumps(errors[:2000]),
                json.dumps({orig: col for orig, col in mapping})
            ])
    except Exception as e:
        messages.warning(request, f"Failed to write import history: {e}")

    messages.info(request, f"Projects created: {projects_created}.")
    messages.info(request, f"WBS inserted/updated: {wbs_inserted}, failed: {wbs_failed}.")
    if errors:
        messages.warning(request, f"{len(errors)} errors occurred during import. Check import_history table for details.")

    preview_headers = orig_headers
    preview_rows = []
    for _, row in df.head(6).iterrows():
        preview_rows.append([str(row.get(h, "")) if not pd.isna(row.get(h, "")) else "" for h in orig_headers])

    return render(request, "settings/import_master.html", {
        "preview_headers": preview_headers,
        "preview_rows": preview_rows,
    })


# ---------------------- Utilities & Settings endpoints ----------------------

def dictfetchall(cursor):
    """Return all rows from a cursor as a list of dicts."""
    cols = [c[0] for c in cursor.description] if cursor.description else []
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def holidays_list(request):
    """List holidays and provide form to add new one."""
    with connection.cursor() as cur:
        cur.execute("SELECT id, holiday_date, name FROM holidays ORDER BY holiday_date")
        holidays = dictfetchall(cur)
    return render(request, "settings/holidays.html", {"holidays": holidays})


def holidays_add(request):
    """Add a holiday (POST)."""
    if request.method != "POST":
        return HttpResponseBadRequest("POST required")
    d = request.POST.get("holiday_date")
    name = request.POST.get("name", "").strip()
    if not d or not name:
        return HttpResponseBadRequest("Date & Name required")
    with connection.cursor() as cur:
        cur.execute("INSERT INTO holidays (holiday_date, name, created_by) VALUES (%s,%s,%s)",
                    [d, name, request.user.email if request.user.is_authenticated else None])
    return redirect(reverse("settings:settings_holidays"))


# ----------------- Monthly Hours Settings endpoints (updated) -----------------

@require_GET
def monthly_hours_settings(request):
    """
    Render settings UI that shows year and months with start_date, end_date and max_hours.
    Template: settings/settings_monthly_hours.html
    """
    try:
        year = int(request.GET.get("year") or datetime.datetime.now().year)
    except Exception:
        year = datetime.datetime.now().year

    months = []
    with connection.cursor() as cur:
        # fetch month, max_hours, start_date, end_date for this year
        cur.execute("""
            SELECT month, max_hours, start_date, end_date
            FROM monthly_hours_limit
            WHERE year=%s
        """, [year])
        rows = cur.fetchall()
        values = {}
        for row in rows:
            m = int(row[0])
            maxh = float(row[1]) if row[1] is not None else 183.75
            sd = row[2]
            ed = row[3]
            # ensure they are date objects or None
            if isinstance(sd, str):
                try:
                    sd = datetime.datetime.strptime(sd, "%Y-%m-%d").date()
                except Exception:
                    sd = None
            if isinstance(ed, str):
                try:
                    ed = datetime.datetime.strptime(ed, "%Y-%m-%d").date()
                except Exception:
                    ed = None
            values[m] = {"max_hours": maxh, "start_date": sd, "end_date": ed}

    for m in range(1, 13):
        entry = values.get(m)
        if entry:
            max_hours = entry["max_hours"]
            sd = entry["start_date"]
            ed = entry["end_date"]
            sd_s = sd.strftime("%Y-%m-%d") if sd else ""
            ed_s = ed.strftime("%Y-%m-%d") if ed else ""
        else:
            first = datetime.date(year, m, 1)
            last = (first.replace(day=28) + datetime.timedelta(days=4)).replace(day=1) - datetime.timedelta(days=1)
            sd_s = first.strftime("%Y-%m-%d")
            ed_s = last.strftime("%Y-%m-%d")
            max_hours = 183.75

        months.append({
            "month": m,
            "value": max_hours,
            "start_date": sd_s,
            "end_date": ed_s
        })

    return render(request, "settings/settings_monthly_hours.html", {"year": year, "months": months})


@require_POST
def save_monthly_hours(request):
    """
    Expects JSON payload:
    { "year": 2025, "months": [ {"month":1,"value":183.75,"start_date":"2025-01-25","end_date":"2025-02-20"}, ... ] }
    """
    try:
        data = json.loads(request.body.decode("utf-8"))
        year = int(data.get("year"))
        months = data.get("months", [])
    except Exception as e:
        return HttpResponseBadRequest("Invalid payload")

    try:
        with connection.cursor() as cur:
            for m in months:
                try:
                    month = int(m.get("month"))
                except Exception:
                    continue
                if not (1 <= month <= 12):
                    continue
                value = float(m.get("value") or 183.75)
                sd_raw = m.get("start_date") or None
                ed_raw = m.get("end_date") or None

                # Robust parse_date: handle date/datetime/timestamp/strings in Y-m-d or m/d/Y
                def parse_date(val):
                    if val is None:
                        return None
                    # if already a date/datetime object
                    if isinstance(val, (datetime.date, datetime.datetime)):
                        return val if isinstance(val, datetime.date) else val.date()
                    # pandas.Timestamp
                    try:
                        import pandas as _pd
                        if isinstance(val, _pd.Timestamp):
                            return val.to_pydatetime().date()
                    except Exception:
                        pass
                    # string -> try common formats
                    val_str = str(val).strip()
                    if not val_str:
                        return None
                    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
                        try:
                            return datetime.datetime.strptime(val_str, fmt).date()
                        except Exception:
                            continue
                    # last attempt: try dateutil-ish fallback (YYYY-M-D)
                    parts = re.split(r'[-/]', val_str)
                    try:
                        if len(parts) == 3:
                            y, mo, d = parts
                            # try detect if format is YYYY MM DD or MM DD YYYY
                            if len(y) == 4:
                                return datetime.date(int(y), int(mo), int(d))
                            elif len(parts[2]) == 4:
                                return datetime.date(int(parts[2]), int(parts[0]), int(parts[1]))
                    except Exception:
                        pass
                    return None

                sd = parse_date(sd_raw)
                ed = parse_date(ed_raw)

                # debug prints (keep temporarily if desired)
                # print("Saving", year, month, value, sd_raw, ed_raw)
                # print("Parsed", sd, ed)

                cur.execute("""
                    INSERT INTO monthly_hours_limit (year, month, start_date, end_date, max_hours)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                      start_date = VALUES(start_date),
                      end_date = VALUES(end_date),
                      max_hours = VALUES(max_hours),
                      updated_at = CURRENT_TIMESTAMP
                """, [year, month, sd, ed, value])
    except Exception as ex:
        return JsonResponse({"ok": False, "error": str(ex)})

    return JsonResponse({"ok": True, "year": year})


@require_GET
def get_monthly_max(request):
    """
    GET params: year, month
    Returns: {ok: True, max_hours: x, start_date: "YYYY-MM-DD", end_date: "YYYY-MM-DD"}
    """
    try:
        year = int(request.GET.get("year"))
        month = int(request.GET.get("month"))
    except Exception:
        return HttpResponseBadRequest("Invalid year/month")

    with connection.cursor() as cur:
        cur.execute("""
            SELECT max_hours, start_date, end_date FROM monthly_hours_limit
            WHERE year=%s AND month=%s
        """, [year, month])
        row = cur.fetchone()

    if row:
        max_hours = float(row[0]) if row[0] is not None else 183.75
        sd = row[1]
        ed = row[2]
    else:
        first = datetime.date(year, month, 1)
        last = (first.replace(day=28) + datetime.timedelta(days=4)).replace(day=1) - datetime.timedelta(days=1)
        sd = first
        ed = last
        max_hours = 183.75

    sd_s = sd.strftime("%Y-%m-%d") if sd else ""
    ed_s = ed.strftime("%Y-%m-%d") if ed else ""

    return JsonResponse({"ok": True, "max_hours": max_hours, "start_date": sd_s, "end_date": ed_s})
