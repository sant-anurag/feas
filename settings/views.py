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
    - numeric-like -> string or float is fine, but we prefer string to be safe
    - otherwise str(v)
    This avoids driver-level bytes-formatting errors.
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

    if isinstance(v, (datetime.date, datetime.datetime)):
        # MySQL DATETIME accepts 'YYYY-MM-DD HH:MM:SS'
        if isinstance(v, datetime.datetime):
            return v.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return v.strftime("%Y-%m-%d")

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
    # Consistent column names used in insert below
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

    started_at = datetime.datetime.datetime.now() if False else datetime.datetime.now()
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

    # Step C: populate projects and prism_wbs
    # Build mapping helpers
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

            # Populate projects (unique by Program) and capture OEM from first occurrence
            if prog_col:
                # gather unique program names from df and first-seen OEM
                programs: Dict[str, Dict[str, Optional[str]]] = {}
                for _, row in df.iterrows():
                    orig_prog = col_to_orig.get(prog_col)
                    prog_val = row.get(orig_prog) if orig_prog else None
                    if prog_val is None or (isinstance(prog_val, float) and pd.isna(prog_val)):
                        # fallback: scan header names for a 'program' column
                        for orig_h, dbc in mapping:
                            if "program" in str(orig_h).strip().lower():
                                prog_val = row.get(orig_h)
                                break
                    if prog_val is None:
                        continue
                    prog_name = str(prog_val).strip()
                    if prog_name == "":
                        continue
                    # Buyer OEM (if present) -> use first non-empty per program
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
                        # fill if previously empty
                        if programs[prog_name].get("oem_name") in (None, "") and oem_val:
                            programs[prog_name]["oem_name"] = oem_val

                # Insert projects using captured OEM
                for p in sorted(programs.keys()):
                    if p == "":
                        continue
                    if p in existing_projects:
                        continue
                    try:
                        oem_for_p = programs[p].get("oem_name")
                        cursor.execute("INSERT INTO projects (name, oem_name) VALUES (%s,%s)", [p, oem_for_p])
                        # fetch id and cache
                        cursor.execute("SELECT id FROM projects WHERE name=%s LIMIT 1", [p])
                        res = cursor.fetchone()
                        if res:
                            existing_projects[p] = res[0]
                        projects_created += 1
                    except Exception as e:
                        errors.append(f"Failed to insert project '{p}': {e}")

            # Populate prism_wbs using ON DUPLICATE KEY UPDATE
            # -----------------------------
            # Populate prism_wbs using ON DUPLICATE KEY UPDATE (QUOTED IDENTIFIERS)
            # -----------------------------
            def _find(orig_variants):
                return find_col_by_variants(orig_variants)

            # detect columns (same as before)
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
                # iom id extraction (same logic)
                iom_val = None
                if id_col:
                    orig = col_to_orig.get(id_col)
                    iom_val = row.get(orig) if orig else None
                if iom_val is None:
                    # fallback: try literal "id" header
                    for orig_header, dbcol in mapping:
                        if str(orig_header).strip().lower() == "id":
                            iom_val = row.get(orig_header)
                            break
                if iom_val is None:
                    continue
                iom_val = str(iom_val).strip()

                # project id lookup
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

                # collect scalar fields
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

                # months
                months_hours_vals = {m: (read_val(month_hours_cols[m]) or 0) for m in months}
                months_fte_vals = {m: (read_val(month_fte_cols[m]) or 0) for m in months}

                # build ordered column list (use DB column names exactly)
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

                # create sql pieces with backticks for safety
                cols_clause = ", ".join([f"`{c}`" for c in insert_cols])
                placeholders = ", ".join(["%s"] * len(insert_cols))
                # IMPORTANT: quote identifiers in update clause too
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

    # Persist import history (use column names from _ensure_import_history_table)
    finished_at = datetime.datetime.datetime.now() if False else datetime.datetime.now()
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
        # do not abort final result if history logging fails, just warn
        messages.warning(request, f"Failed to write import history: {e}")

    # Final UI messages
    messages.info(request, f"Projects created: {projects_created}.")
    messages.info(request, f"WBS inserted/updated: {wbs_inserted}, failed: {wbs_failed}.")
    if errors:
        # If too many errors, only show count and mention import_history for details
        messages.warning(request, f"{len(errors)} errors occurred during import. Check import_history table for details.")

    # Provide a small preview to the template
    preview_headers = orig_headers
    preview_rows = []
    for _, row in df.head(6).iterrows():
        row_list = []
        for h in preview_headers:
            v = row.get(h, "")
            if pd.isna(v):
                v = ""
            else:
                v = str(v)
            row_list.append(v)
        preview_rows.append(row_list)

    preview_headers = orig_headers
    preview_rows = []
    for _, row in df.head(6).iterrows():
        preview_rows.append([str(row.get(h, "")) if not pd.isna(row.get(h, "")) else "" for h in orig_headers])

    return render(request, "settings/import_master.html", {
        "preview_headers": preview_headers,
        "preview_rows": preview_rows,
    })

from django.shortcuts import render
from django.http import JsonResponse, HttpResponseBadRequest
from django.views.decorators.http import require_GET, require_POST
from django.db import connection
from datetime import datetime
import json

@require_GET
def monthly_hours_settings(request):
    print("[monthly_hours_settings] called")
    try:
        year = int(request.GET.get("year") or datetime.now().year)
        print(f"[monthly_hours_settings] year from request: {year}")
    except ValueError:
        year = datetime.now().year
        print(f"[monthly_hours_settings] invalid year, using current: {year}")

    months = []
    with connection.cursor() as cur:
        print(f"[monthly_hours_settings] executing SELECT for year: {year}")
        cur.execute("SELECT month, max_hours FROM monthly_hours_limit WHERE year=%s", [year])
        rows = cur.fetchall()
        print(f"[monthly_hours_settings] rows fetched: {rows}")
        values = {row[0]: float(row[1]) for row in rows}
        print(f"[monthly_hours_settings] values dict: {values}")

    for m in range(1, 13):
        val = values.get(m, 183.75)
        print(f"[monthly_hours_settings] month: {m}, value: {val}")
        months.append({"month": m, "value": val})

    print(f"[monthly_hours_settings] months list: {months}")
    return render(request, "settings/settings_monthly_hours.html", {"year": year, "months": months})


@require_POST
def save_monthly_hours(request):
    print("[save_monthly_hours] called")
    try:
        data = json.loads(request.body.decode("utf-8"))
        print(f"[save_monthly_hours] data loaded: {data}")
        year = int(data.get("year"))
        months = data.get("months", [])
        print(f"[save_monthly_hours] year: {year}, months: {months}")
    except Exception as e:
        print(f"[save_monthly_hours] Exception parsing payload: {e}")
        return HttpResponseBadRequest("Invalid payload")

    try:
        with connection.cursor() as cur:
            for m in months:
                month = int(m.get("month"))
                value = float(m.get("value"))
                print(f"[save_monthly_hours] inserting month: {month}, value: {value}")
                if not (1 <= month <= 12):
                    print(f"[save_monthly_hours] skipping invalid month: {month}")
                    continue
                cur.execute("""
                    INSERT INTO monthly_hours_limit (year, month, max_hours)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE max_hours=VALUES(max_hours), updated_at=CURRENT_TIMESTAMP
                """, [year, month, value])
    except Exception as ex:
        print(f"[save_monthly_hours] Exception during DB insert: {ex}")
        return JsonResponse({"ok": False, "error": str(ex)})

    print(f"[save_monthly_hours] save successful for year: {year}")
    return JsonResponse({"ok": True, "year": year})


@require_GET
def get_monthly_max(request):
    print("[get_monthly_max] called")
    try:
        year = int(request.GET.get("year"))
        month = int(request.GET.get("month"))
        print(f"[get_monthly_max] year: {year}, month: {month}")
    except Exception as e:
        print(f"[get_monthly_max] Exception parsing year/month: {e}")
        return HttpResponseBadRequest("Invalid year/month")

    with connection.cursor() as cur:
        print(f"[get_monthly_max] executing SELECT for year: {year}, month: {month}")
        cur.execute("""
            SELECT max_hours FROM monthly_hours_limit
            WHERE year=%s AND month=%s
        """, [year, month])
        row = cur.fetchone()
        print(f"[get_monthly_max] row fetched: {row}")
    max_hours = float(row[0]) if row else 183.75
    print(f"[get_monthly_max] max_hours: {max_hours}")

    return JsonResponse({"ok": True, "max_hours": max_hours})