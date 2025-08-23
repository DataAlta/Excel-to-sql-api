#POST /api/parse_business_mapping (initial preview)
#POST /api/parse_business_mapping_columns (your code, integrated)
#POST /api/reparse_business_mapping (override header row)
#POST /api/infer_sql_structure (your code, integrated)
#POST /api/generate_sql_with_patterns (your pattern SQL builder, integrated)
#POST /api/save_business_mapping (stub that you can wire to DB/disk)
#CORS (so your Hostinger page can call it)
#Small helpers: _pick_engine and detect_header_row

import io
import json
from typing import Any, Dict, List, Optional

import pandas as pd
from pandas import ExcelFile

from fastapi import FastAPI, APIRouter, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware

# =========================
# FastAPI app + CORS
# =========================
app = FastAPI(title="ExceltoSQL API", version="1.0.0")

origins = [
    "https://dataalta.com",
    "https://www.dataalta.com",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["POST"],  # *
    allow_headers=["*"],
)

router = APIRouter()

# =========================
# Helpers
# =========================
def _pick_engine(filename: str) -> str:
    """Choose a pandas Excel engine based on extension."""
    fname = (filename or "").lower()
    if fname.endswith(".xlsx") or fname.endswith(".xlsm"):
        return "openpyxl"
    if fname.endswith(".xls"):
        return "xlrd"   # requires xlrd for legacy .xls
    # default to openpyxl
    return "openpyxl"

def detect_header_row(sample_df: pd.DataFrame, max_scan: int = 25) -> Optional[int]:
    """
    Heuristic: pick the first row (0-based) among top `max_scan` rows
    that has the highest count of non-null *and* a decent share of strings/unique-ish values.
    Returns 0-based row index or None if nothing reasonable found.
    """
    if sample_df is None or sample_df.empty:
        return None

    best_idx, best_score = None, -1
    nrows = min(len(sample_df), max_scan)
    for i in range(nrows):
        row = sample_df.iloc[i]
        non_null = row.notna().sum()
        as_str = row.astype(str).str.strip()
        non_empty = (as_str != "").sum()
        unique_ratio = as_str.nunique(dropna=True) / max(non_empty, 1)
        # favor rows that are non-null, non-empty and fairly unique (like headings)
        score = non_null * 1.0 + non_empty * 0.5 + unique_ratio * 3.0
        if score > best_score and non_null >= max(1, int(0.4 * len(row))):
            best_idx, best_score = i, score
    return best_idx if best_idx is not None else None

def _preview_rows(df: pd.DataFrame, limit: int) -> List[Dict[str, Any]]:
    """
    Convert the first `limit` rows to a list of dicts (JSON serializable).
    """
    out = []
    for _, row in df.head(limit).iterrows():
        rec = {}
        for c in df.columns:
            v = row.get(c)
            # Convert numpy types to Python builtins
            if pd.isna(v):
                rec[str(c)] = None
            else:
                rec[str(c)] = v.item() if hasattr(v, "item") else v
        out.append(rec)
    return out

# =========================
# 1) Initial preview
# =========================
@router.post("/api/parse_business_mapping")
async def parse_business_mapping(
    file: UploadFile = File(...),
    preview_rows: int = Form(50),
):
    """
    Upload an Excel, do light header detection per sheet, return:
    {
      message, filename,
      preview: [{sheet, header_row_excel, columns, rows}]
    }
    """
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

    try:
        engine = _pick_engine(file.filename)
        xls = ExcelFile(io.BytesIO(content), engine=engine)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Unable to open workbook: {e}")

    preview = []
    for sheet in xls.sheet_names:
        # read a small slice without headers for detection
        head = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=30)
        header_row = detect_header_row(head, max_scan=25)
        if header_row is None:
            # fallback: first non-empty row
            nonempty_mask = head.apply(lambda r: r.notna().any(), axis=1)
            header_row = int(nonempty_mask.idxmax()) if nonempty_mask.any() else 0

        # re-read with inferred header
        df = pd.read_excel(xls, sheet_name=sheet, header=header_row)
        # normalize columns
        cols = []
        for i, c in enumerate(df.columns):
            name = str(c).strip()
            cols.append(name if name else f"Unnamed_{i}")
        df.columns = cols

        preview.append({
            "sheet": sheet,
            "header_row_excel": int(header_row) + 1,  # 1-based for Excel row no.
            "columns": cols,
            "rows": _preview_rows(df, preview_rows),
        })

    return {
        "message": f"Parsed {len(preview)} sheet(s).",
        "filename": file.filename,
        "preview": preview,
    }

# =========================
# 2) Your: parse_business_mapping_columns
# =========================
@router.post("/api/parse_business_mapping_columns")
async def parse_business_mapping_columns(
    file: UploadFile = File(...),
    sheet: str = Form(...),
    roles: str = Form(...),  # JSON: {"output":"ColA","table":"ColB","column":"ColC","mappingType":"ColD","transform":"ColE"}
):
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

    try:
        role_map = json.loads(roles or "{}")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid roles JSON")

    required = ["output", "table", "column"]
    missing = [k for k in required if not role_map.get(k)]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing required role(s): {', '.join(missing)}")

    try:
        engine = _pick_engine(file.filename)
        xls = ExcelFile(io.BytesIO(content), engine=engine)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Unable to open workbook: {e}")

    if sheet not in xls.sheet_names:
        raise HTTPException(status_code=404, detail=f"Sheet '{sheet}' not found")

    # Read sheet with auto header detection like upload route did
    head = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=30)
    header_row = detect_header_row(head, max_scan=25)
    if header_row is None:
        # fallback first non-empty
        nonempty_mask = head.apply(lambda r: r.notna().any(), axis=1)
        header_row = int(nonempty_mask.idxmax()) if nonempty_mask.any() else 0

    df = pd.read_excel(xls, sheet_name=sheet, header=header_row)
    df.columns = [str(c).strip() if str(c).strip() != "" else f"Unnamed_{i}" for i, c in enumerate(df.columns)]

    # Ensure selected columns exist
    for key, colname in role_map.items():
        if not colname:
            continue
        if colname not in df.columns:
            raise HTTPException(status_code=400, detail=f"Column '{colname}' (role '{key}') not found in sheet")

    # Build mapping rows from ALL rows in the sheet
    out_col = role_map["output"]
    tbl_col = role_map["table"]
    col_col = role_map["column"]
    map_col = role_map.get("mappingType")
    trn_col = role_map.get("transform")

    records = []
    for _, row in df.iterrows():
        output = str(row.get(out_col, "")).strip()
        table = str(row.get(tbl_col, "")).strip()
        column = str(row.get(col_col, "")).strip()
        mapping_type = str(row.get(map_col, "")).strip() if map_col else ""
        transform = str(row.get(trn_col, "")).strip() if trn_col else ""

        # skip empty lines (require the required trio)
        if not (output and table and column):
            continue

        records.append({
            "output": output,
            "table": table,
            "column": column,
            "mappingType": mapping_type or "",
            "transform": transform or "",
        })

    return {
        "sheet": sheet,
        "header_row_excel": int(header_row) + 1,
        "rows": records,
        "count": len(records),
        "message": f"Parsed {len(records)} mapping rows from '{sheet}'."
    }

# =========================
# 3) Re-parse a single sheet with a forced header row
# =========================
@router.post("/api/reparse_business_mapping")
async def reparse_business_mapping(
    file: UploadFile = File(...),
    sheet: str = Form(...),
    header_row_excel: int = Form(...),  # 1-based
    preview_rows: int = Form(50),
):
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

    try:
        engine = _pick_engine(file.filename)
        xls = ExcelFile(io.BytesIO(content), engine=engine)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Unable to open workbook: {e}")

    if sheet not in xls.sheet_names:
        raise HTTPException(status_code=404, detail=f"Sheet '{sheet}' not found")

    header_idx = max(0, int(header_row_excel) - 1)
    df = pd.read_excel(xls, sheet_name=sheet, header=header_idx)
    cols = [str(c).strip() if str(c).strip() != "" else f"Unnamed_{i}" for i, c in enumerate(df.columns)]
    df.columns = cols

    return {
        "sheet": sheet,
        "header_row_excel": int(header_idx) + 1,
        "columns": cols,
        "rows": _preview_rows(df, preview_rows),
        "message": f"Re-parsed '{sheet}' with header at Excel row {header_idx + 1}."
    }

# =========================
# 4) Your: infer_sql_structure
# =========================
@router.post("/api/infer_sql_structure")
async def infer_sql_structure(body: Dict[str, Any]):
    """
    Infer a technical SQL structure *purely* from mapping rows.
    Ignores filename/sheet — they are just metadata from the frontend.
    """

    rows = body.get("rows") or []
    if not isinstance(rows, list) or not rows:
        return {"from": "", "select_items": [], "joins": [], "message": "No mapping rows provided"}

    # --- Helper: split table vs alias (if user typed "TableName Alias") ---
    def split_table_alias(t: str) -> Dict[str, str]:
        t = (t or "").strip()
        if not t:
            return {"table": "", "alias": ""}
        parts = t.split()
        if len(parts) >= 2:
            return {"table": " ".join(parts[:-1]), "alias": parts[-1]}
        return {"table": t, "alias": ""}

    # --- Gather stats ---
    table_counts: Dict[str, int] = {}
    table_columns: Dict[str, set] = {}
    parsed_rows = []

    for r in rows:
        t_raw = (r.get("table") or "").strip()
        c = (r.get("column") or "").strip()
        out = (r.get("output") or "").strip()
        tr = (r.get("transform") or "").strip()
        if not (t_raw and c and out):
            continue

        ta = split_table_alias(t_raw)
        table = ta["table"]
        alias = ta["alias"]

        table_counts[table] = table_counts.get(table, 0) + 1
        table_columns.setdefault(table, set()).add(c)

        parsed_rows.append({
            "table": table,
            "alias_in": alias,
            "column": c,
            "output": out,
            "transform": tr,
        })

    if not parsed_rows:
        return {"from": "", "select_items": [], "joins": [], "message": "No complete mapping rows to infer from"}

    # --- Choose base table by frequency ---
    base_table = max(table_counts.items(), key=lambda kv: kv[1])[0]

    # --- Alias assignment ---
    alias_map: Dict[str, str] = {}
    used = set()
    for pr in parsed_rows:
        if pr["alias_in"]:
            alias_map[pr["table"]] = pr["alias_in"]
            used.add(pr["alias_in"])

    def make_alias(t: str) -> str:
        base = (t[:1] or "t").upper()
        cand = base
        i = 2
        while cand in used:
            cand = f"{base}{i}"
            i += 1
        used.add(cand)
        return cand

    for t in table_counts:
        if t not in alias_map:
            alias_map[t] = make_alias(t)

    base_alias = alias_map[base_table]
    base_from = f"{base_table} {base_alias}"

    # --- SELECT items ---
    select_items = []
    for pr in parsed_rows:
        t, a, col, out, tr = pr["table"], alias_map[pr["table"]], pr["column"], pr["output"], pr["transform"]
        expr = tr if tr else f"{a}.{col}"
        alias_for_output = (out.replace(" ", "") or col)
        select_items.append({"output": out, "expression": expr, "alias": alias_for_output})

    # --- JOINs (only if evidence) ---
    joins = []
    if len(table_counts) > 1:
        import re
        key_rx = re.compile(r"(id|code|key)$", re.IGNORECASE)
        base_cols = table_columns[base_table]
        prio_keys_base = {c for c in base_cols if key_rx.search(c)}

        for other in table_counts:
            if other == base_table:
                continue
            other_cols = table_columns[other]
            prio_keys_other = {c for c in other_cols if key_rx.search(c)}

            shared = prio_keys_base & prio_keys_other
            if not shared:
                shared = base_cols & other_cols
            if shared:
                key = sorted(shared)[0]
                joins.append({
                    "type": "LEFT",
                    "left_table": f"{base_table} {base_alias}",
                    "left_key": f"{base_alias}.{key}",
                    "right_table": f"{other} {alias_map[other]}",
                    "right_key": f"{alias_map[other]}.{key}",
                    "condition": ""
                })

    return {
        "from": base_from,
        "select_items": select_items,
        "joins": joins,
        "message": "Inferred from mapping",
    }

# =========================
# 5) Pattern-based SQL builder (your logic)
# =========================
def _render_like(col: str, mode: str, value: str) -> str:
    if mode == "contains":    return f"{col} LIKE '%{value}%'"
    if mode == "starts_with": return f"{col} LIKE '{value}%'"
    if mode == "ends_with":   return f"{col} LIKE '%{value}'"
    if mode == "equals":      return f"{col} = '{value}'"
    raise HTTPException(status_code=400, detail="Unknown text_match mode")

def _render_rank_cte(base_table: str, base_alias: str, group_key: str, order_col: str, direction: str, cte_name: str) -> str:
    base = f"{base_table} {base_alias}".rstrip()  # alias may be empty
    return (
        f"{cte_name} AS (\n"
        f"  SELECT *,\n"
        f"         ROW_NUMBER() OVER (PARTITION BY {group_key} ORDER BY {order_col} {direction}) AS rn\n"
        f"  FROM {base}\n"
        f")"
    )

def _apply_pattern(item: Dict[str, Any], pconf: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not pconf:
        return item
    intent = pconf.get("intent") or "none"
    params = pconf.get("params") or {}
    expr = item.get("expression") or ""
    if intent == "none":
        return item

    if intent == "conditional_sum":
        cond = params.get("condition"); measure = params.get("measure")
        if not cond or not measure:
            return {**item, "note": "conditional_sum missing condition/measure"}
        return {**item, "expression": f"SUM(CASE WHEN {cond} THEN {measure} ELSE 0 END)"}

    if intent == "conditional_count":
        cond = params.get("condition")
        if not cond:
            return {**item, "expression": "COUNT(*)", "note": "conditional_count missing condition → COUNT(*)"}
        return {**item, "expression": f"COUNT(CASE WHEN {cond} THEN 1 END)"}

    if intent == "text_match":
        col = params.get("column"); mode = params.get("mode") or "contains"; value = params.get("value") or ""
        measure = params.get("measure")
        if not col:
            return {**item, "note": "text_match missing column"}
        cond = _render_like(col, mode, value)
        if expr.upper().startswith("SUM(") and measure:
            return {**item, "expression": f"SUM(CASE WHEN {cond} THEN {measure} ELSE 0 END)"}
        if expr.upper().startswith("COUNT("):
            return {**item, "expression": f"COUNT(CASE WHEN {cond} THEN 1 END)"}
        return {**item, "expression": f"CASE WHEN {cond} THEN 1 ELSE 0 END"}

    if intent in ("first_occurrence_per", "last_occurrence_per"):
        gk = params.get("group_key"); oc = params.get("order_col")
        if not gk or not oc:
            return {**item, "note": f"{intent} missing group_key/order_col"}
        return {**item, "meta": {"needs_cte": True, "type": "rank_first_last", "group_key": gk, "order_col": oc,
                                 "direction": "ASC" if intent == "first_occurrence_per" else "DESC",
                                 "cte_name": "ranked_first_last"}}

    if intent == "top_n_per":
        gk = params.get("group_key"); oc = params.get("order_col"); n = int(params.get("n", 1))
        if not gk or not oc:
            return {**item, "note": "top_n_per missing group_key/order_col"}
        return {**item, "meta": {"needs_cte": True, "type": "rank_top_n", "group_key": gk, "order_col": oc, "n": n,
                                 "cte_name": "ranked_topn"}}

    if intent == "distinct_count":
        col = params.get("column")
        if not col:
            return {**item, "note": "distinct_count missing column"}
        return {**item, "expression": f"COUNT(DISTINCT {col})"}

    return item

def _build_sql_with_patterns(payload: Dict[str, Any]) -> str:
    # Require base table from payload (derived from Excel via frontend)
    base = payload.get("from") or {}
    base_table = (base.get("table") or "").strip()
    base_alias = (base.get("alias") or "").strip()

    if not base_table:
        raise HTTPException(status_code=400, detail='Missing FROM base table. Provide it as "<table> [alias]".')

    joins = payload.get("joins") or []
    items = payload.get("select_items") or []
    patterns = payload.get("patterns") or {}

    applied: List[Dict[str, Any]] = []
    ctes: List[str] = []
    where_clauses: List[str] = []

    for it in items:
        key = it.get("alias") or it.get("output") or ""
        new_it = _apply_pattern(it, patterns.get(key))
        meta = new_it.get("meta") or {}
        if meta.get("needs_cte"):
            cte_name = meta.get("cte_name")
            if meta.get("type") == "rank_first_last":
                ctes.append(_render_rank_cte(base_table, base_alias, meta["group_key"], meta["order_col"], meta["direction"], cte_name))
                where_clauses.append(f"{cte_name}.rn = 1")
                if new_it.get("expression") and base_alias:
                    new_it["expression"] = new_it["expression"].replace(f"{base_alias}.", f"{cte_name}.")
            if meta.get("type") == "rank_top_n":
                ctes.append(_render_rank_cte(base_table, base_alias, meta["group_key"], meta["order_col"], "DESC", cte_name))
                where_clauses.append(f"{cte_name}.rn <= {int(meta.get('n', 1))}")
                if new_it.get("expression") and base_alias:
                    new_it["expression"] = new_it["expression"].replace(f"{base_alias}.", f"{cte_name}.")
        applied.append(new_it)

    select_sql = ",\n  ".join(
        [f"{it.get('expression')} AS [{it.get('output')}]" for it in applied if it.get("expression")]
    )
    base_from = f"{base_table} {base_alias}".rstrip()
    join_sql = ("\n" + "\n".join(joins)) if joins else ""
    where_sql = f"\nWHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    with_sql = f"WITH\n" + ",\n".join(ctes) + "\n" if ctes else ""
    return f"{with_sql}SELECT\n  {select_sql}\nFROM {base_from}{join_sql}{where_sql}\n;"

@router.post("/api/generate_sql_with_patterns")
async def generate_sql_with_patterns(payload: Dict[str, Any]):
    try:
        sql = _build_sql_with_patterns(payload)
        return {"sql": sql}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to generate SQL: {e}")

# =========================
# 6) Save (stub)
# =========================
@router.post("/api/save_business_mapping")
async def save_business_mapping(payload: Dict[str, Any]):
    # TODO: Persist to DB / S3 / file. Returning OK for now.
    return {"message": "saved", "received": bool(payload)}

# =========================
# Health
# =========================
@app.get("/")
def health():
    return {"ok": True, "service": "excel-to-sql", "version": "1.0.0"}

# Mount router
app.include_router(router)
