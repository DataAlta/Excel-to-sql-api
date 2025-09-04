import io
import json
from typing import Any, Dict, List, Optional
import re
import pandas as pd

from fastapi import FastAPI, APIRouter, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# =========================
# FastAPI app + CORS
# =========================
app = FastAPI(title="ExceltoSQL API", version="1.0.0")

origins = [
    "https://dataalta.com",
    "https://www.dataalta.com",
    # add localhost while testing if needed:
    # "http://localhost:3000",
    # "http://127.0.0.1:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

router = APIRouter()

# =========================
# Pydantic response models
# =========================
class SheetPreview(BaseModel):
    sheet: str
    header_row_excel: int
    columns: List[str]
    rows: List[Dict[str, Any]]

class ParseResponse(BaseModel):
    message: str
    filename: str
    preview: List[SheetPreview]

# =========================
# Helpers
# =========================
def _pick_engine(filename: str) -> str:
    fname = (filename or "").lower()
    if fname.endswith((".xlsx", ".xlsm")):
        return "openpyxl"
    if fname.endswith(".xls"):
        return "xlrd"  # pip install xlrd==1.2.0 required for legacy .xls
    return "openpyxl"

def detect_header_row(sample_df: pd.DataFrame, max_scan: int = 25) -> Optional[int]:
    if sample_df is None or sample_df.empty:
        return None

    best_idx, best_score = None, -1.0
    nrows = min(len(sample_df), max_scan)
    for i in range(nrows):
        row = sample_df.iloc[i]
        non_null = row.notna().sum()
        as_str = row.astype(str).str.strip()
        non_empty = (as_str != "").sum()
        unique_ratio = as_str.nunique(dropna=True) / max(non_empty, 1)
        score = non_null * 1.0 + non_empty * 0.5 + unique_ratio * 3.0
        if score > best_score and non_null >= max(1, int(0.4 * len(row))):
            best_idx, best_score = i, score
    return best_idx if best_idx is not None else None

def _preview_rows(df: pd.DataFrame, limit: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for _, row in df.head(limit).iterrows():
        rec: Dict[str, Any] = {}
        for c in df.columns:
            v = row.get(c)
            if pd.isna(v):
                rec[str(c)] = None
            else:
                rec[str(c)] = v.item() if hasattr(v, "item") else v
        out.append(rec)
    return out

def parse_join_condition(cond: str):
     parts = cond.split('=')
     if len(parts) == 2:
         return parts[0].strip(), parts[1].strip()
     return "", ""

def get_alias(table_str):
    parts = table_str.strip().split()
    if len(parts) >= 2:
        return parts[-1].strip()
    return ""

def qualify_key(alias, key):
    if not key:
        return ""
    if "." in key:
        return key
    if alias:
        return f"{alias}.{key}"
    return key


def parse_join_condition_sides(condition):
    # returns (left_table, left_col), (right_table, right_col)
    parts = condition.split("=")
    if len(parts) != 2:
        return ("", ""), ("", "")
    left, right = parts[0].strip(), parts[1].strip()
    if "." in left:
        lt, lcol = left.split(".", 1)
    else:
        lt, lcol = "", left
    if "." in right:
        rt, rcol = right.split(".", 1)
    else:
        rt, rcol = "", right
    return (lt.strip(), lcol.strip()), (rt.strip(), rcol.strip())

# =========================
# 1) Initial preview  (now using UploadFile param)
# =========================
@router.post("/api/parse_business_mapping", response_model=ParseResponse)
async def parse_business_mapping(
    file: UploadFile = File(...),
    preview_rows: int = Form(50),
):
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")
    try:
        engine = _pick_engine(file.filename)
        print(f"Using engine: {engine} for file {file.filename}")
        xls = pd.ExcelFile(io.BytesIO(content), engine=engine)
        print(f"Sheet names: {xls.sheet_names}")
    except Exception as e:
        print(f"Error opening workbook: {e}")
        raise HTTPException(status_code=400, detail=f"Unable to open workbook: {e}")

    preview = []
    for sheet in xls.sheet_names:
        try:
            head = pd.read_excel(io.BytesIO(content), sheet_name=sheet, header=None, nrows=30, engine=engine)
            header_row = detect_header_row(head, max_scan=25)
            if header_row is None:
                nonempty_mask = head.apply(lambda r: r.notna().any(), axis=1)
                header_row = int(nonempty_mask.idxmax()) if nonempty_mask.any() else 0
            df = pd.read_excel(io.BytesIO(content), sheet_name=sheet, header=header_row, engine=engine)
            cols = [str(c).strip() if str(c).strip() != "" else f"Unnamed_{i}" for i, c in enumerate(df.columns)]
            df.columns = cols
            preview.append(SheetPreview(
                sheet=sheet,
                header_row_excel=header_row + 1,
                columns=cols,
                rows=_preview_rows(df, preview_rows),
            ))
        except Exception as e:
            print(f"Error processing sheet '{sheet}': {e}")
            raise HTTPException(status_code=400, detail=f"Error processing sheet '{sheet}': {e}")
    print(f"Returning preview for {len(preview)} sheets")
    return ParseResponse(
        message=f"Parsed {len(preview)} sheet(s).",
        filename=file.filename,
        preview=preview,
    )

# =========================
# 2) Parse mapping columns
# =========================
@router.post("/api/parse_business_mapping_columns")
async def parse_business_mapping_columns(
    file: UploadFile = File(...),
    sheet: str = Form(...),
    roles: str = Form(...),
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
        xls = pd.ExcelFile(io.BytesIO(content), engine=engine)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Unable to open workbook: {e}")

    if sheet not in xls.sheet_names:
        raise HTTPException(status_code=404, detail=f"Sheet '{sheet}' not found")

    head = pd.read_excel(io.BytesIO(content), sheet_name=sheet, header=None, nrows=30, engine=engine)
    header_row = detect_header_row(head, max_scan=25)
    if header_row is None:
        nonempty_mask = head.apply(lambda r: r.notna().any(), axis=1)
        header_row = int(nonempty_mask.idxmax()) if nonempty_mask.any() else 0

    df = pd.read_excel(io.BytesIO(content), sheet_name=sheet, header=header_row, engine=engine)
    df.columns = [str(c).strip() if str(c).strip() != "" else f"Unnamed_{i}" for i, c in enumerate(df.columns)]

    for key, colname in role_map.items():
        if not colname:
            continue
        if colname not in df.columns:
            raise HTTPException(status_code=400, detail=f"Column '{colname}' (role '{key}') not found in sheet")

    out_col = role_map["output"]
    tbl_col = role_map["table"]
    col_col = role_map["column"]
    join_col = role_map.get("join")
    trn_col = role_map.get("transform")

    records = []
    for _, row in df.iterrows():
        output = str(row.get(out_col, "")).strip()
        table = str(row.get(tbl_col, "")).strip()
        column = str(row.get(col_col, "")).strip()
        join_value = str(row.get(join_col, "")).strip()
        transform = str(row.get(trn_col, "")).strip() if trn_col else ""

        if not (output and table and column):
            continue

        records.append({
            "output": output,
            "table": table,
            "column": column,
            "join": join_value,
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
    header_row_excel: int = Form(...),
    preview_rows: int = Form(50),
):
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

    try:
        engine = _pick_engine(file.filename)
        xls = pd.ExcelFile(io.BytesIO(content), engine=engine)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Unable to open workbook: {e}")

    if sheet not in xls.sheet_names:
        raise HTTPException(status_code=404, detail=f"Sheet '{sheet}' not found")

    header_idx = max(0, int(header_row_excel) - 1)
    df = pd.read_excel(io.BytesIO(content), sheet_name=sheet, header=header_idx, engine=engine)
    cols = [str(c).strip() if str(c).strip() != "" else f"Unnamed_{i}" for i, c in enumerate(df.columns)]
    df.columns = cols

    return {
        "sheet": sheet,
        "header_row_excel": header_idx + 1,
        "columns": cols,
        "rows": _preview_rows(df, preview_rows),
        "message": f"Re-parsed '{sheet}' with header at Excel row {header_idx + 1}."
    }

# =========================
# 4) Infer SQL structure (kept unchanged)
# =========================

@router.post("/api/infer_sql_structure")
async def infer_sql_structure(body: Dict[str, Any]):
    rows = body.get("rows") or []
    if not isinstance(rows, list) or not rows:
        return {"from": "", "select_items": [], "joins": [], "message": "No mapping rows provided"}

    base_table_from_request = body.get("base_table")

    def split_table_alias(t: str) -> Dict[str, str]:
        t = (t or "").strip()
        if not t:
            return {"table": "", "alias": ""}
        parts = t.split()
        if len(parts) >= 2:
            return {"table": " ".join(parts[:-1]), "alias": parts[-1]}
        return {"table": t, "alias": ""}

    table_counts: Dict[str, int] = {}
    table_columns: Dict[str, set] = {}
    parsed_rows = []

    for r in rows:
        t_raw = (r.get("table") or "").strip()
        c = (r.get("column") or "").strip()
        out = (r.get("output") or "").strip()
        tr = (r.get("transform") or "").strip()
        join_cond = (r.get("join") or "").strip()  # New join role column
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
            "join": join_cond,
        })

    if not parsed_rows:
        return {"from": "", "select_items": [], "joins": [], "message": "No complete mapping rows to infer from"}

    base_table = base_table_from_request if base_table_from_request else max(table_counts.items(), key=lambda kv: kv[1])[0]
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

    base_alias = alias_map.get(base_table)
    if not base_alias:
        base_alias = base_table[:1].upper() if base_table else "T"

    base_from = f"{base_table} {base_alias}"

    select_items = []
    for pr in parsed_rows:
        t, a, col, out, tr = pr["table"], alias_map[pr["table"]], pr["column"], pr["output"], pr["transform"]
        expr = tr if tr else f"{a}.{col}"
        alias_for_output = (out.replace(" ", "") or col)
        select_items.append({"output": out, "expression": expr, "alias": alias_for_output})

    # Track join condition per table to reuse if missing
    join_conditions_by_table: Dict[str, str] = {}
    for pr in parsed_rows:
        t = pr["table"]
        join_cond = pr.get("join", "").strip()
        if join_cond:
            join_conditions_by_table[t] = join_cond
        else:
            # Fill missing join with previous condition if exists
            if t in join_conditions_by_table:
                pr["join"] = join_conditions_by_table[t]


    joins = []
    joined_tables = set()
    
    for t, condition in join_conditions_by_table.items():
        if t == base_table or not t.strip() or t.lower() == "nan":
            continue  # skip base or invalid/nan tables
        # Parse both sides of the join condition
        (ltbl, lcol), (rtbl, rcol) = parse_join_condition_sides(condition)
        if not ltbl or not rtbl:
            continue  # Must have both tables!
        # Skip duplicate joins (optional, your logic)
        join_key = tuple(sorted([ltbl, rtbl]))
        if join_key in joined_tables:
            continue
        
        # Normalize alias_map keys once after building it
        alias_map = {k.strip().lower(): v for k, v in alias_map.items()}

        # Normalize keys when looking up
        ltbl_norm = ltbl.strip().lower()
        rtbl_norm = rtbl.strip().lower()

        lalias = alias_map.get(ltbl_norm, "")
        ralias = alias_map.get(rtbl_norm, "")


        join_clause = {
            "type": "LEFT",
            "left_table": f"{ltbl} {lalias}".strip(),
            "left_key": f"{lalias}.{lcol}" if lalias else lcol,
            "right_table": f"{rtbl} {ralias}".strip(),
            "right_key": f"{ralias}.{rcol}" if ralias else rcol,
            "condition": f"{lalias}.{lcol} = {ralias}.{rcol}" if lalias and ralias else condition,
        }
        joins.append(join_clause)
        joined_tables.add(join_key)
    
    
    return {
        "from": base_from,
        "select_items": select_items,
        "joins": joins,
        "message": "Inferred from mapping with reused join conditions",
    }

   
# =========================
# 5) Pattern-based SQL builder (kept unchanged)
# =========================
def _render_like(col: str, mode: str, value: str) -> str:
    if mode == "contains":    return f"{col} LIKE '%{value}%'"
    if mode == "starts_with": return f"{col} LIKE '{value}%'"
    if mode == "ends_with":   return f"{col} LIKE '%{value}'"
    if mode == "equals":      return f"{col} = '{value}'"
    raise HTTPException(status_code=400, detail="Unknown text_match mode")

def _render_rank_cte(base_table: str, base_alias: str, group_key: str, order_col: str, direction: str, cte_name: str) -> str:
    base = f"{base_table} {base_alias}".rstrip()
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
            return {**item, "expression": "COUNT()", "note": "conditional_count missing condition → COUNT(*)"}
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
    [
        f"{(it.get('expression') and str(it.get('expression')).strip() and str(it.get('expression')).lower() != 'nan' and it.get('expression')) or it.get('output')} AS [{it.get('output')}]"
        for it in applied
        if it.get("output")
    ]
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

from fastapi import Body

@router.post("/api/generate_sql_from_excel_mapping")
async def generate_sql_from_excel_mapping(body: Dict[str, Any] = Body(...)):
    """
    Accept JSON body with 'rows' key containing parsed mappings from Excel.
    Infer SQL structure and generate SQL query string.
    Return the generated SQL text without running it.
    """
    rows = body.get("rows")
    if not rows or not isinstance(rows, list):
        raise HTTPException(status_code=400, detail="Missing or invalid 'rows' in request body.")

    # Step 1: Infer SQL structure from mapping rows
    sql_structure = await infer_sql_structure({"rows": rows})

    # Compose payload for pattern-based SQL builder
    base_from = sql_structure.get("from", "").split()
    if len(base_from) >= 2:
        base_table = base_from[0]
        base_alias = base_from[1]
    else:
        base_table = base_from[0] if base_from else ""
        base_alias = ""

    payload = {
        "from": {"table": base_table, "alias": base_alias},
        "select_items": sql_structure.get("select_items", []),
        "joins": [f"{j['type']} JOIN {j['right_table']} ON {j['left_key']} = {j['right_key']}" for j in sql_structure.get("joins", [])],
        "patterns": {}
    }

    # Step 2: Generate SQL string (text only)
    sql_text = _build_sql_with_patterns(payload)

    return {"sql": sql_text}
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
