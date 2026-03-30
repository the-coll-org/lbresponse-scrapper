"""Decode Power BI DSR (DataShapeResult) response format.

The DSR encoding uses:
- ValueDicts: dictionaries of unique values for string/dict-encoded columns
- S (Schema row): first row defining column-to-dict mappings
- C: values — either dict indices (for dict-encoded cols) or literal values (for numeric/datetime)
- R: repeat bitmask — bit N means column N repeats from previous row
- Ø (U+00D8): null bitmask — bits count backward from last non-repeated column
- RT: restart tokens for pagination
"""

import logging

log = logging.getLogger(__name__)

NULL_MARKER = "\u00d8"


def extract_select_names(prototype_query: dict) -> list[str]:
    """Extract human-readable column names from a prototype query's Select list."""
    names: list[str] = []
    for sel in prototype_query.get("Select", []):
        name = sel.get("NativeReferenceName") or sel.get("Name", "")
        if not name:
            col = sel.get("Column", {})
            name = col.get("Property", "")
        if not name:
            agg = sel.get("Aggregation", {})
            expr = agg.get("Expression", {}).get("Column", {})
            name = expr.get("Property", f"col_{len(names)}")
        names.append(name)
    return names


def parse_dsr_response(
    response: dict, select_names: list[str] | None = None
) -> tuple[list[dict], list | None]:
    """Parse a querydata response, returning (rows_as_dicts, restart_tokens_or_none)."""
    results = response.get("results", [])
    if not results:
        return [], None

    result = results[0].get("result", {})
    data = result.get("data", {})
    dsr = data.get("dsr", {})

    ds_list = dsr.get("DS", [])
    if not ds_list:
        return [], None

    ds = ds_list[0]

    if "odata.error" in ds:
        log.error("Query error: %s", ds["odata.error"])
        return [], None

    value_dicts = ds.get("ValueDicts", {})
    ph = ds.get("PH", [])
    if not ph:
        return [], None

    data_rows = ph[0].get("DM0", [])
    if not data_rows:
        return [], None

    col_schema = None
    col_names: list[str] = []
    col_dict_names: list[str | None] = []
    col_types: list[int | None] = []

    rows: list[dict] = []
    prev_values: list = []

    for row in data_rows:
        if "S" in row:
            col_schema = row["S"]
            col_dict_names = []
            col_types = []
            num_schema_cols = len(col_schema)
            # Use select_names if provided and length matches, else fallback
            if select_names and len(select_names) == num_schema_cols:
                col_names = list(select_names)
            else:
                col_names = [f"col_{i}" for i in range(num_schema_cols)]
            for col_def in col_schema:
                col_dict_names.append(col_def.get("DN", None))
                col_types.append(col_def.get("T", None))
            prev_values = [None] * len(col_names)
            continue

        if col_schema is None:
            continue

        num_cols = len(col_names)
        repeat_mask = row.get("R", 0)
        null_mask = row.get(NULL_MARKER, 0)
        c_values = row.get("C", [])

        current_values = list(prev_values)

        non_repeated_indices = []
        for i in range(num_cols):
            if not (repeat_mask & (1 << i)):
                non_repeated_indices.append(i)

        null_indices = set()
        for bit_pos in range(len(non_repeated_indices)):
            if null_mask & (1 << bit_pos):
                idx = non_repeated_indices[-(bit_pos + 1)]
                null_indices.add(idx)

        c_idx = 0
        for i in non_repeated_indices:
            if i in null_indices:
                current_values[i] = None
            else:
                if c_idx < len(c_values):
                    raw_val = c_values[c_idx]
                    c_idx += 1
                    dn = col_dict_names[i]
                    if dn is not None and dn in value_dicts:
                        vd = value_dicts[dn]
                        if isinstance(raw_val, int) and 0 <= raw_val < len(vd):
                            current_values[i] = vd[raw_val]
                        else:
                            current_values[i] = raw_val
                    else:
                        current_values[i] = raw_val
                else:
                    current_values[i] = None

        prev_values = list(current_values)
        rows.append(dict(zip(col_names, current_values, strict=False)))

    restart_tokens = ds.get("RT", None)
    return rows, restart_tokens


def _extract_column_name(col_def: dict) -> str:
    """Best-effort extraction of a column name from a schema definition."""
    for key in ("GroupKeys", "Select"):
        items = col_def.get(key, [])
        for item in items:
            val = item
            for path in (
                ("Source", "Property"),
                ("Value", "Aggregation", "Expression", "Column", "Property"),
                ("Value", "Column", "Property"),
            ):
                v = val
                for p in path:
                    if isinstance(v, dict):
                        v = v.get(p)
                    else:
                        v = None
                        break
                if v:
                    return str(v)
    return ""
