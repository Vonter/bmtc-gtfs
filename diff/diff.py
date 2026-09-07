#!/usr/bin/env python3
"""Diff HEAD's GTFS against the working feed, using only the standard library."""
import argparse
from collections import Counter
from contextlib import closing
import csv
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parents[1]
VERSION = "2.0.0-rc1"
SPEC = "https://github.com/MobilityData/gtfs-diff/blob/main/spec/v2/specification.md"
# None: a relation without a unique ID, matched by its complete row.
KEYS = {
    "agency.txt": ("agency_id",), "stops.txt": ("stop_id",),
    "routes.txt": ("route_id",), "trips.txt": ("trip_id",),
    "stop_times.txt": ("trip_id", "stop_sequence"),
    "calendar.txt": ("service_id",), "calendar_dates.txt": ("service_id", "date"),
    "fare_attributes.txt": ("fare_id",), "fare_rules.txt": None,
    "shapes.txt": ("shape_id", "shape_pt_sequence"),
    "frequencies.txt": ("trip_id", "start_time"), "transfers.txt": None,
    "pathways.txt": ("pathway_id",), "levels.txt": ("level_id",),
    "feed_info.txt": ("feed_publisher_name",),
    "translations.txt": ("table_name", "field_name", "language", "record_id", "record_sub_id", "field_value"),
    "attributions.txt": ("attribution_id",), "areas.txt": ("area_id",),
    "stop_areas.txt": ("area_id", "stop_id"), "networks.txt": ("network_id",),
    "route_networks.txt": ("route_id",), "fare_media.txt": ("fare_media_id",),
    "fare_products.txt": ("fare_product_id", "rider_category_id", "fare_media_id"),
    "fare_leg_rules.txt": None, "fare_leg_join_rules.txt": None,
    "fare_transfer_rules.txt": None, "timeframes.txt": None,
    "rider_categories.txt": ("rider_category_id",),
    "location_groups.txt": ("location_group_id",),
    "location_group_stops.txt": ("location_group_id", "stop_id"),
    "booking_rules.txt": ("booking_rule_id",),
}
OPTIONAL_KEYS = {
    "translations.txt": {"record_id", "record_sub_id", "field_value"},
    "fare_products.txt": {"rider_category_id", "fare_media_id"},
}


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def csv_value(values):
    out = io.StringIO(newline="")
    csv.writer(out, lineterminator="\r\n").writerow(values)
    return out.getvalue()[:-2]


def members(archive):
    names = [i.filename for i in archive.infolist() if not i.is_dir()]
    if len(names) != len(set(names)):
        raise ValueError("Duplicate ZIP member names cannot be compared reliably")
    return set(names)


def header(archive, name):
    if name not in archive.namelist():
        return []
    with archive.open(name) as raw:
        fields = next(csv.reader(io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")), [])
    if not fields or any(not f for f in fields) or len(fields) != len(set(fields)):
        raise ValueError(f"{name}: missing, empty, or duplicate CSV column names")
    return fields


def rows(archive, name):
    """Yield starting physical CSV line numbers, including multiline records."""
    if name not in archive.namelist():
        return
    with archive.open(name) as raw:
        reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8-sig", newline=""))
        fields = next(reader)
        while True:
            line = reader.line_num + 1
            values = next(reader, None)
            if values is None:
                break
            if not values:
                continue
            if len(values) != len(fields):
                raise ValueError(f"{name}:{line}: expected {len(fields)} fields, got {len(values)}")
            yield line, dict(zip(fields, values))


def primary_key(name, columns):
    key = KEYS[name]
    if key is None:
        return columns
    if name == "agency.txt" and "agency_id" not in columns:
        return ["agency_name"]
    if name == "attributions.txt" and "attribution_id" not in columns:
        return columns
    return [k for k in key if k not in OPTIONAL_KEYS.get(name, set()) or k in columns]


def index_rows(db, table, archive, name, key, columns, common):
    db.execute(f"CREATE TABLE {table} (key TEXT PRIMARY KEY, line INTEGER, value TEXT, compared TEXT)")
    count, batch = 0, []
    for line, row in rows(archive, name):
        count += 1
        batch.append((encode([row.get(k, "") for k in key]), line,
                      encode([row.get(c, "") for c in columns]), encode([row[c] for c in common])))
        if len(batch) == 5000:
            db.executemany(f"INSERT OR IGNORE INTO {table} VALUES (?,?,?,?)", batch)
            batch.clear()
    db.executemany(f"INSERT OR IGNORE INTO {table} VALUES (?,?,?,?)", batch)
    db.commit()
    unique = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    return count, unique != count


def compare_file(base, new, name, db_path, cap):
    old_cols, new_cols = header(base, name), header(new, name)
    columns = old_cols + [c for c in new_cols if c not in old_cols]
    common = [c for c in old_cols if c in new_cols]
    added = [{"name": c, "position": i + 1} for i, c in enumerate(new_cols) if old_cols and c not in old_cols]
    deleted = [{"name": c, "position": i + 1} for i, c in enumerate(old_cols) if new_cols and c not in new_cols]
    action = "modified" if old_cols and new_cols else ("added" if new_cols else "deleted")
    key = primary_key(name, (common or columns) if KEYS[name] is None else columns)
    stats = {"total_rows_base": 0, "total_rows_new": 0,
             "columns_added_count": len(added), "columns_deleted_count": len(deleted)}
    result = {"file_name": name, "file_action": action, "columns_added": added,
              "columns_deleted": deleted, "stats": stats}
    missing = [k for k in key if (old_cols and k not in old_cols) or (new_cols and k not in new_cols)]
    if missing and KEYS[name] is not None:
        result.update(file_action="not_compared", not_compared_reason={
            "code": "missing_primary_key", "message": "Missing key columns: " + ", ".join(missing)})
        stats["total_rows_base"] = sum(1 for _ in rows(base, name))
        stats["total_rows_new"] = sum(1 for _ in rows(new, name))
        return result, stats
    with closing(sqlite3.connect(db_path)) as db:
        db.execute("PRAGMA journal_mode=OFF")
        db.execute("PRAGMA synchronous=OFF")
        db.execute("PRAGMA cache_size=-65536")
        for table in ("base", "new"):
            db.execute(f"DROP TABLE IF EXISTS {table}")
        old_count, old_duplicate = index_rows(db, "base", base, name, key, columns, common)
        new_count, new_duplicate = index_rows(db, "new", new, name, key, columns, common)
        stats.update(total_rows_base=old_count, total_rows_new=new_count)
        if old_duplicate or new_duplicate:
            result.update(file_action="not_compared", not_compared_reason={
                "code": "duplicate_primary_key",
                "message": "Duplicate row identities prevent unambiguous matching: " + ", ".join(key)})
            return result, stats
        if action != "modified":
            # A whole-file addition/deletion counts once; retain its row totals.
            return result, stats
        changes = {"primary_key": key, "columns": columns, "added": [], "deleted": [], "modified": []}
        counts, field_counts, kept = Counter(), Counter(), 0
        # Encounter order: new source records, then deleted base records.
        query = """SELECT n.key, b.line, n.line, b.value, n.value
                   FROM new n LEFT JOIN base b ON n.key=b.key
                   WHERE b.key IS NULL OR b.compared != n.compared ORDER BY n.rowid"""
        common_set = set(common)
        for raw_key, old_line, new_line, old_value, new_value in db.execute(query):
            kind = "added" if old_line is None else "modified"
            counts[kind] += 1
            fields = []
            if kind == "modified":
                old_values, new_values = json.loads(old_value), json.loads(new_value)
                for i, column in enumerate(columns):
                    if column in common_set and old_values[i] != new_values[i]:
                        field_counts[column] += 1
                        if kept < cap:
                            fields.append({"field": column, "base_value": old_values[i], "new_value": new_values[i]})
            if kept < cap:
                change = {"identifier": dict(zip(key, json.loads(raw_key))),
                          "raw_value": csv_value(json.loads(new_value if kind == "added" else old_value)),
                          "new_line_number": new_line}
                if kind == "modified":
                    change.update(base_line_number=old_line, field_changes=fields)
                changes[kind].append(change)
                kept += 1
        query = """SELECT b.key, b.line, b.value FROM base b LEFT JOIN new n ON b.key=n.key
                   WHERE n.key IS NULL ORDER BY b.rowid"""
        for raw_key, line, value in db.execute(query):
            counts["deleted"] += 1
            if kept < cap:
                changes["deleted"].append({"identifier": dict(zip(key, json.loads(raw_key))),
                                           "raw_value": csv_value(json.loads(value)), "base_line_number": line})
                kept += 1
        for kind in ("added", "deleted", "modified"):
            stats[f"rows_{kind}_count"] = counts[kind]
        stats["column_stats"] = [{"column": c, "modifications_count": field_counts[c],
                                  "modifications_percentage": round(100 * field_counts[c] / counts["modified"], 2)}
                                 for c in columns if field_counts[c]]
        changed = sum(counts.values())
        if not changed and not added and not deleted:
            return None, stats
        result["row_changes"] = changes
        if changed > kept:
            result["truncated"] = {"is_truncated": True, "omitted_count": changed - kept}
        return result, stats


def build_diff(base_path, new_path, base_source, new_source, workdir, cap=50):
    output = {"metadata": {"schema_version": VERSION, "generated_at": now(),
                           "row_changes_cap_per_file": cap, "base_feed": base_source,
                           "new_feed": new_source, "unsupported_files": []},
              "summary": {"total_changes": 0, "files_added_count": 0, "files_deleted_count": 0,
                          "files_modified_count": 0, "files_not_compared_count": 0, "files": []},
              "file_diffs": []}
    totals = {}
    with zipfile.ZipFile(base_path) as base, zipfile.ZipFile(new_path) as new:
        old_names, new_names = members(base), members(new)
        for name in sorted(old_names | new_names):
            if name not in KEYS:
                output["metadata"]["unsupported_files"].append({
                    "file_name": name, "present_in": "both" if name in old_names & new_names
                    else ("base" if name in old_names else "new")})
                continue
            print(f"Comparing {name}...", file=sys.stderr, flush=True)
            diff, stats = compare_file(base, new, name, Path(workdir) / "rows.sqlite", cap)
            totals[name] = stats
            if diff is None:
                continue
            output["file_diffs"].append(diff)
            status, summary = diff["file_action"], output["summary"]
            summary[f"files_{status}_count"] += 1
            summary["files"].append({"file_name": name, "status": status})
            if status in ("added", "deleted"):
                summary["total_changes"] += 1
            elif status == "modified":
                summary["total_changes"] += sum(stats.get(f"rows_{k}_count", 0) for k in ("added", "deleted", "modified"))
                summary["total_changes"] += stats["columns_added_count"] + stats["columns_deleted_count"]
    return output, totals


def small_table(path, name, key):
    with zipfile.ZipFile(path) as archive:
        return {row.get(key, ""): row for _, row in rows(archive, name)}


def escape(value):
    return str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace(
        "|", "\\|").replace(chr(96), "&#96;").replace("\r", " ").replace("\n", " ")


def code(value):
    return chr(96) + escape(value) + chr(96)


def signed(value):
    return f"{value:+,}" if value else "0"


def display_source(source):
    if source.startswith("git:"):
        _, commit, path = source.split(":", 2)
        return f"{commit[:12]}:{path}"
    try:
        return str(Path(source).relative_to(ROOT))
    except ValueError:
        return source


def display_date(value):
    if value and len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:]}"
    return escape(value or "—")


def route_names(routes):
    return {rid: row.get("route_short_name") or row.get("route_long_name") or rid
            for rid, row in routes.items()}


def markdown(output, totals, base_path, new_path, refresh=None, json_name="diff.json"):
    meta, summary = output["metadata"], output["summary"]
    old_routes, new_routes = [small_table(p, "routes.txt", "route_id") for p in (base_path, new_path)]
    old_trips, new_trips = [small_table(p, "trips.txt", "trip_id") for p in (base_path, new_path)]
    info = [next(iter(small_table(p, "feed_info.txt", "feed_publisher_name").values()), {}) for p in (base_path, new_path)]
    old_info, new_info = info
    lines = ["# GTFS changes", "", f"Generated: {meta['generated_at']}", "",
             f"**Comparison:** {code(display_source(meta['base_feed']['source']))} → {code(display_source(meta['new_feed']['source']))}.",
             f"**Feed version:** {escape(old_info.get('feed_version', '—'))} → {escape(new_info.get('feed_version', '—'))}. "
             f"**Validity:** {display_date(old_info.get('feed_start_date'))}–{display_date(old_info.get('feed_end_date'))} → "
             f"{display_date(new_info.get('feed_start_date'))}–{display_date(new_info.get('feed_end_date'))}.", ""]
    if refresh and refresh.get("full_refresh_complete") is False:
        premium = refresh.get("premium_fares", {})
        lines += [f"> **Snapshot caveat:** premium-fare collection is incomplete "
                  f"({premium.get('snapshot_processed_pairs', 0):,}/{premium.get('total_station_pairs', 0):,} pairs processed). "
                  "Fare-rule reductions include missing coverage and must not be interpreted as fare cuts.", ""]
    lines += ["## File changes", "", "| File | Before | After | Net | Added | Deleted | Modified |",
              "|---|---:|---:|---:|---:|---:|---:|"]
    by_name = {d["file_name"]: d for d in output["file_diffs"]}
    for name, stats in totals.items():
        before, after = stats["total_rows_base"], stats["total_rows_new"]
        status = by_name.get(name, {}).get("file_action", "unchanged")
        cells = [f"{stats.get(f'rows_{k}_count', 0):,}" for k in ("added", "deleted", "modified")]
        if status in ("added", "deleted", "not_compared"):
            cells = ["—"] * 3
        label = code(name) + (f" ({status.replace('_', ' ')})" if status != "modified" else "")
        lines.append(f"| {label} | {before:,} | {after:,} | {signed(after-before)} | {' | '.join(cells)} |")
    lines += ["", f"**Total:** {summary['total_changes']:,} row/column/file changes; "
              f"{summary['files_added_count']} files added, {summary['files_deleted_count']} deleted, "
              f"{summary['files_modified_count']} modified, {summary['files_not_compared_count']} not compared.", ""]
    shared = old_trips.keys() & new_trips.keys()
    reused = sum((old_trips[k].get("route_id"), old_trips[k].get("direction_id")) !=
                 (new_trips[k].get("route_id"), new_trips[k].get("direction_id")) for k in shared)
    if reused:
        lines += [f"**Trip-ID caveat:** {reused:,}/{len(shared):,} shared trip IDs now refer to a different route/direction. "
                  "Trip and stop-time modifications are literal ID-based changes, not counts of changed or cancelled services. "
                  "Shape-point changes also include geometry resampling and sequence shifts.", ""]
    lines += ["## Route service changes", ""]
    old_names, new_names = route_names(old_routes), route_names(new_routes)
    old_name_set, new_name_set = set(old_names.values()), set(new_names.values())
    for label, names in [("Added", new_name_set - old_name_set), ("Removed", old_name_set - new_name_set)]:
        labels = sorted(names)
        shown = ", ".join(escape(x) for x in labels[:12]) or "None"
        suffix = f"; +{len(labels)-12} more" if len(labels) > 12 else ""
        lines.append(f"- **{label} route names ({len(labels):,}):** {shown}{suffix}.")
    old_ids, new_ids = {}, {}
    for target, names in ((old_ids, old_names), (new_ids, new_names)):
        for rid, name in names.items():
            target.setdefault(name, set()).add(rid)
    reassigned = sum(old_ids[name] != new_ids[name] for name in old_name_set & new_name_set)
    if reassigned:
        lines.append(f"- **ID reassignment:** {reassigned:,} retained route names have different route IDs.")
    old_counts = Counter(old_names.get(t.get("route_id", ""), t.get("route_id", "")) for t in old_trips.values())
    new_counts = Counter(new_names.get(t.get("route_id", ""), t.get("route_id", "")) for t in new_trips.values())
    changes = [(rid, old_counts[rid], new_counts[rid]) for rid in old_counts.keys() | new_counts.keys()
               if old_counts[rid] != new_counts[rid]]
    changes.sort(key=lambda item: (-abs(item[2] - item[1]), item[0]))
    lines += ["", "Largest changes in scheduled trip records, grouped by route name; directions and duplicate names combined. Renames are not inferred.", "",
              "| Route | Before | After | Net |", "|---|---:|---:|---:|"]
    for rid, old, new in changes[:8]:
        lines.append(f"| {escape(rid)} | {old:,} | {new:,} | {signed(new-old)} |")
    if not changes:
        lines.append("| No changes | — | — | 0 |")
    lines += ["", "## Notable field changes", ""]
    details = 0
    for name in ("stops.txt", "routes.txt", "fare_attributes.txt", "translations.txt"):
        columns = sorted(totals.get(name, {}).get("column_stats", []), key=lambda c: (-c["modifications_count"], c["column"]))
        if columns:
            lines.append(f"- **{name}:** " + "; ".join(f"{escape(c['column'])} {c['modifications_count']:,}" for c in columns[:4]) + " modified rows.")
            details += 1
    if not details:
        lines.append("No field changes in stops, routes, fare attributes, or translations.")
    for diff in output["file_diffs"]:
        if diff["file_action"] == "not_compared":
            lines.append(f"- **Not compared — {escape(diff['file_name'])}:** {escape(diff['not_compared_reason']['message'])}.")
        for action in ("added", "deleted"):
            if diff.get(f"columns_{action}"):
                names = ", ".join(escape(c["name"]) for c in diff[f"columns_{action}"])
                lines.append(f"- **{escape(diff['file_name'])}:** columns {action}: {names}.")
    if meta["unsupported_files"]:
        lines += ["", "**Outside schema scope:** " + ", ".join(
            f"{escape(f['file_name'])} ({f['present_in']})" for f in meta["unsupported_files"]) + "."]
    lines += ["", "## Reproduce and interpret", "", chr(96)*3 + "sh", "python3 diff/diff.py", chr(96)*3, "",
              f"[GTFS Diff {VERSION}]({SPEC}) JSON: {code(json_name)}. Full counts; at most "
              f"{meta['row_changes_cap_per_file']} row examples per file across all actions. "
              "Rows match declared keys; keyless relations use complete rows. CSV row/column ordering and ZIP metadata are ignored. "
              "Column-only changes count once, without inflating modified-row totals. "
              "Whole-file additions/deletions count once; their row totals appear in Before/After. "
              "Duplicate or missing keys are reported as not compared. Values are compared as strings.", "",
              "Snapshots are copied before comparison; downloaded_at records local snapshot capture time, not an upstream download time.", "",
              f"- Base SHA-256: {code(meta['base_feed']['sha256'])}",
              f"- New SHA-256: {code(meta['new_feed']['sha256'])}", ""]
    return "\n".join(lines)


def source_metadata(path, source):
    with path.open("rb") as f:
        digest = hashlib.file_digest(f, "sha256").hexdigest()
    return {"source": source, "downloaded_at": now(), "sha256": digest}


def write_atomic(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as f:
        temporary = Path(f.name)
        f.write(text)
    try:
        temporary.chmod(0o644)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    baseline = parser.add_mutually_exclusive_group()
    baseline.add_argument("--base-ref", default="HEAD", help="Old feed's Git commit/ref (default: HEAD)")
    baseline.add_argument("--base-feed", type=Path, help="Use a local ZIP instead of Git")
    parser.add_argument("--git-path", default="gtfs/bmtc.zip", help="Feed path inside the baseline commit")
    parser.add_argument("--feed", type=Path, default=ROOT / "gtfs/bmtc.zip", help="New GTFS ZIP")
    parser.add_argument("--markdown", type=Path, default=ROOT / "diff/diff.md")
    parser.add_argument("--json", dest="json_path", type=Path, default=ROOT / "diff/diff.json")
    parser.add_argument("--max-row-changes", type=int, choices=range(51), default=50, metavar="0..50")
    args = parser.parse_args(argv)
    inputs = {args.feed.resolve()}
    if args.base_feed:
        inputs.add(args.base_feed.resolve())
    outputs = {args.markdown.resolve(), args.json_path.resolve()}
    if len(outputs) != 2 or inputs & outputs:
        parser.error("Output paths must be distinct and must not overwrite input feeds")
    try:
        with tempfile.TemporaryDirectory(prefix="gtfs-diff-") as workdir:
            base_path, new_path = Path(workdir) / "base.zip", Path(workdir) / "new.zip"
            if args.base_feed:
                shutil.copyfile(args.base_feed, base_path)
                base_label = str(args.base_feed.resolve())
            else:
                commit = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "--verify", "--end-of-options",
                                                  f"{args.base_ref}^{{commit}}"], text=True).strip()
                base_label = f"git:{commit}:{args.git_path}"
                with base_path.open("wb") as f:
                    subprocess.run(["git", "-C", str(ROOT), "show", f"{commit}:{args.git_path}"], stdout=f, check=True)
            shutil.copyfile(args.feed, new_path)
            base_source = source_metadata(base_path, base_label)
            new_source = source_metadata(new_path, str(args.feed.resolve()))
            output, totals = build_diff(base_path, new_path, base_source, new_source, workdir, args.max_row_changes)
            refresh = None
            summary_path = ROOT / "raw/refresh-summary.json"
            if args.feed.resolve() == ROOT / "gtfs/bmtc.zip" and summary_path.exists():
                candidate = json.loads(summary_path.read_text())
                if candidate.get("counts") and all(totals.get(f"{k}.txt", {}).get("total_rows_new") == v
                                                    for k, v in candidate["counts"].items()):
                    refresh = candidate
            report = markdown(output, totals, base_path, new_path, refresh, args.json_path.name)
            write_atomic(args.json_path, json.dumps(output, ensure_ascii=False, indent=2) + "\n")
            write_atomic(args.markdown, report)
        print(f"Wrote {args.markdown} and {args.json_path}")
    except (OSError, ValueError, csv.Error, sqlite3.Error, zipfile.BadZipFile, subprocess.CalledProcessError) as exc:
        parser.exit(1, f"GTFS diff failed: {exc}\n")


if __name__ == "__main__":
    main()
