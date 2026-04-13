import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config import load_config
from paths import REPORTS_DIR, ensure_output_dirs


REPORT_DIR = REPORTS_DIR / "missing_message_db_ranges"
TEXT_REPORT = REPORT_DIR / "report.txt"
JSON_REPORT = REPORT_DIR / "report.json"
IGNORED_NAMES = {"message_fts.db", "message_resource.db"}


def _message_index(name):
    stem = Path(name).stem
    if not stem.startswith("message_"):
        return None
    suffix = stem.split("_", 1)[1]
    return int(suffix) if suffix.isdigit() else None


def _message_db_paths(directory):
    if not directory.exists():
        return {}
    result = {}
    for path in directory.glob("message_*.db"):
        if path.name in IGNORED_NAMES:
            continue
        index = _message_index(path.name)
        if index is None:
            continue
        result[index] = path
    return result


def _iter_msg_tables(conn):
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'Msg_%' ORDER BY name"
    ).fetchall()
    return [row[0] for row in rows]


def _format_ts(value):
    if value is None:
        return None
    return datetime.fromtimestamp(int(value)).strftime("%Y-%m-%d %H:%M:%S")


def _db_span(path):
    conn = sqlite3.connect(str(path))
    try:
        tables = _iter_msg_tables(conn)
        global_min = None
        global_max = None
        total_rows = 0
        per_table = []
        cur = conn.cursor()
        for table in tables:
            min_ts, max_ts, row_count = cur.execute(
                f"SELECT MIN(create_time), MAX(create_time), COUNT(*) FROM [{table}]"
            ).fetchone()
            row_count = int(row_count or 0)
            if row_count == 0:
                continue
            total_rows += row_count
            global_min = min(global_min, int(min_ts)) if global_min is not None else int(min_ts)
            global_max = max(global_max, int(max_ts)) if global_max is not None else int(max_ts)
            per_table.append(
                {
                    "table": table,
                    "min_create_time": int(min_ts),
                    "max_create_time": int(max_ts),
                    "min_create_time_text": _format_ts(min_ts),
                    "max_create_time_text": _format_ts(max_ts),
                    "row_count": row_count,
                }
            )
        return {
            "path": str(path),
            "db": path.name,
            "db_index": _message_index(path.name),
            "row_count": total_rows,
            "table_count": len(per_table),
            "min_create_time": global_min,
            "max_create_time": global_max,
            "min_create_time_text": _format_ts(global_min),
            "max_create_time_text": _format_ts(global_max),
            "tables": per_table,
        }
    finally:
        conn.close()


def _normalize_missing(values):
    normalized = set()
    for value in values or []:
        raw = str(value).strip()
        if not raw:
            continue
        if raw.isdigit():
            normalized.add(int(raw))
            continue
        index = _message_index(raw if raw.endswith(".db") else f"{raw}.db")
        if index is None:
            raise ValueError(f"无法识别缺失分库标识: {value}")
        normalized.add(index)
    return sorted(normalized)


def _neighbor_span(spans, missing_index, direction):
    indexes = sorted(spans)
    if direction == "newer":
        candidates = [index for index in indexes if index < missing_index and spans[index]["min_create_time"] is not None]
        return spans[candidates[-1]] if candidates else None
    candidates = [index for index in indexes if index > missing_index and spans[index]["max_create_time"] is not None]
    return spans[candidates[0]] if candidates else None


def _estimate_missing_range(spans, missing_index):
    newer_span = _neighbor_span(spans, missing_index, "newer")
    older_span = _neighbor_span(spans, missing_index, "older")

    if older_span and newer_span:
        estimate_type = "between_neighbors"
        estimate_text = (
            f"大概率覆盖 {older_span['max_create_time_text']} 到 {newer_span['min_create_time_text']} 这段聊天"
        )
        suggestion = (
            f"请在微信里优先打开 {older_span['max_create_time_text']} 到 {newer_span['min_create_time_text']} "
            "附近的老聊天，再重新提 key。"
        )
    elif older_span:
        estimate_type = "after_older_neighbor"
        estimate_text = f"大概率覆盖晚于 {older_span['max_create_time_text']} 的历史消息"
        suggestion = f"请在微信里优先打开 {older_span['max_create_time_text']} 之后那段时间的老聊天，再重新提 key。"
    elif newer_span:
        estimate_type = "before_newer_neighbor"
        estimate_text = f"大概率覆盖早于 {newer_span['min_create_time_text']} 的历史消息"
        suggestion = f"请在微信里优先打开 {newer_span['min_create_time_text']} 之前那段时间的老聊天，再重新提 key。"
    else:
        estimate_type = "unknown"
        estimate_text = "当前缺少足够的相邻已解密分库，暂时无法估出可靠时间段"
        suggestion = "请先补齐更多相邻分库，或直接去微信里翻更早/更晚的历史聊天后再提 key。"

    return {
        "db": f"message_{missing_index}.db",
        "db_index": missing_index,
        "estimate_type": estimate_type,
        "estimate_text": estimate_text,
        "suggestion": suggestion,
        "older_neighbor": {
            "db": older_span["db"],
            "max_create_time_text": older_span["max_create_time_text"],
        } if older_span else None,
        "newer_neighbor": {
            "db": newer_span["db"],
            "min_create_time_text": newer_span["min_create_time_text"],
        } if newer_span else None,
    }


def build_report(missing_indexes=None):
    ensure_output_dirs()
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    cfg = load_config()
    source_message_dir = Path(cfg["db_dir"]) / "message"
    decrypted_message_dir = Path(cfg["decrypted_dir"]) / "message"

    source_paths = _message_db_paths(source_message_dir)
    decrypted_paths = _message_db_paths(decrypted_message_dir)
    spans = {index: _db_span(path) for index, path in sorted(decrypted_paths.items())}

    if missing_indexes is None:
        missing_indexes = sorted(index for index in source_paths if index not in decrypted_paths)

    estimates = [_estimate_missing_range(spans, index) for index in missing_indexes]

    payload = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_message_dir": str(source_message_dir),
        "decrypted_message_dir": str(decrypted_message_dir),
        "source_message_db_indexes": sorted(source_paths),
        "decrypted_message_db_indexes": sorted(decrypted_paths),
        "missing_message_db_indexes": missing_indexes,
        "decrypted_db_spans": [spans[index] for index in sorted(spans)],
        "missing_estimates": estimates,
    }
    JSON_REPORT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# 缺失 message 分库时间段报告",
        f"生成时间: {payload['generated_at']}",
        f"源 message 目录: {source_message_dir}",
        f"已解密 message 目录: {decrypted_message_dir}",
        f"源分库编号: {payload['source_message_db_indexes']}",
        f"已解密分库编号: {payload['decrypted_message_db_indexes']}",
        f"缺失分库编号: {missing_indexes}",
        "",
        "## 已解密分库时间跨度",
    ]
    for index in sorted(spans):
        span = spans[index]
        lines.append(
            f"- {span['db']}: {span['min_create_time_text']} -> {span['max_create_time_text']} | "
            f"消息 {span['row_count']} 条 | 表 {span['table_count']} 个"
        )

    lines.append("")
    lines.append("## 缺失分库估算")
    if not estimates:
        lines.append("- 当前未发现缺失的 message_x.db。")
    for estimate in estimates:
        lines.append(f"- {estimate['db']}: {estimate['estimate_text']}")
        if estimate["older_neighbor"]:
            lines.append(
                f"  更老邻居: {estimate['older_neighbor']['db']} | 最晚消息 {estimate['older_neighbor']['max_create_time_text']}"
            )
        if estimate["newer_neighbor"]:
            lines.append(
                f"  更新邻居: {estimate['newer_neighbor']['db']} | 最早消息 {estimate['newer_neighbor']['min_create_time_text']}"
            )
        lines.append(f"  建议: {estimate['suggestion']}")
    TEXT_REPORT.write_text("\n".join(lines), encoding="utf-8")

    print(f"JSON: {JSON_REPORT}")
    print(f"文本: {TEXT_REPORT}")
    if estimates:
        for estimate in estimates:
            print(f"{estimate['db']}: {estimate['estimate_text']}")
    else:
        print("当前未发现缺失的 message_x.db。")


def _parse_args():
    parser = argparse.ArgumentParser(description="估算缺失 message_x.db 大概覆盖的时间段")
    parser.add_argument(
        "--missing",
        dest="missing",
        action="append",
        help="手动指定缺失分库，可传 9、message_9、message_9.db；重复传参可指定多个",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    build_report(_normalize_missing(args.missing) if args.missing else None)
