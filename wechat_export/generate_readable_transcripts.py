import argparse
import json
from pathlib import Path

from export_named_contacts import CONTACT_SPECS, FIELD_SEPARATOR, _contact_folder, _safe_name
from review_speaker_assignments import collect_question_payload


def _selected_specs(targets=None):
    if not targets:
        return CONTACT_SPECS
    lowered = {target.casefold() for target in targets}
    return [
        spec for spec in CONTACT_SPECS
        if spec["name"].casefold() in lowered or spec["identifier"].casefold() in lowered
    ]


def _origin_path(spec):
    folder = _contact_folder(spec)
    return folder / f"{_safe_name(spec['name'])}_origin.txt"


def _readable_path(spec):
    folder = _contact_folder(spec)
    return folder / f"{_safe_name(spec['name'])}_readable.txt"


def _metadata_path(spec):
    return _contact_folder(spec) / "metadata.json"


def _parse_origin_line(line):
    parts = line.split(FIELD_SEPARATOR, 3)
    if len(parts) != 4:
        raise ValueError(f"origin 行格式不正确: {line}")
    return parts


def _render_readable(lines):
    if not lines:
        return []
    header = _parse_origin_line(lines[0])
    if header != ["时间戳", "说话人", "说话人id", "消息正文"]:
        raise ValueError(f"origin 表头不符合预期: {lines[0]}")

    readable_lines = []
    for line in lines[1:]:
        if not line.strip():
            continue
        timestamp, speaker, _speaker_id, content = _parse_origin_line(line)
        readable_lines.append(f"[{timestamp}] {speaker}: {content}")
    return readable_lines


def _update_metadata(spec, readable_name):
    path = _metadata_path(spec)
    if not path.exists():
        return
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return
    payload["readable_file"] = readable_name
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def generate_readable(targets=None):
    payload = collect_question_payload(targets)
    summary_by_name = {item["contact_name"]: item for item in payload["summary"]}
    generated = []
    skipped = []

    for spec in _selected_specs(targets):
        summary = summary_by_name.get(spec["name"])
        if not summary or summary.get("status") != "ok":
            skipped.append((spec["name"], "未找到联系人"))
            continue
        if int(summary.get("question_count", 0)) > 0:
            skipped.append((spec["name"], f"仍有 {summary['question_count']} 个未确认问题"))
            continue

        origin_path = _origin_path(spec)
        if not origin_path.exists():
            skipped.append((spec["name"], f"缺少 origin 文件: {origin_path.name}"))
            continue

        lines = origin_path.read_text(encoding="utf-8").splitlines()
        readable_lines = _render_readable(lines)
        readable_path = _readable_path(spec)
        readable_path.write_text("\n".join(readable_lines), encoding="utf-8")
        _update_metadata(spec, readable_path.name)
        generated.append((spec["name"], readable_path))

    for name, path in generated:
        print(f"[OK] {name}: {path}")
    for name, reason in skipped:
        print(f"[SKIP] {name}: {reason}")


def _parse_args():
    parser = argparse.ArgumentParser(description="在没有未确认说话人id时，从 origin 生成 readable 聊天记录")
    parser.add_argument(
        "--contact",
        dest="contacts",
        action="append",
        help="只处理指定联系人，可传姓名或 identifier；重复传参可处理多位",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    generate_readable(args.contacts)
