import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

from export_named_contacts import (
    CONTACT_SPECS,
    _base_type,
    _build_sender_anchor_map,
    _db_index_from_name,
    _decode_message_content,
    _format_message,
    _iter_contact_rows,
    _manual_assignments_for_contact,
    _parse_voice_info,
    _resolve_contact,
    _safe_name,
    _speaker_for_row,
    _speaker_id_from_sender_id,
)
from paths import DECRYPTED_DIR, MESSAGE_DIR, REPORTS_DIR, ensure_output_dirs


REVIEW_DIR = REPORTS_DIR / "speaker_assignment_review"
QUESTIONS_JSON = REVIEW_DIR / "questions.json"
QUESTIONS_TXT = REVIEW_DIR / "questions.txt"


def _truncate(text, limit=120):
    text = str(text).replace("\n", " / ").strip()
    return text if len(text) <= limit else f"{text[:limit - 1]}…"


def _case_for_db(contact_name, identifier, username, db_name, rows, manual_assignments):
    counterpart_label = "她"
    sender_stats, sender_map, anchor_conflicts = _build_sender_anchor_map(rows, counterpart_label)

    predicted_rows = []
    side_to_ids = defaultdict(set)
    fallback_ids = set()

    for row in rows:
        base_type = _base_type(row[1])
        text = _decode_message_content(row[6], row[7])
        voice_meta = _parse_voice_info(text) if base_type == 34 else {}
        content = _format_message(base_type, text, voice_meta, None)
        speaker, method = _speaker_for_row(db_name, row, counterpart_label, sender_map, manual_assignments)
        sender_id = row[3]
        predicted_rows.append(
            {
                "row": row,
                "speaker": speaker,
                "method": method,
                "content": content,
            }
        )
        if sender_id is not None:
            side_to_ids[speaker].add(int(sender_id))
            if method == "server_seq":
                fallback_ids.add(int(sender_id))

    reasons = []
    candidate_ids = set()
    confirmed_ids = {
        int(sender_id)
        for sender_id in sender_stats
        if _speaker_id_from_sender_id(db_name, sender_id) in manual_assignments
    }
    unresolved_my_ids = sorted(sender_id for sender_id in side_to_ids.get("我", set()) if sender_id not in confirmed_ids)
    unresolved_other_ids = sorted(sender_id for sender_id in side_to_ids.get(counterpart_label, set()) if sender_id not in confirmed_ids)
    unresolved_single_ids = [sender_id for sender_id in sorted(sender_stats) if sender_id not in confirmed_ids]

    if len(unresolved_my_ids) > 1:
        ids = unresolved_my_ids
        candidate_ids.update(ids)
        reasons.append(
            {
                "type": "same_db_same_side_multiple_ids",
                "side": "我",
                "speaker_ids": [_speaker_id_from_sender_id(db_name, sender_id) for sender_id in ids],
                "message": f"同一个 DB 内，“我”侧出现多个说话人id: {[_speaker_id_from_sender_id(db_name, sender_id) for sender_id in ids]}",
            }
        )

    if len(unresolved_other_ids) > 1:
        ids = unresolved_other_ids
        candidate_ids.update(ids)
        reasons.append(
            {
                "type": "same_db_same_side_multiple_ids",
                "side": counterpart_label,
                "speaker_ids": [_speaker_id_from_sender_id(db_name, sender_id) for sender_id in ids],
                "message": f"同一个 DB 内，“{counterpart_label}”侧出现多个说话人id: {[_speaker_id_from_sender_id(db_name, sender_id) for sender_id in ids]}",
            }
        )

    all_sender_ids = sorted(sender_stats)
    if len(all_sender_ids) == 1 and unresolved_single_ids:
        candidate_ids.update(unresolved_single_ids)
        reasons.append(
            {
                "type": "same_db_only_one_id",
                "speaker_ids": [_speaker_id_from_sender_id(db_name, sender_id) for sender_id in unresolved_single_ids],
                "message": f"同一个 DB 内只出现一个说话人id: {[_speaker_id_from_sender_id(db_name, sender_id) for sender_id in unresolved_single_ids]}",
            }
        )

    if not reasons:
        return None

    candidate_ids.update(fallback_ids)
    candidate_ids.update(anchor_conflicts)
    candidate_ids = sorted(candidate_ids)

    entries = []
    for sender_id in candidate_ids:
        speaker_id = _speaker_id_from_sender_id(db_name, sender_id)
        sender_rows = [item for item in predicted_rows if item["row"][3] == sender_id]
        if not sender_rows:
            continue
        predicted_counter = Counter(item["speaker"] for item in sender_rows)
        predicted_speaker = predicted_counter.most_common(1)[0][0]
        methods = Counter(item["method"] for item in sender_rows)
        stats = sender_stats.get(sender_id, {"count": 0, "status": Counter(), "server_seq": Counter()})
        samples = []
        for item in sender_rows[:5]:
            row = item["row"]
            samples.append(
                {
                    "time": datetime.fromtimestamp(row[2]).strftime("%Y-%m-%d %H:%M:%S"),
                    "predicted_speaker": item["speaker"],
                    "content": _truncate(item["content"]),
                }
            )
        entries.append(
            {
                "speaker_id": speaker_id,
                "predicted_speaker": predicted_speaker,
                "message_count": stats["count"],
                "status": dict(sorted(stats["status"].items())),
                "server_seq": dict(stats["server_seq"]),
                "methods": dict(methods),
                "samples": samples,
            }
        )

    question_lines = [
        f"联系人: {contact_name}",
        f"identifier: {identifier}",
        f"username: {username}",
        f"数据库: {db_name}",
        "请回复这些说话人id分别属于“我”“她”还是“系统”:",
    ]
    for entry in entries:
        question_lines.append(f"- {entry['speaker_id']} => 我/她/系统")

    return {
        "contact_name": contact_name,
        "identifier": identifier,
        "username": username,
        "db": db_name,
        "db_index": _db_index_from_name(db_name),
        "reasons": reasons,
        "entries": entries,
        "reply_template": [f"{entry['speaker_id']}=我/她/系统" for entry in entries],
        "question_lines": question_lines,
    }


def _render_case_text(index, case):
    lines = [
        f"## Q{index:03d} | {case['contact_name']} | {case['db']}",
    ]
    for reason in case["reasons"]:
        lines.append(f"- {reason['message']}")
    lines.append("建议你直接按下面格式回复:")
    for template in case["reply_template"]:
        lines.append(f"- {template}")
    lines.append("")
    for entry in case["entries"]:
        lines.append(
            "说话人id {speaker_id} | 当前预测 {speaker} | 消息 {count} 条 | "
            "status {status} | server_seq {server_seq} | 判定来源 {methods}".format(
                speaker_id=entry["speaker_id"],
                speaker=entry["predicted_speaker"],
                count=entry["message_count"],
                status=entry["status"],
                server_seq=entry["server_seq"],
                methods=entry["methods"],
            )
        )
        lines.append("样本:")
        for sample in entry["samples"]:
            lines.append(
                f"- {sample['time']} | {sample['predicted_speaker']} | {sample['content']}"
            )
        lines.append("")
    return lines


def collect_question_payload(targets=None):
    ensure_output_dirs()
    REVIEW_DIR.mkdir(parents=True, exist_ok=True)

    msg_paths = sorted(
        path for path in MESSAGE_DIR.glob("message_*.db")
        if path.name not in {"message_fts.db", "message_resource.db"}
    )

    selected_specs = CONTACT_SPECS
    if targets:
        lowered = {target.casefold() for target in targets}
        selected_specs = [
            spec for spec in CONTACT_SPECS
            if spec["name"].casefold() in lowered or spec["identifier"].casefold() in lowered
        ]

    contact_conn = sqlite3.connect(str(DECRYPTED_DIR / "contact" / "contact.db"))
    contact_cur = contact_conn.cursor()

    cases = []
    summary = []

    for spec in selected_specs:
        contact = _resolve_contact(contact_cur, spec)
        if not contact:
            summary.append(
                {
                    "contact_name": spec["name"],
                    "identifier": spec["identifier"],
                    "status": "contact_not_found",
                    "question_count": 0,
                }
            )
            continue

        contact_cases = []
        manual_assignments = _manual_assignments_for_contact(spec, contact)
        for db_name, rows in _iter_contact_rows(msg_paths, contact["username"]):
            case = _case_for_db(spec["name"], spec["identifier"], contact["username"], db_name, rows, manual_assignments)
            if case:
                contact_cases.append(case)

        contact_cases.sort(key=lambda item: item["db_index"])
        cases.extend(contact_cases)
        summary.append(
            {
                "contact_name": spec["name"],
                "identifier": spec["identifier"],
                "username": contact["username"],
                "status": "ok",
                "question_count": len(contact_cases),
                "output_folder": str(Path("wechat_export") / "output" / "exports" / "contacts" / _safe_name(spec["name"])),
            }
        )

    contact_conn.close()

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_contacts": len(selected_specs),
        "total_questions": len(cases),
        "summary": summary,
        "cases": cases,
    }


def write_question_reports(payload):
    QUESTIONS_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    cases = payload["cases"]

    text_lines = [
        "# 说话人归属人工确认清单",
        f"生成时间: {payload['generated_at']}",
        f"联系人数: {payload['total_contacts']}",
        f"待确认问题数: {payload['total_questions']}",
        "",
    ]
    for index, case in enumerate(cases, start=1):
        text_lines.extend(_render_case_text(index, case))
    QUESTIONS_TXT.write_text("\n".join(text_lines), encoding="utf-8")


def build_questions(targets=None):
    payload = collect_question_payload(targets)
    write_question_reports(payload)
    cases = payload["cases"]

    print(f"已生成 {len(cases)} 个待确认问题")
    print(f"JSON: {QUESTIONS_JSON}")
    print(f"文本: {QUESTIONS_TXT}")


def _parse_args():
    parser = argparse.ArgumentParser(description="生成说话人归属人工确认清单")
    parser.add_argument(
        "--contact",
        dest="contacts",
        action="append",
        help="只生成指定联系人，可传姓名或 identifier；重复传参可处理多位",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    build_questions(args.contacts)
