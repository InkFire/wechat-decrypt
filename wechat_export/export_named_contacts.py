import argparse
import hashlib
import html
import json
import re
import sqlite3
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import zstandard as zstd

from paths import CONTACT_EXPORTS_DIR, MESSAGE_DIR, DECRYPTED_DIR, ensure_output_dirs


CONTACT_SPECS = [
    {"name": "赵金铭", "identifier": "shigurekintoki"},
    {"name": "英畅", "identifier": "yingchang001"},
    {"name": "吕晓丽", "identifier": "lily8023lily"},
    {"name": "李想", "identifier": "lxz0200919"},
    {"name": "陈昱", "identifier": "Sivia7cww"},
    {"name": "崔圆方", "identifier": "eskimoyyz"},
    {"name": "罗涵", "identifier": "luohan497242"},
    {"name": "邹立", "identifier": "jenniesen46"},
    {"name": "郑然", "identifier": "zhengran916"},
    {"name": "刘雨辰", "identifier": "chunchun710"},
    {"name": "宫甜甜", "identifier": "metianer"},
    {"name": "张璐", "identifier": "qq90590325"},
    {"name": "李婷婷", "identifier": "wxid_2532175321412"},
    {"name": "刘欣怡", "identifier": "wxid_k04xxaj6xhvj21"},
    {"name": "张婷", "identifier": "Z_ing"},
]

MSG_TYPE_MAP = {
    1: "文本",
    3: "图片",
    62: "视频",
    34: "语音",
    42: "名片",
    43: "视频",
    47: "表情",
    48: "位置",
    49: "链接/文件",
    50: "通话",
    10000: "系统",
    10002: "撤回",
}

APPMSG_TYPE_MAP = {
    "5": "链接",
    "6": "文件",
    "19": "合并转发",
    "33": "小程序",
    "36": "小程序",
    "57": "引用",
    "74": "文件",
    "2000": "转账",
    "2001": "红包",
}

SYSTEM_TYPES = {10000, 10002}
FIELD_SEPARATOR = "⟦"
CONTACT_CONFIG_FILE = "contact_config.json"

_ZSTD = zstd.ZstdDecompressor()


def _safe_name(name):
    return re.sub(r'[<>:"/\\\\|?*]', "_", name).strip() or "未命名"


def _contact_folder(spec):
    return CONTACT_EXPORTS_DIR / _safe_name(spec["name"])


def _contact_config_path(spec):
    return _contact_folder(spec) / CONTACT_CONFIG_FILE


def _load_contact_config(spec):
    path = _contact_config_path(spec)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _manual_assignments_for_contact(spec, contact):
    config = _load_contact_config(spec)
    merged = config.get("manual_assignments", {})
    if not isinstance(merged, dict):
        merged = {}
    normalized = {}
    for speaker_id, speaker in merged.items():
        if speaker in {"我", "她", "系统"}:
            normalized[str(speaker_id)] = speaker
    return normalized


def _decode_message_content(content, ct_value):
    if content is None:
        return ""
    if ct_value == 4 and isinstance(content, bytes):
        try:
            return _ZSTD.decompress(content).decode("utf-8", errors="replace")
        except Exception:
            return ""
    if isinstance(content, bytes):
        try:
            return content.decode("utf-8", errors="replace")
        except Exception:
            return ""
    return str(content)


def _clean_text(text):
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = "".join(ch if ch.isprintable() or ch in "\n\t" else " " for ch in text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


def _single_line(text):
    return _clean_text(text).replace("\n", " / ").replace(FIELD_SEPARATOR, "〖")


def _truncate(text, limit=96):
    text = _single_line(text)
    return text if len(text) <= limit else f"{text[:limit - 1]}…"


def _parse_xml(text):
    if not text:
        return None
    xml_text = str(text).strip()
    start = xml_text.find("<")
    if start < 0:
        return None
    xml_text = xml_text[start:]
    try:
        return ET.fromstring(xml_text)
    except ET.ParseError:
        return None


def _first_text(root, path):
    if root is None:
        return ""
    node = root.find(path)
    if node is None:
        return ""
    return _single_line("".join(node.itertext()))


def _parse_name_card(xml_text):
    root = _parse_xml(xml_text)
    if root is None:
        return ""
    for value in (
        root.get("nickname"),
        root.get("displayname"),
        _first_text(root, ".//nickname"),
        _first_text(root, ".//alias"),
        _first_text(root, ".//username"),
    ):
        cleaned = _single_line(value or "")
        if cleaned:
            return cleaned
    return ""


def _parse_location(xml_text):
    root = _parse_xml(xml_text)
    if root is None:
        return ""
    location = root.find(".//location")
    if location is None:
        return ""
    for value in (
        location.get("label"),
        location.get("poiname"),
        location.get("name"),
    ):
        cleaned = _single_line(value or "")
        if cleaned:
            return cleaned
    return ""


def _parse_appmsg_summary(xml_text):
    root = _parse_xml(xml_text)
    if root is None:
        return "[链接/文件]"
    app_type = _first_text(root, ".//appmsg/type")
    label = APPMSG_TYPE_MAP.get(app_type, "链接/文件")
    title = _first_text(root, ".//appmsg/title")
    desc = _first_text(root, ".//appmsg/des")
    source = _first_text(root, ".//appmsg/sourcedisplayname")
    parts = []
    if title:
        parts.append(title)
    if desc and desc != title:
        parts.append(desc)
    if source and source not in parts:
        parts.append(f"来源:{source}")
    summary = " - ".join(parts)
    return f"[{label}] {summary}".strip() if summary else f"[{label}]"


def _strip_markup(text):
    text = re.sub(r"<!\\[CDATA\\[(.*?)\\]\\]>", r"\\1", text, flags=re.S)
    text = re.sub(r"<[^>]+>", "", text)
    return _single_line(html.unescape(text))


def _summarize_voip(xml_text):
    root = _parse_xml(xml_text)
    if root is None:
        return ""
    for path in (".//msg", ".//VoIPBubbleMsg/msg"):
        msg_text = _first_text(root, path)
        if msg_text:
            return f"[通话] {msg_text}"
    return "[通话]"


def _base_type(local_type):
    return int(local_type) % 4294967296


def _parse_voice_info(xml_text):
    if not xml_text:
        return {}
    root = _parse_xml(xml_text)
    if root is None:
        return {}
    voice = root.find(".//voicemsg")
    if voice is None:
        return {}
    duration_ms = int(voice.get("voicelength") or 0)
    transcript = ""
    for path in (
        ".//voicetrans/transtext",
        ".//voicetrans/text",
        ".//transtext",
        ".//voicetranscontent",
    ):
        transcript = _first_text(root, path)
        if transcript:
            break
    return {
        "duration_ms": duration_ms,
        "duration_s": round(duration_ms / 1000, 1) if duration_ms else None,
        "voiceformat": voice.get("voiceformat") or "",
        "transcript": transcript,
    }


def _format_message(base_type, text, voice_meta, voice_relpath):
    cleaned = _single_line(text)
    if base_type == 1:
        if "<voipmsg" in text:
            return _summarize_voip(text)
        if "<" in text and ">" in text:
            stripped = _strip_markup(text)
            if stripped:
                return stripped
        return cleaned or "[空文本]"
    if base_type == 3:
        return "[图片]"
    if base_type == 34:
        duration = voice_meta.get("duration_s")
        duration_part = f" {duration}s" if duration else ""
        transcript = _single_line(voice_meta.get("transcript") or "")
        transcript_part = f" 转写: {transcript}" if transcript else ""
        return f"[语音{duration_part}]{transcript_part}"
    if base_type == 42:
        card_name = _parse_name_card(text)
        return f"[名片] {card_name}".strip() if card_name else "[名片]"
    if base_type == 43:
        return "[视频]"
    if base_type == 62:
        return "[视频]"
    if base_type == 47:
        return "[表情]"
    if base_type == 48:
        location = _parse_location(text)
        return f"[定位] {location}".strip() if location else "[定位]"
    if base_type == 49:
        return _parse_appmsg_summary(text)
    if base_type == 50:
        if "<voipmsg" in text:
            return _summarize_voip(text)
        return cleaned or "[通话]"
    if base_type == 10000:
        if "<" in text and ">" in text:
            stripped = _strip_markup(text)
            if stripped:
                return stripped
        return cleaned or "[系统消息]"
    if base_type == 10002:
        if "<" in text and ">" in text:
            stripped = _strip_markup(text)
            if stripped:
                return stripped
        return cleaned or "[撤回消息]"
    label = MSG_TYPE_MAP.get(base_type, f"type={base_type}")
    return f"[{label}] {cleaned}".strip()


def _resolve_contact(contact_cur, spec):
    identifier = spec["identifier"]
    name = spec["name"]

    candidates = []
    rules = [
        ("username", identifier, 0),
        ("alias", identifier, 1),
        ("remark", name, 2),
        ("nick_name", name, 3),
    ]
    for field, value, priority in rules:
        rows = contact_cur.execute(
            f"SELECT username, alias, remark, nick_name FROM contact WHERE {field}=?",
            (value,),
        ).fetchall()
        for row in rows:
            candidates.append((priority, row))

    if not candidates:
        return None

    _, row = sorted(candidates, key=lambda item: item[0])[0]
    username, alias, remark, nick_name = row
    return {
        "requested_name": name,
        "identifier": identifier,
        "username": username or "",
        "alias": alias or "",
        "remark": remark or "",
        "nick_name": nick_name or "",
    }


def _build_sender_stats(rows):
    sender_stats = {}
    for row in rows:
        sender_id = row[3]
        if sender_id is None:
            continue
        info = sender_stats.setdefault(
            int(sender_id),
            {
                "count": 0,
                "status": Counter(),
                "server_seq": Counter(),
            },
        )
        info["count"] += 1
        if row[4] is not None:
            info["status"][int(row[4])] += 1
        info["server_seq"]["zero" if int(row[5] or 0) == 0 else "nonzero"] += 1
    return sender_stats


def _build_sender_anchor_map(rows, contact_name):
    sender_stats = _build_sender_stats(rows)
    sender_map = {}
    conflicts = []
    for sender_id, info in sender_stats.items():
        status_counter = info["status"]
        if 2 in status_counter and 4 in status_counter:
            conflicts.append(sender_id)
            continue
        if 2 in status_counter:
            sender_map[sender_id] = "我"
        elif 4 in status_counter:
            sender_map[sender_id] = contact_name
    return sender_stats, sender_map, conflicts


def _speaker_for_row(db_name, row, counterpart_label, sender_map, manual_assignments):
    speaker_id = _speaker_id_for_row(db_name, row)
    manual_speaker = manual_assignments.get(speaker_id)
    if manual_speaker == "我":
        return "我", "manual"
    if manual_speaker == "她":
        return counterpart_label, "manual"
    if manual_speaker == "系统":
        return "系统", "manual"
    status = int(row[4] or 0)
    if status == 2:
        return "我", "status"
    if status == 4:
        return counterpart_label, "status"
    sender_id = row[3]
    if sender_id in sender_map:
        return sender_map[sender_id], "sender_id"
    return ("我" if int(row[5] or 0) == 0 else counterpart_label), "server_seq"


def _review_lines_for_db(counterpart_label, db_name, predicted_rows, sender_stats, anchor_conflicts, manual_assignments):
    side_to_ids = defaultdict(set)
    fallback_ids = set()
    all_sender_ids = sorted(sender_stats)
    confirmed_ids = {
        int(sender_id)
        for sender_id in sender_stats
        if _speaker_id_from_sender_id(db_name, sender_id) in manual_assignments
    }
    for item in predicted_rows:
        speaker = item["speaker"]
        sender_id = item["row"][3]
        if sender_id is None:
            continue
        side_to_ids[speaker].add(int(sender_id))
        if item["method"] == "server_seq":
            fallback_ids.add(int(sender_id))

    review_reasons = []
    unresolved_my_ids = sorted(sender_id for sender_id in side_to_ids.get("我", set()) if sender_id not in confirmed_ids)
    unresolved_other_ids = sorted(sender_id for sender_id in side_to_ids.get(counterpart_label, set()) if sender_id not in confirmed_ids)
    unresolved_single_ids = [sender_id for sender_id in all_sender_ids if sender_id not in confirmed_ids]
    unresolved_fallback_ids = sorted(sender_id for sender_id in fallback_ids if sender_id not in confirmed_ids)
    unresolved_conflicts = sorted(sender_id for sender_id in anchor_conflicts if sender_id not in confirmed_ids)

    if len(unresolved_my_ids) > 1:
        ids = [_speaker_id_from_sender_id(db_name, sender_id) for sender_id in unresolved_my_ids]
        review_reasons.append(f"同一个 DB 内，“我”侧出现多个说话人id，需要确认这些 id 是否都属于同一个人: {ids}")
    if len(unresolved_other_ids) > 1:
        ids = [_speaker_id_from_sender_id(db_name, sender_id) for sender_id in unresolved_other_ids]
        review_reasons.append(f"同一个 DB 内，“{counterpart_label}”侧出现多个说话人id，需要确认这些 id 是否都属于同一个人: {ids}")
    if len(all_sender_ids) == 1 and unresolved_single_ids:
        ids = [_speaker_id_from_sender_id(db_name, sender_id) for sender_id in unresolved_single_ids]
        review_reasons.append(f"同一个 DB 内只出现一个说话人id，需要人工确认这个人到底是“我”还是“{counterpart_label}”: {ids}")
    if unresolved_fallback_ids:
        ids = [_speaker_id_from_sender_id(db_name, sender_id) for sender_id in unresolved_fallback_ids]
        review_reasons.append(f"同一个 DB 内存在仅靠 server_seq 推断的说话人id: {ids}")
    if unresolved_conflicts:
        ids = [_speaker_id_from_sender_id(db_name, sender_id) for sender_id in unresolved_conflicts]
        review_reasons.append(f"同一个 DB 内同一个说话人id 同时出现 status=2 和 status=4: {ids}")

    if not review_reasons:
        return []

    lines = [f"## {db_name}", "触发人工复核原因:"]
    lines.extend(f"- {reason}" for reason in review_reasons)
    lines.append("- 说明: 下方“预测说话人”是脚本当前推断，不是最终真值。")
    lines.append("")

    ids_to_show = sorted(set().union(unresolved_my_ids, unresolved_other_ids, unresolved_fallback_ids, unresolved_conflicts, unresolved_single_ids))
    for sender_id in ids_to_show:
        sender_rows = [item for item in predicted_rows if item["row"][3] == sender_id]
        if not sender_rows:
            continue
        predicted_counter = Counter(item["speaker"] for item in sender_rows)
        predicted_speaker = predicted_counter.most_common(1)[0][0]
        methods = Counter(item["method"] for item in sender_rows)
        stats = sender_stats.get(sender_id, {"count": 0, "status": Counter(), "server_seq": Counter()})
        lines.append(
            "说话人id {speaker_id} | 预测说话人 {speaker} | 消息 {count} 条 | "
            "status {status_text} | server_seq {server_seq_text} | 判定来源 {methods}".format(
                speaker_id=_speaker_id_from_sender_id(db_name, sender_id),
                speaker=predicted_speaker,
                count=stats["count"],
                status_text=dict(sorted(stats["status"].items())),
                server_seq_text=dict(stats["server_seq"]),
                methods=dict(methods),
            )
        )
        lines.append("样本:")
        for item in sender_rows[:5]:
            row = item["row"]
            time_str = datetime.fromtimestamp(row[2]).strftime("%Y-%m-%d %H:%M:%S")
            lines.append(
                f"- {time_str} | {item['speaker']} | {item['msg_type']} | {_truncate(item['content'])}"
            )
        lines.append("")
    return lines


def _db_index_from_name(db_name):
    match = re.search(r"message_(\d+)\.db$", db_name)
    return int(match.group(1)) if match else -1


def _speaker_id_from_sender_id(db_name, sender_id):
    return f"db{_db_index_from_name(db_name)}_{int(sender_id or 0)}"


def _speaker_id_for_row(db_name, row):
    sender_id = int(row[3] or 0)
    return _speaker_id_from_sender_id(db_name, sender_id)


def _iter_contact_rows(msg_paths, username):
    table_name = "Msg_" + hashlib.md5(username.encode()).hexdigest()
    for path in msg_paths:
        conn = sqlite3.connect(str(path))
        cur = conn.cursor()
        exists = cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        if not exists:
            conn.close()
            continue
        rows = cur.execute(
            f"""
            SELECT local_id, local_type, create_time, real_sender_id, status, server_seq,
                   message_content, WCDB_CT_message_content
            FROM [{table_name}]
            ORDER BY create_time ASC
            """
        ).fetchall()
        conn.close()
        if rows:
            yield path.name, rows


def _voice_blob(media_cur, create_time, local_id):
    return media_cur.execute(
        """
        SELECT voice_data
        FROM VoiceInfo
        WHERE create_time=? AND local_id=?
        """,
        (create_time, local_id),
    ).fetchone()


def export_contacts(targets=None):
    ensure_output_dirs()
    output_root = CONTACT_EXPORTS_DIR
    output_root.mkdir(parents=True, exist_ok=True)

    msg_paths = sorted(
        path for path in MESSAGE_DIR.glob("message_*.db")
        if path.name not in {"message_fts.db", "message_resource.db"}
    )
    contact_conn = sqlite3.connect(str(DECRYPTED_DIR / "contact" / "contact.db"))
    contact_cur = contact_conn.cursor()
    media_conn = sqlite3.connect(str(DECRYPTED_DIR / "message" / "media_0.db"))
    media_cur = media_conn.cursor()

    summary = []

    selected_specs = CONTACT_SPECS
    if targets:
        lowered = {target.casefold() for target in targets}
        selected_specs = [
            spec for spec in CONTACT_SPECS
            if spec["name"].casefold() in lowered or spec["identifier"].casefold() in lowered
        ]

    for spec in selected_specs:
        contact = _resolve_contact(contact_cur, spec)
        folder = _contact_folder(spec)
        folder.mkdir(parents=True, exist_ok=True)
        voice_dir = folder / "voice_samples"
        voice_dir.mkdir(parents=True, exist_ok=True)
        transcript_path = folder / f"{_safe_name(spec['name'])}_origin.txt"
        metadata_path = folder / "metadata.json"
        review_path = folder / "speaker_review.txt"

        if not contact:
            metadata = {
                "requested_name": spec["name"],
                "identifier": spec["identifier"],
                "status": "contact_not_found",
            }
            metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
            review_path.write_text("未找到联系人，未生成说话人复核信息。\n", encoding="utf-8")
            summary.append((spec["name"], "未找到联系人", 0, 0))
            continue

        counterpart_label = "她"
        manual_assignments = _manual_assignments_for_contact(spec, contact)
        transcript_records = []
        voice_exports = []
        db_parts = []
        review_sections = [
            f"# {spec['name']} 说话人人工复核",
            "以下内容用于检查同一个 message_x.db 内说话人id（dbx_y）的归属。",
            "如果同一个 DB 内同一侧出现多个说话人id、只出现一个说话人id，或某些消息只能靠 server_seq 推断，请人工确认“我”和“她”的归属。",
            "",
        ]
        total_messages = 0
        contact_voice_messages = 0
        review_required_dbs = 0

        for db_name, rows in _iter_contact_rows(msg_paths, contact["username"]):
            sender_stats, sender_map, anchor_conflicts = _build_sender_anchor_map(rows, counterpart_label)
            predicted_rows = []
            db_count = 0
            db_index = _db_index_from_name(db_name)
            for row in rows:
                (
                    local_id, local_type, create_time, real_sender_id, status, server_seq,
                    message_content, ct_value,
                ) = row
                base_type = _base_type(local_type)
                text = _decode_message_content(message_content, ct_value)
                speaker, method = _speaker_for_row(db_name, row, counterpart_label, sender_map, manual_assignments)
                voice_meta = _parse_voice_info(text) if base_type == 34 else {}
                voice_relpath = None

                if base_type == 34 and speaker == counterpart_label:
                    blob_row = _voice_blob(media_cur, create_time, local_id)
                    if blob_row and blob_row[0]:
                        ext = ".silk" if b"SILK_V3" in blob_row[0][:32] else ".bin"
                        duration_ms = voice_meta.get("duration_ms") or 0
                        file_name = (
                            f"{db_name.replace('.db', '')}_"
                            f"{datetime.fromtimestamp(create_time).strftime('%Y%m%d_%H%M%S')}_"
                            f"local{local_id}_{duration_ms}ms{ext}"
                        )
                        voice_path = voice_dir / file_name
                        if not voice_path.exists():
                            voice_path.write_bytes(blob_row[0])
                        voice_relpath = str(Path("voice_samples") / file_name)
                        voice_exports.append({
                            "file": file_name,
                            "create_time": create_time,
                            "local_id": local_id,
                            "duration_ms": duration_ms,
                            "db": db_name,
                        })
                        contact_voice_messages += 1

                content = _format_message(base_type, text, voice_meta, voice_relpath)
                msg_type = MSG_TYPE_MAP.get(base_type, f"type={base_type}")
                transcript_records.append(
                    {
                        "create_time": create_time,
                        "local_id": local_id,
                        "db_index": db_index,
                        "speaker": speaker,
                        "speaker_id": _speaker_id_for_row(db_name, row),
                        "content": content,
                    }
                )
                predicted_rows.append(
                    {
                        "row": row,
                        "speaker": speaker,
                        "method": method,
                        "msg_type": msg_type,
                        "content": content,
                    }
                )
                total_messages += 1
                db_count += 1

            review_lines = _review_lines_for_db(
                counterpart_label,
                db_name,
                predicted_rows,
                sender_stats,
                anchor_conflicts,
                manual_assignments,
            )
            if review_lines:
                review_required_dbs += 1
                review_sections.extend(review_lines)

            if db_count:
                db_parts.append(
                    {
                        "db": db_name,
                        "messages": db_count,
                        "sender_anchor_map": {str(key): value for key, value in sorted(sender_map.items())},
                        "sender_status_summary": {
                            str(key): {
                                "count": value["count"],
                                "status": dict(sorted(value["status"].items())),
                                "server_seq": dict(value["server_seq"]),
                            }
                            for key, value in sorted(sender_stats.items())
                        },
                    }
                )

        transcript_records.sort(key=lambda item: (item["create_time"], item["db_index"], item["local_id"]))
        transcript_lines = [FIELD_SEPARATOR.join(["时间戳", "说话人", "说话人id", "消息正文"])]
        for item in transcript_records:
            time_str = datetime.fromtimestamp(item["create_time"]).strftime("%Y-%m-%d %H:%M:%S")
            transcript_lines.append(
                FIELD_SEPARATOR.join([time_str, item["speaker"], item["speaker_id"], item["content"]])
            )
        transcript_path.write_text("\n".join(transcript_lines), encoding="utf-8")
        if review_required_dbs == 0:
            review_sections.extend(
                [
                    "未发现需要人工确认的 sender_id 冲突。",
                    "当前联系人各 DB 的 sender_id 归属可以直接按脚本推断使用。",
                ]
            )
        review_path.write_text("\n".join(review_sections), encoding="utf-8")
        metadata = {
            "requested_name": spec["name"],
            "identifier": spec["identifier"],
            "resolved_contact": contact,
            "status": "ok",
            "contact_config_file": CONTACT_CONFIG_FILE if _contact_config_path(spec).exists() else None,
            "manual_assignments_applied": manual_assignments,
            "total_messages": total_messages,
            "contact_voice_messages": contact_voice_messages,
            "db_parts": db_parts,
            "voice_exports": voice_exports,
            "transcript_file": transcript_path.name,
            "speaker_review_file": review_path.name,
            "review_required_db_count": review_required_dbs,
        }
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        summary.append((spec["name"], contact["username"], total_messages, contact_voice_messages))

    contact_conn.close()
    media_conn.close()

    print("导出完成：")
    for name, username, total_messages, voice_count in summary:
        print(f"  {name} | {username} | 消息 {total_messages} 条 | 对方语音 {voice_count} 条")
    print(f"输出目录: {output_root}")


def _parse_args():
    parser = argparse.ArgumentParser(description="导出指定联系人聊天记录")
    parser.add_argument(
        "--contact",
        dest="contacts",
        action="append",
        help="只导出指定联系人，可传姓名或 identifier；重复传参可导出多位",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    export_contacts(args.contacts)
