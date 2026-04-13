import os
import sqlite3
from datetime import datetime
import hashlib
import re

from paths import BATCH_EXPORTS_DIR, MESSAGE_DIR, ensure_output_dirs

# 六个用户的配置 - TTDWX_修正为正确的username
users = [
    {"username": "qq90590325", "hash": "86d3dd5960b085b7e5282d70c3d46c70"},
    {"username": "wxid_k04xxaj6xhvj21", "hash": "dfc1a239c95f8f7329f1520efb376de1"},
    {"username": "Z_ing_", "hash": "f31e829b6cc70235cdea6459c23cbd97"},
    {"username": "metianer", "hash": "0ac7072a7cc576eaa475d407a243a750"},
    {"username": "wxid_2532175321412", "hash": hashlib.md5("wxid_2532175321412".encode()).hexdigest()},
    {"username": "chunchun710", "hash": hashlib.md5("chunchun710".encode()).hexdigest()}
]

ensure_output_dirs()
output_base = BATCH_EXPORTS_DIR

msg_type_map = {
    1: '文本', 3: '图片', 34: '语音', 42: '名片', 43: '视频',
    47: '表情', 48: '位置', 49: '链接/文件', 50: '通话',
    10000: '系统', 10002: '撤回'
}

os.makedirs(output_base, exist_ok=True)


def is_garbled_text(text):
    """检测文本是否为乱码（大量不可读字符）"""
    if not text or len(text) < 3:
        return False
    
    # 统计各类字符比例
    total = len(text)
    cjk_count = 0          # 中日韩文字（包括繁体）
    ascii_printable = 0   # 可打印ASCII
    emoji_like = 0         # emoji类字符
    latin_garbage = 0      # 拉丁乱码（如 à, ì, ö, ñ, ù 等）
    
    for ch in text:
        code = ord(ch)
        
        # CJK统一汉字（包括简体和繁体）
        if 0x4E00 <= code <= 0x9FFF:
            cjk_count += 1
        # CJK扩展A
        elif 0x3400 <= code <= 0x4DBF:
            cjk_count += 1
        # CJK兼容
        elif 0xF900 <= code <= 0xFAFF:
            cjk_count += 1
        # 全角符号
        elif 0xFF00 <= code <= 0xFFEF:
            cjk_count += 1
        # 标点符号区
        elif 0x3000 <= code <= 0x303F:
            cjk_count += 1
        # Emoji区域
        elif 0x2600 <= code <= 0x27BF or \
             0xFE00 <= code <= 0xFE0F or \
             0x1F000 <= code <= 0x1FFFF or \
             0x2702 <= code <= 0x27B0:
            emoji_like += 1
        # 可打印ASCII
        elif 0x20 <= code <= 0x7E:
            ascii_printable += 1
        # 拉丁扩展字符（通常是乱码）
        elif 0x80 <= code <= 0xFF or 0x100 <= code <= 0x17F or 0x180 <= code <= 0x24F:
            latin_garbage += 1
    
    readable_ratio = (cjk_count + ascii_printable + emoji_like) / total
    latin_ratio = latin_garbage / total
    
    # 规则1: 可读内容太少且拉丁乱码多 -> 乱码
    if readable_ratio < 0.4 and latin_ratio > 0.15:
        return True
    
    # 规则2: 超长文本且拉丁乱码占比高（如用户提到的那类长串乱码）
    if total > 80 and latin_ratio > 0.3:
        return True
    
    # 规则3: 连续的Latin扩展字符过多（超过20个连续）
    consecutive_latin = 0
    max_consecutive = 0
    for ch in text:
        code = ord(ch)
        if 0x80 <= code <= 0xFF or 0x100 <= code <= 0x24F:
            consecutive_latin += 1
            max_consecutive = max(max_consecutive, consecutive_latin)
        else:
            consecutive_latin = 0
    if max_consecutive > 20:
        return True
    
    return False


def clean_message_content(message_content):
    """清理消息内容，尽量保留可读部分"""
    if isinstance(message_content, bytes):
        decoded_ok = False
        
        # 策略1: 尝试各种编码
        for enc in ['utf-8', 'gbk', 'gb2312', 'gb18030', 'utf-16', 'latin-1']:
            try:
                decoded = message_content.decode(enc)
                if len(decoded) > 0:
                    message_content = decoded
                    decoded_ok = True
                    break
            except:
                continue
        
        # 策略2: 强制解码
        if not decoded_ok:
            try:
                message_content = message_content.decode('utf-8', errors='replace')
            except:
                message_content = message_content.decode('latin-1', errors='replace')
    
    # 清理控制字符但保留中文、emoji等
    cleaned_chars = []
    for c in message_content:
        code = ord(c)
        if c.isprintable() or c in '\n\t\r':
            cleaned_chars.append(c)
        # CJK范围 -> 保留
        elif 0x4E00 <= code <= 0x9FFF or 0x3400 <= code <= 0x4DBF or \
             0x20000 <= code <= 0x2A6DF or 0x2A700 <= code <= 0x2B73F or \
             0xFF00 <= code <= 0xFFEF or 0x3000 <= code <= 0x303F or \
             0xFE30 <= code <= 0xFE4F or 0xF900 <= code <= 0xFAFF or \
             0x2F800 <= code <= 0x2FA1F or 0xE000 <= code <= 0xF8FF or \
             0x2600 <= code <= 0x27BF or 0xFE00 <= code <= 0xFE0F or \
             0x1F000 <= code <= 0x1FFFF or 0x2000 <= code <= 0x206F:
            cleaned_chars.append(c)
        elif code < 32 and c not in '\n\t\r':
            cleaned_chars.append(' ')
    
    message_content = ''.join(cleaned_chars)
    message_content = re.sub(r' {2,}', ' ', message_content).strip()
    
    # 检测是否为乱码
    if is_garbled_text(message_content):
        return "[无法解码]"
    
    return message_content


for user in users:
    target_table = f"Msg_{user['hash']}"
    username = user['username']
    
    print(f"\n{'='*60}")
    print(f"处理用户: {username}")
    print(f"表名: {target_table}")
    print(f"{'='*60}")
    
    export_file = output_base / f"chat_export_{username}_sorted.txt"
    stats_file = output_base / f"chat_export_{username}_stats.txt"
    
    all_records = []
    db_stats = {}
    speaker_by_db_stats = {}
    speaker_global_stats = {}
    
    # 分析每个DB的status分布
    db_status_info = {}  # {db_file: {'has_status2': bool, 'has_status4': bool}}
    
    for db_path in sorted(MESSAGE_DIR.glob("message_*.db")):
        file_name = db_path.name
        if not (file_name.startswith("message_") and file_name.endswith(".db")):
            continue
        if file_name in ("message_fts.db", "message_resource.db"):
            continue
        
        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (target_table,))
            result = cursor.fetchone()
            
            if result:
                db_num = file_name.replace("message_", "").replace(".db", "")
                
                cursor.execute(
                    f"SELECT create_time, local_type, message_content, real_sender_id, server_seq, status "
                    f"FROM {target_table} ORDER BY create_time ASC"
                )
                records = cursor.fetchall()
                
                # 预分析该DB的status分布
                has_status2 = any(r[5] == 2 for r in records)
                has_status4 = any(r[5] == 4 for r in records)
                db_status_info[file_name] = {
                    'has_status2': has_status2,
                    'has_status4': has_status4,
                    'can_identify_by_status': has_status2 and has_status4
                }
                
                record_count = len(records)
                
                if record_count > 0:
                    db_stats[file_name] = {
                        'record_count': record_count,
                        'first_timestamp': records[0][0],
                        'last_timestamp': records[-1][0]
                    }
                    print(f"  处理文件: {file_name} ({record_count}条, status可用={db_status_info[file_name]['can_identify_by_status']})")
                
                if db_num not in speaker_by_db_stats:
                    speaker_by_db_stats[db_num] = {}
                
                for record in records:
                    timestamp, local_type, message_content, real_sender_id, server_seq, status = record
                    
                    local_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                    
                    base_type = local_type % 4294967296
                    msg_type = msg_type_map.get(base_type, f'type={local_type}')
                    
                    # 处理消息内容
                    if base_type == 1:
                        message_content = clean_message_content(message_content)
                    else:
                        message_content = f"[{msg_type}]"
                    
                    # 说话人ID格式：dbX_original_id
                    speaker_id = f"db{db_num}_{real_sender_id}"
                    
                    # 说话人判断规则（优先使用status字段）：
                    # status=2 -> 我 (本地发送的消息)
                    # status=4 -> 她 (接收的消息)
                    can_by_status = db_status_info[file_name]['can_identify_by_status']
                    
                    if can_by_status:
                        if status == 2:
                            speaker = "我"
                        elif status == 4:
                            speaker = "她"
                        else:
                            # 其他status值（如撤回等），用server_seq辅助判断
                            speaker = "我" if server_seq == 0 else "她"
                    else:
                        # 该DB只有一种status，尝试用server_seq判断
                        has_zero = any(r[4] == 0 for r in records)
                        has_nonzero = any(r[4] != 0 for r in records)
                        if has_zero and has_nonzero:
                            speaker = "我" if server_seq == 0 else "她"
                        else:
                            speaker = "未知"
                    
                    if real_sender_id not in speaker_by_db_stats[db_num]:
                        speaker_by_db_stats[db_num][real_sender_id] = 0
                    speaker_by_db_stats[db_num][real_sender_id] += 1
                    
                    full_speaker_key = f"{file_name}:{real_sender_id}"
                    if full_speaker_key not in speaker_global_stats:
                        speaker_global_stats[full_speaker_key] = 0
                    speaker_global_stats[full_speaker_key] += 1
                    
                    all_records.append({
                        'local_time': local_time,
                        'msg_type': msg_type,
                        'message_content': message_content,
                        'speaker_id': speaker_id,
                        'speaker': speaker,
                        'real_sender_id': real_sender_id,
                        'server_seq': server_seq,
                        'status': status,
                        'db_file': file_name,
                        'timestamp': timestamp
                    })
                
                conn.close()
        except Exception as e:
            print(f"  错误: {e}")
    
    if not all_records:
        print(f"  未找到该用户的聊天记录")
        continue
    
    all_records.sort(key=lambda x: x['local_time'])
    
    with open(export_file, 'w', encoding='utf-8') as f:
        f.write('本地时间|消息类型|说话人|说话人ID|消息内容\n')
        for record in all_records:
            f.write(f"{record['local_time']}|{record['msg_type']}|{record['speaker']}|{record['speaker_id']}|{record['message_content']}\n")
    
    with open(stats_file, 'w', encoding='utf-8') as f:
        f.write(f'微信聊天记录统计 - {username}\n')
        f.write('=' * 50 + '\n')
        f.write(f"总记录数: {len(all_records)}\n")
        
        first_time = min(r['local_time'] for r in all_records)
        last_time = max(r['local_time'] for r in all_records)
        f.write(f"时间跨度: {first_time} 至 {last_time}\n")
        
        f.write('\n涉及的数据库文件:\n')
        f.write('-' * 50 + '\n')
        total_records = 0
        for db_file, stats in sorted(db_stats.items()):
            ft = datetime.fromtimestamp(stats['first_timestamp']).strftime('%Y-%m-%d %H:%M:%S')
            lt = datetime.fromtimestamp(stats['last_timestamp']).strftime('%Y-%m-%d %H:%M:%S')
            info = db_status_info.get(db_file, {})
            can_id = info.get('can_identify_by_status', False)
            f.write(f"{db_file}: {stats['record_count']} 条 ({ft} 至 {lt}) [status可识别:{can_id}]\n")
            total_records += stats['record_count']
        f.write('-' * 50 + '\n')
        f.write(f"合计: {total_records} 条记录\n")
        
        f.write('\n说话人ID统计 (按DB文件):\n')
        f.write('-' * 50 + '\n')
        for db_num in sorted(speaker_by_db_stats.keys(), key=lambda x: int(x) if x.isdigit() else x):
            f.write(f"\n  {db_num}:\n")
            for sid, count in sorted(speaker_by_db_stats[db_num].items(), key=lambda x: -x[1]):
                f.write(f"    db{db_num}_{sid}: {count} 条\n")
        
        f.write('\n说话人识别规则:\n')
        f.write('=' * 50 + '\n')
        f.write("主规则: status=2 -> 我, status=4 -> 她\n")
        f.write("备选: 同一DB中同时存在seq=0和seq!=0时\n")
        f.write("       seq=0 -> 我, seq!=0 -> 她\n")
        f.write("兜底: 无法判断时标记为'未知'\n\n")
        
        f.write('各DB文件状态:\n')
        f.write('-' * 50 + '\n')
        for db_file in sorted(db_status_info.keys()):
            info = db_status_info[db_file]
            s2 = info.get('has_status2', False)
            s4 = info.get('has_status4', False)
            can = info.get('can_identify_by_status', False)
            f.write(f"  {db_file}: status2={s2}, status4={s4} => {'✓可识别' if can else '✗需备选'}\n")
        
        me_count = sum(1 for r in all_records if r['speaker'] == '我')
        her_count = sum(1 for r in all_records if r['speaker'] == '她')
        unknown_count = sum(1 for r in all_records if r['speaker'] == '未知')
        f.write(f'\n说话人统计:\n')
        f.write(f"  我 (status=2): {me_count} 条\n")
        f.write(f"  她 (status=4): {her_count} 条\n")
        f.write(f"  未知: {unknown_count} 条\n")
    
    print(f"  导出完成: {export_file}")
    print(f"  统计文件: {stats_file}")

print(f"\n{'='*60}")
print("全部处理完成！")
print(f"输出目录: {output_base}")
print(f"{'='*60}")
