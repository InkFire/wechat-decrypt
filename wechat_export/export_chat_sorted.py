import os
import sqlite3
from datetime import datetime

from paths import MANUAL_EXPORTS_DIR, MESSAGE_DIR, ensure_output_dirs

# 目标用户的表名
target_table = "Msg_dfc1a239c95f8f7329f1520efb376de1"
ensure_output_dirs()
export_file = MANUAL_EXPORTS_DIR / "chat_export_wxid_k04xxaj6xhvj21_sorted.txt"
stats_file = MANUAL_EXPORTS_DIR / "chat_export_wxid_k04xxaj6xhvj21_stats.txt"

# 消息类型映射
msg_type_map = {
    1: '文本',
    3: '图片',
    34: '语音',
    42: '名片',
    43: '视频',
    47: '表情',
    48: '位置',
    49: '链接/文件',
    50: '通话',
    10000: '系统',
    10002: '撤回'
}

# 存储所有记录的列表
all_records = []
# 存储数据库统计信息
db_stats = {}

# 遍历所有message_*.db文件
for db_path in sorted(MESSAGE_DIR.glob("message_*.db")):
    file_name = db_path.name
    if file_name.startswith("message_") and file_name.endswith(".db"):
        print(f"处理文件: {file_name}")
        
        try:
            # 连接数据库
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            
            # 检查是否存在目标表
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (target_table,))
            result = cursor.fetchone()
            
            if result:
                # 读取所有记录，包含real_sender_id字段
                cursor.execute(f"SELECT create_time, local_type, message_content, real_sender_id FROM {target_table} ORDER BY create_time ASC")
                records = cursor.fetchall()
                
                record_count = len(records)
                print(f"  读取 {record_count} 条记录")
                
                # 处理每条记录
                for record in records:
                    timestamp = record[0]
                    local_type = record[1]
                    message_content = record[2]
                    real_sender_id = record[3]
                    
                    # 转换时间戳为本地时间
                    local_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                    
                    # 转换消息类型
                    base_type = local_type % 4294967296
                    msg_type = msg_type_map.get(base_type, f'type={local_type}')
                    
                    # 处理消息内容
                    if base_type == 1:  # 文本消息
                        if isinstance(message_content, bytes):
                            try:
                                # 尝试解码为字符串
                                message_content = message_content.decode('utf-8', errors='replace')
                            except:
                                message_content = "[文本解码失败]"
                        # 过滤乱码，只保留可打印字符
                        message_content = ''.join(c for c in message_content if c.isprintable() or c in '\n\t')
                        # 过滤掉过长的乱码内容
                        if len(message_content) > 1000:
                            message_content = "[文本内容过长]"
                        # 检查是否包含乱码（只过滤包含�字符的内容）
                        elif '�' in message_content:
                            message_content = "[文本内容包含乱码]"
                    else:  # 非文本消息
                        message_content = f"[{msg_type}]"
                    
                    # 使用原始的说话人ID
                    speaker = str(real_sender_id)
                    
                    # 添加到记录列表
                    all_records.append({
                        'local_time': local_time,
                        'msg_type': msg_type,
                        'message_content': message_content,
                        'speaker': speaker,
                        'real_sender_id': real_sender_id,
                        'timestamp': timestamp,
                        'db_file': file_name
                    })
                
                # 更新数据库统计信息
                if record_count > 0:
                    db_stats[file_name] = {
                        'record_count': record_count,
                        'first_timestamp': records[0][0],
                        'last_timestamp': records[-1][0]
                    }
                
                conn.close()
        except Exception as e:
            print(f"  错误: {e}")

# 统计说话人ID
speaker_stats = {}
for record in all_records:
    speaker_id = record['real_sender_id']
    if speaker_id not in speaker_stats:
        speaker_stats[speaker_id] = 0
    speaker_stats[speaker_id] += 1

# 按照本地时间排序
all_records.sort(key=lambda x: x['local_time'])

# 写入排序后的记录
with open(export_file, 'w', encoding='utf-8') as f:
    # 写入表头
    f.write('本地时间|消息类型|说话人|消息内容\n')
    
    for record in all_records:
        # 确保所有内容都是字符串
        local_time = str(record['local_time'])
        msg_type = str(record['msg_type'])
        speaker = str(record['speaker'])
        message_content = str(record['message_content'])
        
        # 写入行
        f.write(f"{local_time}|{msg_type}|{speaker}|{message_content}\n")

# 生成统计文件
with open(stats_file, 'w', encoding='utf-8') as f:
    f.write('微信聊天记录统计\n')
    f.write('=' * 50 + '\n')
    f.write(f"总记录数: {len(all_records)}\n")
    
    # 计算时间跨度
    if all_records:
        first_time = min(record['local_time'] for record in all_records)
        last_time = max(record['local_time'] for record in all_records)
        f.write(f"时间跨度: {first_time} 至 {last_time}\n")
    
    f.write('\n涉及的数据库文件:\n')
    f.write('-' * 50 + '\n')
    
    total_records = 0
    for db_file, stats in sorted(db_stats.items()):
        first_time = datetime.fromtimestamp(stats['first_timestamp']).strftime('%Y-%m-%d %H:%M:%S')
        last_time = datetime.fromtimestamp(stats['last_timestamp']).strftime('%Y-%m-%d %H:%M:%S')
        f.write(f"{db_file}: {stats['record_count']} 条记录 ({first_time} 至 {last_time})\n")
        total_records += stats['record_count']
    
    f.write('-' * 50 + '\n')
    f.write(f"合计: {total_records} 条记录\n")
    
    f.write('\n说话人ID统计:\n')
    f.write('-' * 50 + '\n')
    for speaker_id, count in sorted(speaker_stats.items(), key=lambda x: x[1], reverse=True):
        f.write(f"说话人ID {speaker_id}: {count} 条消息\n")
    f.write('-' * 50 + '\n')
    f.write(f"说话人ID总数: {len(speaker_stats)}\n")

print(f"\n导出完成！")
print(f"排序后的聊天记录已保存到 {export_file}")
print(f"统计信息已保存到 {stats_file}")
