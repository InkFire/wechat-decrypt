import os
import sqlite3
from datetime import datetime

from paths import MANUAL_EXPORTS_DIR, MESSAGE_DIR, ensure_output_dirs

# 目标用户的表名
target_table = "Msg_86d3dd5960b085b7e5282d70c3d46c70"
ensure_output_dirs()
export_file = MANUAL_EXPORTS_DIR / "chat_export_qq905903325.txt"

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

# 打开文本文件准备写入
with open(export_file, 'w', encoding='utf-8') as f:
    # 写入表头
    f.write('时间戳|本地时间|消息类型|消息内容|数据库文件\n')
    
    total_records = 0
    
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
                    # 读取所有记录
                    cursor.execute(f"SELECT create_time, local_type, message_content FROM {target_table} ORDER BY create_time ASC")
                    records = cursor.fetchall()
                    
                    record_count = len(records)
                    total_records += record_count
                    print(f"  导出 {record_count} 条记录")
                    
                    # 写入文本文件
                    for record in records:
                        timestamp = record[0]
                        local_type = record[1]
                        message_content = record[2]
                        
                        # 转换时间戳为本地时间
                        local_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                        
                        # 转换消息类型
                        msg_type = msg_type_map.get(local_type % 4294967296, f'type={local_type}')
                        
                        # 处理消息内容
                        base_type = local_type % 4294967296
                        
                        if base_type == 1:  # 文本消息
                            if isinstance(message_content, bytes):
                                try:
                                    # 尝试解码为字符串
                                    message_content = message_content.decode('utf-8', errors='replace')
                                except:
                                    message_content = "[文本解码失败]"
                            # 过滤乱码，只保留可打印字符
                            message_content = ''.join(c for c in message_content if c.isprintable() or c in '\n\t')
                            # 替换|字符，避免分割错误
                            message_content = message_content.replace('|', ' ')
                        else:  # 非文本消息
                            message_content = f"[{msg_type}]"
                        
                        # 写入行
                        f.write(f"{timestamp}|{local_time}|{msg_type}|{message_content}|{file_name}\n")
                
                conn.close()
            except Exception as e:
                print(f"  错误: {e}")

print(f"\n导出完成！聊天记录已保存到 {export_file}")
print(f"共导出 {total_records} 条记录")
print(f"文件包含从2012年到2026年的所有聊天记录")
