import os
import sqlite3
from datetime import datetime

from paths import MESSAGE_DIR, REPORTS_DIR, ensure_output_dirs

# 目标用户的表名
target_table = "Msg_dfc1a239c95f8f7329f1520efb376de1"
ensure_output_dirs()
output_file = REPORTS_DIR / "server_seq_distribution.txt"

# 打开输出文件
with open(output_file, 'w', encoding='utf-8') as f:
    # 遍历所有message_*.db文件
    for db_path in sorted(MESSAGE_DIR.glob("message_*.db")):
        file_name = db_path.name
        if file_name.startswith("message_") and file_name.endswith(".db"):
            try:
                # 连接数据库
                conn = sqlite3.connect(str(db_path))
                cursor = conn.cursor()
                
                # 检查是否存在目标表
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (target_table,))
                result = cursor.fetchone()
                
                if result:
                    f.write(f"\n{'='*60}\n")
                    f.write(f"数据库文件: {file_name}\n")
                    f.write(f"{'='*60}\n")
                    
                    # 统计server_seq的分布
                    cursor.execute(f"SELECT server_seq, COUNT(*) FROM {target_table} GROUP BY server_seq ORDER BY server_seq")
                    server_seq_stats = cursor.fetchall()
                    
                    f.write(f"\nserver_seq分布:\n")
                    for server_seq, count in server_seq_stats:
                        f.write(f"  server_seq={server_seq}: {count} 条记录\n")
                    
                    # 读取server_seq不为0的前10条记录
                    cursor.execute(f"SELECT create_time, server_seq, real_sender_id, message_content FROM {target_table} WHERE server_seq != 0 ORDER BY create_time ASC LIMIT 10")
                    records = cursor.fetchall()
                    
                    if records:
                        f.write(f"\nserver_seq不为0的前10条记录:\n")
                        for i, record in enumerate(records, 1):
                            create_time, server_seq, real_sender_id, message_content = record
                            
                            # 处理message_content
                            if isinstance(message_content, bytes):
                                try:
                                    message_content = message_content.decode('utf-8', errors='replace')
                                    message_content = ''.join(c for c in message_content if c.isprintable() or c in '\n\t')
                                    if len(message_content) > 30:
                                        message_content = message_content[:30] + "..."
                                except:
                                    message_content = "<bytes>"
                            
                            # 转换时间戳
                            local_time = datetime.fromtimestamp(create_time).strftime('%Y-%m-%d %H:%M:%S')
                            
                            f.write(f"{i:2d}. {local_time} | server_seq={server_seq:10d} | real_sender_id={real_sender_id:4d} | {message_content}\n")
                    else:
                        f.write(f"\n没有找到server_seq不为0的记录\n")
                    
                    conn.close()
            except Exception as e:
                f.write(f"处理文件 {file_name} 时出错: {e}\n")

print(f"server_seq分布已保存到 {output_file}")
