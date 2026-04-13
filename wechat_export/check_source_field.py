import os
import sqlite3

from paths import MESSAGE_DIR

# 目标用户的表名
target_table = "Msg_dfc1a239c95f8f7329f1520efb376de1"

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
                print(f"\n{'='*60}")
                print(f"数据库文件: {file_name}")
                print(f"{'='*60}")
                
                # 读取前10条记录，重点关注source字段
                cursor.execute(f"SELECT local_id, real_sender_id, source, message_content FROM {target_table} LIMIT 10")
                records = cursor.fetchall()
                
                print(f"\n前10条记录的source字段:")
                for i, record in enumerate(records, 1):
                    local_id, real_sender_id, source, message_content = record
                    
                    # 处理message_content
                    if isinstance(message_content, bytes):
                        try:
                            message_content = message_content.decode('utf-8', errors='replace')
                            message_content = ''.join(c for c in message_content if c.isprintable() or c in '\n\t')
                            if len(message_content) > 50:
                                message_content = message_content[:50] + "..."
                        except:
                            message_content = "<bytes>"
                    
                    # 处理source字段
                    if source is None:
                        source_hex = "NULL"
                        source_str = "NULL"
                    elif isinstance(source, bytes):
                        source_hex = source.hex()
                        try:
                            source_str = source.decode('utf-8', errors='replace')
                        except:
                            source_str = "<bytes>"
                    else:
                        source_hex = "N/A"
                        source_str = str(source)
                    
                    print(f"\n记录 {i}:")
                    print(f"  local_id: {local_id}")
                    print(f"  real_sender_id: {real_sender_id}")
                    print(f"  source (hex): {source_hex[:200] if len(source_hex) > 200 else source_hex}")
                    print(f"  source (str): {source_str[:200] if len(source_str) > 200 else source_str}")
                    print(f"  message_content: {message_content}")
                
                conn.close()
        except Exception as e:
            print(f"处理文件 {file_name} 时出错: {e}")
