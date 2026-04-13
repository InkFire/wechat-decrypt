import os
import sqlite3

from paths import MESSAGE_DIR

# 目标用户的表名
target_table = "Msg_86d3dd5960b085b7e5282d70c3d46c70"

# 遍历所有message_*.db文件
for db_path in sorted(MESSAGE_DIR.glob("message_*.db")):
    file_name = db_path.name
    if file_name.startswith("message_") and file_name.endswith(".db"):
        print(f"检查文件: {file_name}")
        
        try:
            # 连接数据库
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            
            # 检查是否存在目标表
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (target_table,))
            result = cursor.fetchone()
            
            if result:
                print(f"Found target table {target_table} in {file_name}")
                
                # 检查表中的记录数量
                cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
                count = cursor.fetchone()[0]
                print(f"  Record count: {count}")
                
                # 检查表结构
                cursor.execute(f"PRAGMA table_info({target_table})")
                columns = cursor.fetchall()
                print(f"  Table structure: {[col[1] for col in columns]}")
                
                # 查看前5条记录
                cursor.execute(f"SELECT create_time, local_type, message_content FROM {target_table} ORDER BY create_time ASC LIMIT 5")
                records = cursor.fetchall()
                print("  First 5 records:")
                for i, record in enumerate(records):
                    print(f"    {i+1}. Timestamp: {record[0]}, Type: {record[1]}, Content: {record[2][:50]}...")
                
            else:
                print(f"Target table {target_table} not found")
            
            conn.close()
        except Exception as e:
            print(f"  Error: {e}")
        print()
