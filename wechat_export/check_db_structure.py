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
                
                # 获取表结构
                cursor.execute(f"PRAGMA table_info({target_table})")
                columns = cursor.fetchall()
                
                print("\n表结构:")
                for col in columns:
                    print(f"  {col[1]} ({col[2]})")
                
                # 读取前5条记录
                cursor.execute(f"SELECT * FROM {target_table} LIMIT 5")
                records = cursor.fetchall()
                
                print(f"\n前5条记录:")
                for i, record in enumerate(records, 1):
                    print(f"\n记录 {i}:")
                    for j, col in enumerate(columns):
                        value = record[j]
                        if isinstance(value, bytes):
                            value = f"<bytes, length={len(value)}>"
                        elif value is not None and len(str(value)) > 100:
                            value = str(value)[:100] + "..."
                        print(f"  {col[1]}: {value}")
                
                conn.close()
        except Exception as e:
            print(f"处理文件 {file_name} 时出错: {e}")
