import os
import sqlite3
from datetime import datetime
import hashlib

from paths import MESSAGE_DIR, target_table_for

# 用正确的username计算hash
target_username = "wxid_2532175321412"
target_hash = hashlib.md5(target_username.encode()).hexdigest()
target_table = f"Msg_{target_hash}"

print(f"Username: {target_username}")
print(f"Hash: {target_hash}")
print(f"Table: {target_table}")

# 研究字段规律：找一个有seq=0和seq!=0的DB，分析status等字段
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
            # 检查该表是否存在
            cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
            count = cursor.fetchone()[0]
            
            if count > 0:
                # 分析server_seq和status的关系
                cursor.execute(
                    f"SELECT server_seq, status, real_sender_id, local_type, message_content "
                    f"FROM {target_table} ORDER BY create_time ASC LIMIT 50"
                )
                records = cursor.fetchall()
                
                has_zero = any(r[0] == 0 for r in records)
                has_nonzero = any(r[0] != 0 for r in records)
                
                if has_zero and has_nonzero:
                    print(f"\n{'='*60}")
                    print(f"数据库: {file_name} ({count}条)")
                    print(f"{'='*60}")
                    
                    print("\n--- server_seq=0 的记录 ---")
                    zero_recs = [r for r in records if r[0] == 0][:10]
                    for r in zero_recs:
                        seq, status, sender_id, ltype, content = r
                        content_str = str(content)[:50] if content else "NULL"
                        if isinstance(content, bytes):
                            try:
                                content_str = content.decode('utf-8', errors='replace')[:50]
                            except:
                                pass
                        print(f"  seq={seq:>10d} | status={status} | sender_id={sender_id:>4d} | type={ltype} | {content_str}")
                    
                    print("\n--- server_seq!=0 的记录 ---")
                    nonzero_recs = [r for r in records if r[0] != 0][:10]
                    for r in nonzero_recs:
                        seq, status, sender_id, ltype, content = r
                        content_str = str(content)[:50] if content else "NULL"
                        if isinstance(content, bytes):
                            try:
                                content_str = content.decode('utf-8', errors='replace')[:50]
                            except:
                                pass
                        print(f"  seq={seq:>10d} | status={status} | sender_id={sender_id:>4d} | type={ltype} | {content_str}")
                    
                    # 统计status分布
                    print("\n--- status 字段分布 ---")
                    cursor.execute(
                        f"SELECT server_seq, status, COUNT(*) FROM {target_table} "
                        f"GROUP BY server_seq, status ORDER BY server_seq, status"
                    )
                    stats = cursor.fetchall()
                    for s_seq, s_status, cnt in stats:
                        print(f"  server_seq={s_seq}, status={s_status}: {cnt}条")
                    
                    break
        
        conn.close()
    except Exception as e:
        print(f"Error {file_name}: {e}")

# 也检查 qq90590325 的规律
print("\n\n" + "="*60)
print("额外检查: qq90590325 的 message_13.db (全部seq=0的情况)")
print("="*60)

target_table2 = target_table_for("qq905903325")
db_path13 = MESSAGE_DIR / "message_13.db"

conn = sqlite3.connect(str(db_path13))
cursor = conn.cursor()

cursor.execute(
    f"SELECT status, real_sender_id, local_type, message_content "
    f"FROM {target_table2} ORDER BY create_time ASC LIMIT 30"
)
records = cursor.fetchall()

print("\n前30条记录的字段值:")
for r in records:
    status, sender_id, ltype, content = r
    content_str = str(content)[:40] if content else "NULL"
    if isinstance(content, bytes):
        try:
            content_str = content.decode('utf-8', errors='replace')[:40]
        except:
            pass
    print(f"  status={status} | sender_id={sender_id:>4d} | type={ltype} | {content_str}")

cursor.execute(
    f"SELECT status, real_sender_id, COUNT(*) FROM {target_table2} "
    f"GROUP BY status, real_sender_id ORDER BY status, real_sender_id"
)
stats = cursor.fetchall()
print("\nstatus + sender_id 分布:")
for s_status, s_sid, cnt in stats:
    print(f"  status={s_status}, sender_id={s_sid}: {cnt}条")

conn.close()
