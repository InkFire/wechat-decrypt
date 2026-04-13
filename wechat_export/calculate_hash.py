import hashlib

username = "wxid_k04xxaj6xhvj21"
hash_value = hashlib.md5(username.encode()).hexdigest()
print(f"Username: {username}")
print(f"Hash: {hash_value}")
print(f"Table name: Msg_{hash_value}")
