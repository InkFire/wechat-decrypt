import hashlib

usernames = ["qq905903325", "wxid_k04xxaj6xhvj21", "Z_ing_", "metianer"]

for username in usernames:
    hash_value = hashlib.md5(username.encode()).hexdigest()
    print(f"Username: {username}")
    print(f"Hash: {hash_value}")
    print(f"Table name: Msg_{hash_value}")
    print()
