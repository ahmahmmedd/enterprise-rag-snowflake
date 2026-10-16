with open("rsa_key.pub", "r") as f:
    key_lines = f.readlines()

# Strip headers, footers, and all hidden newline characters
clean_key = "".join([line.strip() for line in key_lines if "PUBLIC KEY" not in line])

print("\n--- RUN THIS EXACT SQL IN SNOWSIGHT ---")
print("USE ROLE ACCOUNTADMIN;")
print(f"ALTER USER RAG_APP_USER SET RSA_PUBLIC_KEY = '{clean_key}';")
print("---------------------------------------\n")