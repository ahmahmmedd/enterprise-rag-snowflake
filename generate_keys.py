from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

# Generate a 2048-bit RSA private key
key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

# Format the private key (unencrypted for local development)
private_key = key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# Format the public key
public_key = key.public_key().public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo
)

# Save to your project folder
with open("rsa_key.p8", "wb") as f: 
    f.write(private_key)
with open("rsa_key.pub", "wb") as f: 
    f.write(public_key)

print("Enterprise RSA Keys generated successfully!")