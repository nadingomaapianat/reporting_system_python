"""
Encrypted env files at rest — same scheme and file format as new_adib_backend and reporting
Node (src/env-vault.ts), so deployment uses one procedure and one passcode style everywhere:

  .env / environment.env   plain files copied in by deployment; encrypted on start, then deleted
  <name>.enc               AES-256-CBC, key = scrypt(passcode, b"salt", 32), "<iv hex>:<cipher hex>"
  .passcode                "<salt>:<pbkdf2-sha512(passcode, salt, 1000, 64) hex>"

Start-up (for each of .env and environment.env):
  - plain file, no .passcode -> set an initial passcode, encrypt, delete the plain file
  - plain file and .passcode -> new settings copied in: verify passcode, re-encrypt, delete
  - only <name>.enc          -> enter passcode, decrypt into os.environ (optionally change it)
  - nothing                  -> settings come from the server environment

The passcode is typed at the prompt, or given in ENV_PASSCODE for non-interactive starts.
Values already in the environment (server / docker-compose) win over the files'. .env is
applied before environment.env, as load_dotenv() did.
"""
from __future__ import annotations

import hashlib
import io
import os
import secrets
import sys
from typing import List, Optional

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from dotenv import dotenv_values

ENV_FILES = (".env", "environment.env")
PASSCODE_FILE = ".passcode"


def _key(passcode: str) -> bytes:
    # Node crypto.scryptSync defaults: N=16384, r=8, p=1
    return hashlib.scrypt(passcode.encode("utf-8"), salt=b"salt", n=16384, r=8, p=1, dklen=32)


def encrypt(text: str, passcode: str) -> str:
    iv = secrets.token_bytes(16)
    padder = padding.PKCS7(128).padder()
    data = padder.update(text.encode("utf-8")) + padder.finalize()
    enc = Cipher(algorithms.AES(_key(passcode)), modes.CBC(iv)).encryptor()
    return iv.hex() + ":" + (enc.update(data) + enc.finalize()).hex()


def decrypt(encrypted: str, passcode: str) -> str:
    iv_hex, content_hex = encrypted.strip().split(":", 1)
    dec = Cipher(algorithms.AES(_key(passcode)), modes.CBC(bytes.fromhex(iv_hex))).decryptor()
    data = dec.update(bytes.fromhex(content_hex)) + dec.finalize()
    unpadder = padding.PKCS7(128).unpadder()
    return (unpadder.update(data) + unpadder.finalize()).decode("utf-8")


def hash_passcode(passcode: str) -> str:
    salt = secrets.token_hex(16)
    return f"{salt}:{hashlib.pbkdf2_hmac('sha512', passcode.encode(), salt.encode(), 1000, 64).hex()}"


def compare_passcode(passcode: str, stored_hash: str) -> bool:
    salt, key = stored_hash.strip().split(":", 1)
    return secrets.compare_digest(key, hashlib.pbkdf2_hmac("sha512", passcode.encode(), salt.encode(), 1000, 64).hex())


def _apply(text: str) -> None:
    for key, value in dotenv_values(stream=io.StringIO(text)).items():
        if key not in os.environ and value is not None:
            os.environ[key] = value


def unlock_env(root: Optional[str] = None) -> None:
    root = root or os.getcwd()
    plain = [f for f in ENV_FILES if os.path.exists(os.path.join(root, f))]
    sealed = [f for f in ENV_FILES if f not in plain and os.path.exists(os.path.join(root, f + ".enc"))]
    if not plain and not sealed:
        return  # settings come from the server environment

    pass_path = os.path.join(root, PASSCODE_FILE)
    from_env = os.environ.pop("ENV_PASSCODE", None)  # never visible to the rest of the app
    interactive = not from_env
    if interactive and not sys.stdin.isatty():
        raise RuntimeError("A passcode is needed to unlock the env files: start interactively or set ENV_PASSCODE.")
    ask = (lambda q: input(q).strip()) if interactive else None

    stored = open(pass_path, encoding="utf-8").read() if os.path.exists(pass_path) else None
    if stored is None and sealed:
        raise RuntimeError(f"{sealed[0]}.enc found but .passcode is missing; copy the plain file in again.")

    if stored is None:
        passcode = from_env or ask("Set an initial passcode to encrypt environment variables: ")
        if not passcode:
            raise RuntimeError("Passcode must not be empty.")
        with open(pass_path, "w", encoding="utf-8") as fh:
            fh.write(hash_passcode(passcode))
    else:
        prompt = "Enter passcode to encrypt the new environment variables: " if plain else "Enter passcode to decrypt environment variables: "
        passcode = from_env or ask(prompt)
        if not compare_passcode(passcode, stored):
            raise RuntimeError("Passcode does not match.")

    loaded: List[tuple] = []
    for name in ENV_FILES:
        path = os.path.join(root, name)
        if name in plain:
            text = open(path, encoding="utf-8").read()
            with open(path + ".enc", "w", encoding="utf-8") as fh:
                fh.write(encrypt(text, passcode))
            os.remove(path)
        elif name in sealed:
            text = decrypt(open(path + ".enc", encoding="utf-8").read(), passcode)
        else:
            continue
        _apply(text)
        loaded.append((name, text))

    if interactive and not plain and ask("Would you like to change the passcode? (y/n): ").lower() == "y":
        new = ask("Enter new passcode: ")
        if not new:
            raise RuntimeError("Passcode must not be empty.")
        with open(pass_path, "w", encoding="utf-8") as fh:
            fh.write(hash_passcode(new))
        for name, text in loaded:
            with open(os.path.join(root, name + ".enc"), "w", encoding="utf-8") as fh:
                fh.write(encrypt(text, new))
