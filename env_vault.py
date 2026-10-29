"""
Encrypted env files at rest, same file formats and start-up procedure as the main backend
(new_adib_backend src/encrypt-util.ts + src/main.ts) and reporting Node (src/env-vault.ts), so
every service is deployed the same way:

  .env / environment.env   plain files copied in by deployment; encrypted on start, then deleted
  <name>.enc               'v2:' + base64(salt16 | iv12 | tag16 | ciphertext), AES-256-GCM,
                           key = scrypt(passcode, salt); the backend's legacy v1 CBC files still open
  .passcode                'pbkdf2$600000$<saltHex>$<hashHex>' (PBKDF2-SHA512), verifies the passcode

Start-up (for each of .env and environment.env):
  - plain file, no .passcode -> set an initial passcode, encrypt, delete the plain file
  - plain file and .passcode -> new settings copied in: check the passcode, re-encrypt, delete
  - only <name>.enc          -> enter the passcode, decrypt into os.environ (optionally change it)
  - nothing                  -> settings come from the server environment

The passcode is typed at the prompt, or given in ENV_PASSCODE when the process has no terminal
(the Docker CMD). With neither, a plain file is still loaded (with a warning) so a deploy never
breaks; only an encrypted file that cannot be opened stops the start. Values from the files
override the process environment, as the backend's decrypt does; .env is applied before
environment.env, as load_dotenv() did.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import io
import os
import secrets
import sys
from typing import Optional

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from dotenv import dotenv_values

V2_PREFIX = "v2:"
SALT_LEN, IV_LEN, TAG_LEN = 16, 12, 16
PBKDF2_ITERATIONS = 600000
PBKDF2_KEYLEN = 64
ENV_FILES = (".env", "environment.env")


def _scrypt(passcode: str, salt: bytes) -> bytes:
    # Node crypto.scryptSync defaults: N=16384, r=8, p=1.
    return hashlib.scrypt(passcode.encode("utf-8"), salt=salt, n=16384, r=8, p=1, dklen=32)


def encrypt(text: str, passcode: str) -> str:
    salt, iv = secrets.token_bytes(SALT_LEN), secrets.token_bytes(IV_LEN)
    sealed = AESGCM(_scrypt(passcode, salt)).encrypt(iv, text.encode("utf-8"), None)  # ciphertext | tag
    ciphertext, tag = sealed[:-TAG_LEN], sealed[-TAG_LEN:]
    return V2_PREFIX + base64.b64encode(salt + iv + tag + ciphertext).decode("ascii")


def decrypt(encrypted: str, passcode: str) -> str:
    payload = encrypted.strip()
    if payload.startswith(V2_PREFIX):
        raw = base64.b64decode(payload[len(V2_PREFIX):])
        if len(raw) < SALT_LEN + IV_LEN + TAG_LEN:
            raise ValueError("Encrypted content is malformed")
        salt, iv = raw[:SALT_LEN], raw[SALT_LEN:SALT_LEN + IV_LEN]
        tag, ciphertext = raw[SALT_LEN + IV_LEN:SALT_LEN + IV_LEN + TAG_LEN], raw[SALT_LEN + IV_LEN + TAG_LEN:]
        # Raises InvalidTag on a wrong passcode or a tampered file.
        return AESGCM(_scrypt(passcode, salt)).decrypt(iv, ciphertext + tag, None).decode("utf-8")
    # Legacy v1 (CBC, constant salt), for files written by the backend before the pen-test fix.
    iv_hex, content_hex = payload.split(":", 1)
    decryptor = Cipher(algorithms.AES(_scrypt(passcode, b"salt")), modes.CBC(bytes.fromhex(iv_hex))).decryptor()
    padded = decryptor.update(bytes.fromhex(content_hex)) + decryptor.finalize()
    unpadder = padding.PKCS7(128).unpadder()
    return (unpadder.update(padded) + unpadder.finalize()).decode("utf-8")


def _pbkdf2(passcode: str, salt: str, iterations: int, keylen: int) -> bytes:
    # The salt is the hex string itself, as Node's crypto.pbkdf2 uses it.
    return hashlib.pbkdf2_hmac("sha512", passcode.encode("utf-8"), salt.encode("utf-8"), iterations, keylen)


def hash_passcode(passcode: str) -> str:
    salt = secrets.token_hex(16)
    return f"pbkdf2${PBKDF2_ITERATIONS}${salt}${_pbkdf2(passcode, salt, PBKDF2_ITERATIONS, PBKDF2_KEYLEN).hex()}"


def compare_passcode(passcode: str, stored_hash: str) -> bool:
    stored = (stored_hash or "").strip()
    try:
        if stored.startswith("pbkdf2$"):
            _, iter_str, salt, key_hex = stored.split("$")
            iterations = int(iter_str)
            if iterations < 1 or not salt or not key_hex or len(key_hex) % 2:
                return False
            return hmac.compare_digest(bytes.fromhex(key_hex), _pbkdf2(passcode, salt, iterations, len(key_hex) // 2))
        salt, key_hex = stored.split(":")  # legacy '<saltHex>:<hashHex>', 1000 iterations
        return bool(salt and key_hex) and hmac.compare_digest(
            bytes.fromhex(key_hex), _pbkdf2(passcode, salt, 1000, PBKDF2_KEYLEN)
        )
    except ValueError:
        return False


def _apply(contents: str) -> None:
    for key, value in dotenv_values(stream=io.StringIO(contents)).items():
        if value is not None:
            os.environ[key] = value


def _ask(question: str) -> str:
    sys.stdout.write(question)
    sys.stdout.flush()
    return (sys.stdin.readline() or "").strip()


def unlock_env(root: Optional[str] = None) -> None:
    # `python main.py` imports main twice (as __main__, then uvicorn's "main:app"), and the
    # RELOAD=1 worker is a child process without a terminal: both inherit the unlocked
    # os.environ, so they must not ask again.
    if os.environ.get("ENV_VAULT_UNLOCKED") == "1":
        return
    try:
        _unlock(root or os.getcwd())
    finally:
        os.environ.pop("ENV_PASSCODE", None)
    os.environ["ENV_VAULT_UNLOCKED"] = "1"


def _unlock(root: str) -> None:
    pass_path = os.path.join(root, ".passcode")
    files = [
        (os.path.join(root, name), os.path.join(root, name + ".enc"))
        for name in ENV_FILES
    ]
    files = [(plain, enc) for plain, enc in files if os.path.exists(plain) or os.path.exists(enc)]
    if not files:
        return  # settings come from the server environment

    from_env = (os.environ.pop("ENV_PASSCODE", "") or "").strip() or None  # never visible to the app
    interactive = not from_env and sys.stdin is not None and sys.stdin.isatty()

    if not from_env and not interactive:
        for plain, enc in files:
            if os.path.exists(plain):
                print(f"[env] {os.path.basename(plain)} is NOT encrypted: start interactively or set ENV_PASSCODE.",
                      file=sys.stderr)
                _apply(open(plain, encoding="utf-8").read())
            else:
                raise RuntimeError(f"{os.path.basename(enc)} needs the passcode: start interactively or set ENV_PASSCODE.")
        return

    passcode: Optional[str] = from_env
    opened = []  # (enc path, plain text) of files decrypted from .enc, for a passcode change
    for plain, enc in files:
        if os.path.exists(plain):
            text = open(plain, encoding="utf-8").read()
            if not os.path.exists(pass_path):
                passcode = passcode or _ask("Set an initial passcode to encrypt environment variables: ")
                if not passcode:
                    raise RuntimeError("Passcode must not be empty.")
                with open(pass_path, "w", encoding="utf-8") as fh:
                    fh.write(hash_passcode(passcode))
            else:
                passcode = passcode or _ask("Enter passcode to encrypt the new environment variables: ")
                if not compare_passcode(passcode, open(pass_path, encoding="utf-8").read()):
                    raise RuntimeError("Passcode does not match.")
            with open(enc, "w", encoding="utf-8") as fh:
                fh.write(encrypt(text, passcode))
            os.remove(plain)
            _apply(text)
            continue

        if not os.path.exists(pass_path):
            raise RuntimeError(f"{os.path.basename(enc)} found but .passcode is missing: copy the plain file in again.")
        passcode = passcode or _ask("Enter passcode to decrypt environment variables: ")
        if not compare_passcode(passcode, open(pass_path, encoding="utf-8").read()):
            raise RuntimeError("Passcode does not match.")
        text = decrypt(open(enc, encoding="utf-8").read(), passcode)
        _apply(text)
        opened.append((enc, text))

    if interactive and opened and _ask("Would you like to change the passcode? (y/n): ").lower() == "y":
        new = _ask("Enter new passcode: ")
        if not new:
            raise RuntimeError("Passcode must not be empty.")
        with open(pass_path, "w", encoding="utf-8") as fh:
            fh.write(hash_passcode(new))
        for enc, text in opened:
            with open(enc, "w", encoding="utf-8") as fh:
                fh.write(encrypt(text, new))
