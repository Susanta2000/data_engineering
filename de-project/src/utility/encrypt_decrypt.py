from Cryptodome.Protocol.KDF import PBKDF2
from Cryptodome.Cipher import AES
from dependencies.dev import config
from loguru import logger
from sys import exit
import base64


try:
    key = config.KEY
    iv = config.IV
    salt = config.SALT

    if not (key and iv and salt):
        raise Exception("Error while fetching key/iv/salt")
    logger.info("Successfully validate key, iv and salt parameters")
except Exception as err:
    logger.error(f"Error for encryption parameters. Error is {err}")
    exit(0)

BS = 16    # Byte size
pad = lambda s: bytes(s + (BS - len(s) % BS) * chr(BS - len(s) % BS), "utf-8")   # Padding
unpad = lambda s: s[0:-ord(s[-1:])]   # Unpadding


def get_private_key() -> bytes:
    salt_ = salt.encode("utf-8")
    kdf = PBKDF2(key, salt_, 16, 1000)
    key32 = kdf[:32]
    return key32


def encrypt(raw: str) -> bytes:
    """
    Encrypt the raw text and return.
    :param raw: Any credential.
    :return: Encrypted bytes.
    """
    raw = pad(raw)
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode("utf-8"))
    return base64.b64encode(cipher.encrypt(raw))


def decrypt(enc: str) -> str:
    """
    Decrypt the encrypted bytes.
    :param enc: Encrypted bytes.
    :return: Decrypted string.
    """
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode("utf-8"))
    return unpad(cipher.decrypt(base64.b64decode(enc))).decode("utf-8")
