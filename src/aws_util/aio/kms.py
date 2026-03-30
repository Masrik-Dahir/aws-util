"""Async wrappers for :mod:`aws_util.kms`."""

from __future__ import annotations

from aws_util._async_wrap import async_wrap
from aws_util.kms import (
    DataKey,
    EncryptResult,
    decrypt as _sync_decrypt,
    decrypt_data_key as _sync_decrypt_data_key,
    encrypt as _sync_encrypt,
    envelope_decrypt as _sync_envelope_decrypt,
    envelope_encrypt as _sync_envelope_encrypt,
    generate_data_key as _sync_generate_data_key,
    re_encrypt as _sync_re_encrypt,
)

__all__ = [
    "EncryptResult",
    "DataKey",
    "encrypt",
    "decrypt",
    "generate_data_key",
    "envelope_encrypt",
    "envelope_decrypt",
    "re_encrypt",
    "decrypt_data_key",
]

encrypt = async_wrap(_sync_encrypt)
decrypt = async_wrap(_sync_decrypt)
generate_data_key = async_wrap(_sync_generate_data_key)
envelope_encrypt = async_wrap(_sync_envelope_encrypt)
envelope_decrypt = async_wrap(_sync_envelope_decrypt)
re_encrypt = async_wrap(_sync_re_encrypt)
decrypt_data_key = async_wrap(_sync_decrypt_data_key)
