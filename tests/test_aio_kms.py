"""Tests for aws_util.aio.kms — 100 % line coverage."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from aws_util.aio.kms import (
    DataKey,
    EncryptResult,
    decrypt,
    decrypt_data_key,
    encrypt,
    envelope_decrypt,
    envelope_encrypt,
    generate_data_key,
    re_encrypt,
)


def _mc(return_value=None, side_effect=None):
    c = AsyncMock()
    if side_effect:
        c.call.side_effect = side_effect
    else:
        c.call.return_value = return_value or {}
    return c


# ---------------------------------------------------------------------------
# encrypt
# ---------------------------------------------------------------------------


async def test_encrypt_string(monkeypatch):
    mc = _mc({"CiphertextBlob": b"ct", "KeyId": "k1", "EncryptionAlgorithm": "SYMMETRIC_DEFAULT"})
    monkeypatch.setattr("aws_util.aio.kms.async_client", lambda *a, **kw: mc)
    r = await encrypt("k1", "hello")
    assert isinstance(r, EncryptResult)
    assert r.ciphertext_blob == b"ct"
    assert r.key_id == "k1"


async def test_encrypt_bytes(monkeypatch):
    mc = _mc({"CiphertextBlob": b"ct", "KeyId": "k1"})
    monkeypatch.setattr("aws_util.aio.kms.async_client", lambda *a, **kw: mc)
    r = await encrypt("k1", b"raw")
    assert r.encryption_algorithm == "SYMMETRIC_DEFAULT"  # fallback via .get


async def test_encrypt_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("boom"))
    monkeypatch.setattr("aws_util.aio.kms.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to encrypt"):
        await encrypt("k1", "x")


# ---------------------------------------------------------------------------
# decrypt
# ---------------------------------------------------------------------------


async def test_decrypt_basic(monkeypatch):
    mc = _mc({"Plaintext": b"plain"})
    monkeypatch.setattr("aws_util.aio.kms.async_client", lambda *a, **kw: mc)
    assert await decrypt(b"ct") == b"plain"


async def test_decrypt_with_key_id(monkeypatch):
    mc = _mc({"Plaintext": b"p"})
    monkeypatch.setattr("aws_util.aio.kms.async_client", lambda *a, **kw: mc)
    await decrypt(b"ct", key_id="k2")
    assert "KeyId" in mc.call.call_args[1]


async def test_decrypt_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("bad"))
    monkeypatch.setattr("aws_util.aio.kms.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="KMS decryption failed"):
        await decrypt(b"ct")


# ---------------------------------------------------------------------------
# generate_data_key
# ---------------------------------------------------------------------------


async def test_generate_data_key(monkeypatch):
    mc = _mc({"Plaintext": b"dk", "CiphertextBlob": b"edk", "KeyId": "k1"})
    monkeypatch.setattr("aws_util.aio.kms.async_client", lambda *a, **kw: mc)
    r = await generate_data_key("k1")
    assert isinstance(r, DataKey)
    assert r.plaintext == b"dk"


async def test_generate_data_key_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("nope"))
    monkeypatch.setattr("aws_util.aio.kms.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to generate data key"):
        await generate_data_key("k1")


# ---------------------------------------------------------------------------
# envelope_encrypt / envelope_decrypt
# ---------------------------------------------------------------------------


async def test_envelope_encrypt(monkeypatch):
    mc = _mc({"Plaintext": b"\x00" * 32, "CiphertextBlob": b"edk", "KeyId": "k1"})
    monkeypatch.setattr("aws_util.aio.kms.async_client", lambda *a, **kw: mc)
    r = await envelope_encrypt("k1", "hello")
    assert "ciphertext" in r
    assert "encrypted_data_key" in r
    assert r["encrypted_data_key"] == b"edk"


async def test_envelope_encrypt_bytes(monkeypatch):
    mc = _mc({"Plaintext": b"\x00" * 32, "CiphertextBlob": b"edk", "KeyId": "k1"})
    monkeypatch.setattr("aws_util.aio.kms.async_client", lambda *a, **kw: mc)
    r = await envelope_encrypt("k1", b"raw")
    assert isinstance(r["ciphertext"], bytes)


async def test_envelope_decrypt(monkeypatch):
    # first encrypt
    key = b"\x00" * 32
    mc = _mc()
    mc.call.side_effect = [
        {"Plaintext": key, "CiphertextBlob": b"edk", "KeyId": "k1"},  # generate_data_key
    ]
    monkeypatch.setattr("aws_util.aio.kms.async_client", lambda *a, **kw: mc)
    enc = await envelope_encrypt("k1", b"test data")

    mc2 = _mc({"Plaintext": key})
    monkeypatch.setattr("aws_util.aio.kms.async_client", lambda *a, **kw: mc2)
    result = await envelope_decrypt(enc["ciphertext"], enc["encrypted_data_key"])
    assert result == b"test data"


async def test_envelope_decrypt_bad_data(monkeypatch):
    mc = _mc({"Plaintext": b"\x00" * 32})
    monkeypatch.setattr("aws_util.aio.kms.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="envelope_decrypt failed"):
        await envelope_decrypt(b"bad" * 10, b"edk")


# ---------------------------------------------------------------------------
# re_encrypt
# ---------------------------------------------------------------------------


async def test_re_encrypt(monkeypatch):
    mc = _mc({
        "CiphertextBlob": b"new_ct",
        "KeyId": "k2",
        "DestinationEncryptionAlgorithm": "SYMMETRIC_DEFAULT",
    })
    monkeypatch.setattr("aws_util.aio.kms.async_client", lambda *a, **kw: mc)
    r = await re_encrypt(b"ct", "k2")
    assert r.ciphertext_blob == b"new_ct"


async def test_re_encrypt_with_source(monkeypatch):
    mc = _mc({"CiphertextBlob": b"new_ct", "KeyId": "k2"})
    monkeypatch.setattr("aws_util.aio.kms.async_client", lambda *a, **kw: mc)
    r = await re_encrypt(b"ct", "k2", source_key_id="k1")
    assert r.encryption_algorithm == "SYMMETRIC_DEFAULT"  # fallback


async def test_re_encrypt_error(monkeypatch):
    mc = _mc(side_effect=RuntimeError("fail"))
    monkeypatch.setattr("aws_util.aio.kms.async_client", lambda *a, **kw: mc)
    with pytest.raises(RuntimeError, match="Failed to re-encrypt"):
        await re_encrypt(b"ct", "k2")


# ---------------------------------------------------------------------------
# decrypt_data_key
# ---------------------------------------------------------------------------


async def test_decrypt_data_key(monkeypatch):
    mc = _mc({"Plaintext": b"dk"})
    monkeypatch.setattr("aws_util.aio.kms.async_client", lambda *a, **kw: mc)
    assert await decrypt_data_key(b"edk") == b"dk"
