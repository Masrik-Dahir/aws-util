"""Tests for aws_util.kms module."""
from __future__ import annotations

import boto3
import pytest

from aws_util.kms import (
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

REGION = "us-east-1"


@pytest.fixture
def kms_key_id(kms_key):
    _, key_id = kms_key
    return key_id


@pytest.fixture
def kms_key_id_2():
    client = boto3.client("kms", region_name=REGION)
    resp = client.create_key(Description="second-key")
    return resp["KeyMetadata"]["KeyId"]


# ---------------------------------------------------------------------------
# encrypt / decrypt
# ---------------------------------------------------------------------------


def test_encrypt_bytes(kms_key_id):
    result = encrypt(kms_key_id, b"hello bytes", region_name=REGION)
    assert isinstance(result, EncryptResult)
    assert result.ciphertext_blob
    assert result.key_id


def test_encrypt_string(kms_key_id):
    result = encrypt(kms_key_id, "hello string", region_name=REGION)
    assert result.ciphertext_blob


def test_encrypt_decrypt_roundtrip(kms_key_id):
    plaintext = b"secret data"
    enc = encrypt(kms_key_id, plaintext, region_name=REGION)
    dec = decrypt(enc.ciphertext_blob, region_name=REGION)
    assert dec == plaintext


def test_encrypt_decrypt_string_roundtrip(kms_key_id):
    enc = encrypt(kms_key_id, "my-secret", region_name=REGION)
    dec = decrypt(enc.ciphertext_blob, region_name=REGION)
    assert dec == b"my-secret"


def test_decrypt_with_key_id(kms_key_id):
    enc = encrypt(kms_key_id, b"data", region_name=REGION)
    dec = decrypt(enc.ciphertext_blob, key_id=kms_key_id, region_name=REGION)
    assert dec == b"data"


def test_encrypt_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.kms as kmsmod

    mock_client = MagicMock()
    mock_client.encrypt.side_effect = ClientError(
        {"Error": {"Code": "NotFoundException", "Message": "key not found"}},
        "Encrypt",
    )
    monkeypatch.setattr(kmsmod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to encrypt"):
        encrypt("nonexistent-key", "data", region_name=REGION)


def test_decrypt_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.kms as kmsmod

    mock_client = MagicMock()
    mock_client.decrypt.side_effect = ClientError(
        {"Error": {"Code": "InvalidCiphertextException", "Message": "bad ciphertext"}},
        "Decrypt",
    )
    monkeypatch.setattr(kmsmod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="KMS decryption failed"):
        decrypt(b"bad-ciphertext", region_name=REGION)


# ---------------------------------------------------------------------------
# generate_data_key
# ---------------------------------------------------------------------------


def test_generate_data_key(kms_key_id):
    result = generate_data_key(kms_key_id, region_name=REGION)
    assert isinstance(result, DataKey)
    assert result.plaintext
    assert result.ciphertext_blob
    assert result.key_id


def test_generate_data_key_aes128(kms_key_id):
    result = generate_data_key(kms_key_id, key_spec="AES_128", region_name=REGION)
    assert result.plaintext


def test_generate_data_key_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.kms as kmsmod

    mock_client = MagicMock()
    mock_client.generate_data_key.side_effect = ClientError(
        {"Error": {"Code": "NotFoundException", "Message": "key not found"}},
        "GenerateDataKey",
    )
    monkeypatch.setattr(kmsmod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to generate data key"):
        generate_data_key("nonexistent-key", region_name=REGION)


# ---------------------------------------------------------------------------
# envelope_encrypt / envelope_decrypt
# ---------------------------------------------------------------------------


def test_envelope_encrypt_returns_ciphertext_and_key(kms_key_id):
    result = envelope_encrypt(kms_key_id, b"sensitive data", region_name=REGION)
    assert "ciphertext" in result
    assert "encrypted_data_key" in result
    assert len(result["ciphertext"]) > 12  # nonce + data


def test_envelope_encrypt_string_input(kms_key_id):
    result = envelope_encrypt(kms_key_id, "string data", region_name=REGION)
    assert result["ciphertext"]


def test_envelope_encrypt_decrypt_roundtrip(kms_key_id):
    plaintext = b"top secret message"
    enc = envelope_encrypt(kms_key_id, plaintext, region_name=REGION)
    dec = envelope_decrypt(
        enc["ciphertext"],
        enc["encrypted_data_key"],
        region_name=REGION,
    )
    assert dec == plaintext


def test_envelope_decrypt_bad_ciphertext(kms_key_id):
    enc = envelope_encrypt(kms_key_id, b"data", region_name=REGION)
    # Corrupt the ciphertext (keep nonce, corrupt data)
    corrupted = enc["ciphertext"][:12] + b"\x00" * 100
    with pytest.raises(RuntimeError, match="envelope_decrypt failed"):
        envelope_decrypt(corrupted, enc["encrypted_data_key"], region_name=REGION)


# ---------------------------------------------------------------------------
# re_encrypt
# ---------------------------------------------------------------------------


def test_re_encrypt(kms_key_id, kms_key_id_2):
    enc = encrypt(kms_key_id, b"data-to-reencrypt", region_name=REGION)
    result = re_encrypt(
        enc.ciphertext_blob,
        destination_key_id=kms_key_id_2,
        region_name=REGION,
    )
    assert isinstance(result, EncryptResult)
    assert result.ciphertext_blob


def test_re_encrypt_with_source_key(kms_key_id, kms_key_id_2):
    enc = encrypt(kms_key_id, b"data", region_name=REGION)
    result = re_encrypt(
        enc.ciphertext_blob,
        destination_key_id=kms_key_id_2,
        source_key_id=kms_key_id,
        region_name=REGION,
    )
    assert result.ciphertext_blob


def test_re_encrypt_runtime_error(monkeypatch):
    from botocore.exceptions import ClientError
    from unittest.mock import MagicMock
    import aws_util.kms as kmsmod

    mock_client = MagicMock()
    mock_client.re_encrypt.side_effect = ClientError(
        {"Error": {"Code": "NotFoundException", "Message": "key not found"}},
        "ReEncrypt",
    )
    monkeypatch.setattr(kmsmod, "get_client", lambda *a, **kw: mock_client)
    with pytest.raises(RuntimeError, match="Failed to re-encrypt"):
        re_encrypt(b"ciphertext", "dest-key-id", region_name=REGION)


# ---------------------------------------------------------------------------
# decrypt_data_key (convenience wrapper)
# ---------------------------------------------------------------------------


def test_decrypt_data_key(kms_key_id):
    data_key = generate_data_key(kms_key_id, region_name=REGION)
    result = decrypt_data_key(data_key.ciphertext_blob, region_name=REGION)
    assert result == data_key.plaintext
