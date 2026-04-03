"""Native async KMS utilities — real non-blocking I/O via :mod:`aws_util.aio._engine`."""

from __future__ import annotations

from typing import Any

from cryptography.exceptions import InvalidTag

from aws_util.aio._engine import async_client
from aws_util.exceptions import wrap_aws_error
from aws_util.kms import DataKey, EncryptResult

__all__ = [
    "DataKey",
    "EncryptResult",
    "decrypt",
    "decrypt_data_key",
    "encrypt",
    "envelope_decrypt",
    "envelope_encrypt",
    "generate_data_key",
    "re_encrypt",
]


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


async def encrypt(
    key_id: str,
    plaintext: str | bytes,
    encryption_algorithm: str = "SYMMETRIC_DEFAULT",
    region_name: str | None = None,
) -> EncryptResult:
    """Encrypt data using a KMS key.

    Args:
        key_id: KMS key ID, alias (``"alias/my-key"``), or ARN.
        plaintext: Data to encrypt.  Strings are UTF-8 encoded automatically.
        encryption_algorithm: KMS encryption algorithm.  Use
            ``"SYMMETRIC_DEFAULT"`` (default) for symmetric keys.
        region_name: AWS region override.

    Returns:
        An :class:`EncryptResult` containing the ciphertext blob.

    Raises:
        RuntimeError: If encryption fails.
    """
    raw = plaintext.encode("utf-8") if isinstance(plaintext, str) else plaintext
    try:
        client = async_client("kms", region_name)
        resp = await client.call(
            "Encrypt",
            KeyId=key_id,
            Plaintext=raw,
            EncryptionAlgorithm=encryption_algorithm,
        )
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to encrypt with KMS key {key_id!r}") from exc
    return EncryptResult(
        ciphertext_blob=resp["CiphertextBlob"],
        key_id=resp["KeyId"],
        encryption_algorithm=resp.get("EncryptionAlgorithm", encryption_algorithm),
    )


async def decrypt(
    ciphertext_blob: bytes,
    key_id: str | None = None,
    encryption_algorithm: str = "SYMMETRIC_DEFAULT",
    region_name: str | None = None,
) -> bytes:
    """Decrypt a ciphertext blob using KMS.

    Args:
        ciphertext_blob: The encrypted bytes returned by :func:`encrypt`.
        key_id: KMS key ID, alias, or ARN.  Required for asymmetric keys;
            optional for symmetric keys (KMS infers the key from the
            ciphertext).
        encryption_algorithm: Algorithm that was used to encrypt the data.
        region_name: AWS region override.

    Returns:
        The original plaintext as bytes.

    Raises:
        RuntimeError: If decryption fails.
    """
    kwargs: dict[str, Any] = {
        "CiphertextBlob": ciphertext_blob,
        "EncryptionAlgorithm": encryption_algorithm,
    }
    if key_id is not None:
        kwargs["KeyId"] = key_id
    try:
        client = async_client("kms", region_name)
        resp = await client.call("Decrypt", **kwargs)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, "KMS decryption failed") from exc
    return resp["Plaintext"]


async def generate_data_key(
    key_id: str,
    key_spec: str = "AES_256",
    region_name: str | None = None,
) -> DataKey:
    """Generate a data key for envelope encryption.

    Use the returned plaintext key to encrypt your data locally, then store
    the encrypted ``ciphertext_blob`` alongside the data.  Discard the
    plaintext key from memory after use.

    Args:
        key_id: KMS CMK ID, alias, or ARN used to protect the data key.
        key_spec: ``"AES_256"`` (default) or ``"AES_128"``.
        region_name: AWS region override.

    Returns:
        A :class:`DataKey` with both the plaintext and encrypted key material.

    Raises:
        RuntimeError: If key generation fails.
    """
    try:
        client = async_client("kms", region_name)
        resp = await client.call("GenerateDataKey", KeyId=key_id, KeySpec=key_spec)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to generate data key with KMS key {key_id!r}") from exc
    return DataKey(
        plaintext=resp["Plaintext"],
        ciphertext_blob=resp["CiphertextBlob"],
        key_id=resp["KeyId"],
    )


# ---------------------------------------------------------------------------
# Complex utilities
# ---------------------------------------------------------------------------


async def envelope_encrypt(
    key_id: str,
    plaintext: bytes | str,
    region_name: str | None = None,
) -> dict[str, bytes]:
    """Encrypt data using the envelope encryption pattern.

    Generates a one-time AES-256 data key via KMS, encrypts *plaintext*
    locally with AES-GCM, and returns both the ciphertext and the encrypted
    data key.  The plaintext data key never leaves memory after this call.

    Args:
        key_id: KMS CMK ID, alias (``"alias/my-key"``), or ARN.
        plaintext: Data to encrypt.  Strings are UTF-8 encoded.
        region_name: AWS region override.

    Returns:
        A dict with keys:
        - ``"ciphertext"`` -- AES-GCM ciphertext bytes (nonce prepended)
        - ``"encrypted_data_key"`` -- KMS-encrypted data key bytes (store
          alongside ciphertext; pass to :func:`envelope_decrypt`)

    Raises:
        RuntimeError: If KMS key generation fails.
    """
    import os

    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    data_key = await generate_data_key(key_id, region_name=region_name)
    raw = plaintext.encode("utf-8") if isinstance(plaintext, str) else plaintext

    nonce = os.urandom(12)
    aesgcm = AESGCM(data_key.plaintext)
    ciphertext = nonce + aesgcm.encrypt(nonce, raw, None)

    # Note: `del` removes the name binding but does NOT zero the underlying
    # memory.  True key material erasure is not possible in CPython without
    # ctypes.  The plaintext key remains in the DataKey model until garbage
    # collected.
    del aesgcm

    return {
        "ciphertext": ciphertext,
        "encrypted_data_key": data_key.ciphertext_blob,
    }


async def envelope_decrypt(
    ciphertext: bytes,
    encrypted_data_key: bytes,
    region_name: str | None = None,
) -> bytes:
    """Decrypt data produced by :func:`envelope_encrypt`.

    Uses KMS to recover the data key, then decrypts the ciphertext locally
    with AES-GCM.

    Args:
        ciphertext: The ``"ciphertext"`` value returned by
            :func:`envelope_encrypt`.
        encrypted_data_key: The ``"encrypted_data_key"`` value returned by
            :func:`envelope_encrypt`.
        region_name: AWS region override.

    Returns:
        The original plaintext as bytes.

    Raises:
        RuntimeError: If KMS decryption or AES-GCM authentication fails.
    """
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    plaintext_key = await decrypt_data_key(encrypted_data_key, region_name=region_name)
    nonce = ciphertext[:12]
    data = ciphertext[12:]
    aesgcm = AESGCM(plaintext_key)
    try:
        return aesgcm.decrypt(nonce, data, None)
    except (InvalidTag, ValueError) as exc:
        raise wrap_aws_error(exc, "envelope_decrypt failed") from exc


async def re_encrypt(
    ciphertext_blob: bytes,
    destination_key_id: str,
    source_key_id: str | None = None,
    region_name: str | None = None,
) -> EncryptResult:
    """Re-encrypt a ciphertext under a different KMS key without exposing the plaintext.

    Useful for key rotation: move data encrypted under an old key to a new key
    without decrypting it locally.  KMS performs the decrypt/re-encrypt
    operation entirely within the service boundary.

    Args:
        ciphertext_blob: Ciphertext bytes produced by :func:`encrypt`.
        destination_key_id: KMS key ID, alias, or ARN of the new key.
        source_key_id: KMS key ID, alias, or ARN of the current key.
            Required for asymmetric source keys; optional for symmetric keys.
        region_name: AWS region override.

    Returns:
        An :class:`EncryptResult` containing the re-encrypted ciphertext.

    Raises:
        RuntimeError: If re-encryption fails.
    """
    kwargs: dict[str, Any] = {
        "CiphertextBlob": ciphertext_blob,
        "DestinationKeyId": destination_key_id,
    }
    if source_key_id is not None:
        kwargs["SourceKeyId"] = source_key_id
    try:
        client = async_client("kms", region_name)
        resp = await client.call("ReEncrypt", **kwargs)
    except RuntimeError as exc:
        raise wrap_aws_error(exc, f"Failed to re-encrypt to key {destination_key_id!r}") from exc
    return EncryptResult(
        ciphertext_blob=resp["CiphertextBlob"],
        key_id=resp["KeyId"],
        encryption_algorithm=resp.get("DestinationEncryptionAlgorithm", "SYMMETRIC_DEFAULT"),
    )


async def decrypt_data_key(
    ciphertext_blob: bytes,
    region_name: str | None = None,
) -> bytes:
    """Decrypt an encrypted data key generated by :func:`generate_data_key`.

    Convenience alias for the envelope encryption pattern — semantically
    distinct from general-purpose :func:`decrypt`.

    Args:
        ciphertext_blob: The ``ciphertext_blob`` field from a
            :class:`DataKey`.
        region_name: AWS region override.

    Returns:
        The plaintext data key as bytes.
    """
    # Convenience alias for the envelope encryption pattern — semantically
    # distinct from general-purpose decrypt.
    return await decrypt(ciphertext_blob=ciphertext_blob, region_name=region_name)
