# SPDX-License-Identifier: Apache-2.0
"""Local JWT verification for Tazama tokens issued by auth-service.

Verifies the signature, expiry, and required claim of tokens signed by
Tazama's auth-service (see docs/auth/00-overview.md). Verification is fully
offline against a static RSA public key file — no JWKS fetch, no dependency
on Keycloak or auth-service being reachable at request time. See
docs/auth/01-issue-161-gap.md for why this is the pattern to follow instead
of live JWKS verification against Keycloak.
"""
import logging
import os
import threading
from typing import Any, Optional

import jwt
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
from cryptography.hazmat.primitives.serialization import load_pem_public_key

logger = logging.getLogger("auth")

_REQUIRED_CLAIM = "QUERY_LAKEHOUSE"
_ALGORITHMS = ["RS256"]
_LEEWAY_SECONDS = 30

_public_key: Optional[bytes] = None  # noqa: UP045 (repo runtime is Python 3.9, `X | None` needs 3.10+)
_public_key_error: Optional[str] = None  # noqa: UP045
_public_key_lock = threading.Lock()


def _load_public_key() -> bytes:
    """Read, parse, and validate the configured RSA public key.

    Parsing at load time (rather than trusting any readable file) ensures a
    malformed or non-RSA key fails here — surfaced by get_public_key_status()
    and /health — instead of passing jwt.decode()'s InvalidKeyError to every
    request at verify_token() time.
    """
    path = os.environ.get("CERT_PATH_PUBLIC")
    if not path:
        raise RuntimeError(
            "CERT_PATH_PUBLIC is not set — BIAR cannot verify Tazama JWTs without it. "
            "This must point at the same public key file auth-service signs with "
            "(see docs/auth/03-deployment-notes.md)."
        )
    try:
        with open(path, "rb") as f:
            key_bytes = f.read()
    except OSError as exc:
        raise RuntimeError(f"Could not read CERT_PATH_PUBLIC at '{path}': {exc}") from exc

    try:
        key = load_pem_public_key(key_bytes)
    except ValueError as exc:
        raise RuntimeError(f"CERT_PATH_PUBLIC at '{path}' is not a valid PEM public key: {exc}") from exc

    if not isinstance(key, RSAPublicKey):
        raise RuntimeError(
            f"CERT_PATH_PUBLIC at '{path}' is a {type(key).__name__}, not an RSA public key "
            f"(algorithms={_ALGORITHMS} requires RSA)."
        )

    return key_bytes


def _get_public_key() -> bytes:
    """Load and cache the verification key on first use.

    Deliberately lazy rather than loaded at import time: lakehouse_query_api.py
    imports this module before constructing the FastAPI app, so a module-level
    load would crash the whole process — including the unauthenticated
    /health and /tables routes — on a misconfigured or not-yet-mounted key.
    Loading on first verify_token() call keeps those routes reachable so an
    operator can see what's wrong instead of a bare crash loop.
    """
    global _public_key, _public_key_error
    if _public_key is not None:
        return _public_key
    with _public_key_lock:
        if _public_key is not None:
            return _public_key
        try:
            _public_key = _load_public_key()
            _public_key_error = None
        except RuntimeError as exc:
            _public_key_error = str(exc)
            raise
        return _public_key


def get_public_key_status() -> dict[str, Any]:
    """Report whether the verification key is loaded, without raising.

    For use by /health — surfaces a misconfigured CERT_PATH_PUBLIC as a
    reported check rather than only as 401s on protected routes.
    """
    if _public_key is not None:
        return {"loaded": True}
    try:
        _get_public_key()
        return {"loaded": True}
    except RuntimeError as exc:
        return {"loaded": False, "error": str(exc)}


def _extract_all_claims(payload: dict[str, Any]) -> list[str]:
    claims = payload.get("claims", [])
    if isinstance(claims, list):
        return list(claims)
    return []


def verify_token(token: str) -> dict[str, Any]:
    """Verify a Tazama JWT's signature, expiry, and required claim.

    Raises PermissionError if the required claim or tenant id is absent, or
    an InvalidTokenError subclass on any signature/expiry/format failure.
    Callers should map both to an HTTP error (403 and 401 respectively).
    """
    payload = jwt.decode(
        token,
        _get_public_key(),
        algorithms=_ALGORITHMS,
        leeway=_LEEWAY_SECONDS,
        options={"require": ["exp"]},
    )

    all_claims = _extract_all_claims(payload)
    if _REQUIRED_CLAIM not in all_claims:
        raise PermissionError(f"Token is missing required claim: '{_REQUIRED_CLAIM}'")

    tenant_id = payload.get("tenantId") or payload.get("tenant_id")
    if not tenant_id:
        raise PermissionError("Token is missing 'tenantId' or 'tenant_id' claim")

    return {
        "tenant_id": tenant_id,
        "claims": all_claims,
        "payload": payload,
    }
