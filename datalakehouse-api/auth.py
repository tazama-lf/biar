# SPDX-License-Identifier: Apache-2.0
"""Local JWT verification for Tazama tokens issued by auth-service.

Verifies the signature, expiry, and required claim of tokens signed by
Tazama's auth-service (see docs/auth/00-overview.md). Verification is fully
offline against a static RSA public key file — no JWKS fetch, no dependency
on Keycloak or auth-service being reachable at request time. See
docs/auth/01-issue-161-gap.md for why this is the pattern to follow instead
of live JWKS verification against Keycloak.
"""
import os
import logging
from typing import Any, Dict, List

import jwt
from jwt import InvalidTokenError

logger = logging.getLogger("auth")

_REQUIRED_CLAIM = "QUERY_LAKEHOUSE"
_ALGORITHMS = ["RS256"]
_LEEWAY_SECONDS = 30


def _load_public_key() -> bytes:
    path = os.environ.get("CERT_PATH_PUBLIC")
    if not path:
        raise RuntimeError(
            "CERT_PATH_PUBLIC is not set — BIAR cannot verify Tazama JWTs without it. "
            "This must point at the same public key file auth-service signs with "
            "(see docs/auth/03-deployment-notes.md)."
        )
    try:
        with open(path, "rb") as f:
            return f.read()
    except OSError as exc:
        raise RuntimeError(f"Could not read CERT_PATH_PUBLIC at '{path}': {exc}") from exc


_public_key: bytes = _load_public_key()


def _extract_all_claims(payload: Dict[str, Any]) -> List[str]:
    claims = payload.get("claims", [])
    if isinstance(claims, list):
        return list(claims)
    return []


def verify_token(token: str) -> Dict[str, Any]:
    """Verify a Tazama JWT's signature, expiry, and required claim.

    Raises PermissionError if the required claim is absent, or an
    InvalidTokenError subclass on any signature/expiry/format failure.
    Callers should map both to an HTTP error (403 and 401 respectively).
    """
    payload = jwt.decode(
        token,
        _public_key,
        algorithms=_ALGORITHMS,
        leeway=_LEEWAY_SECONDS,
        options={"require": ["exp"]},
    )

    all_claims = _extract_all_claims(payload)
    if _REQUIRED_CLAIM not in all_claims:
        raise PermissionError(f"Token is missing required claim: '{_REQUIRED_CLAIM}'")

    tenant_id = payload.get("tenantId") or payload.get("tenant_id")
    if not tenant_id:
        raise InvalidTokenError("Token is missing 'tenantId' claim")

    return {
        "tenant_id": tenant_id,
        "claims": all_claims,
        "payload": payload,
    }
