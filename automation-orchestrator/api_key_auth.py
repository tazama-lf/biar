# SPDX-License-Identifier: Apache-2.0
"""Shared-secret check for the orchestrator's trigger endpoint.

NiFi sends the key in the ``x-api-key`` header; the orchestrator compares it
with ``ORCHESTRATOR_API_KEY``. Kept free of Spark imports so it can be tested
on its own.
"""
import hmac
import logging
import os
from typing import Optional

from fastapi import Header, HTTPException, status

logger = logging.getLogger("api_key_auth")

API_KEY_ENV = "ORCHESTRATOR_API_KEY"


def configured_api_key() -> str:
    return os.environ.get(API_KEY_ENV, "").strip()


def api_key_status() -> str:
    return "configured" if configured_api_key() else "missing"


def require_api_key(
    x_api_key: Optional[str] = Header(  # noqa: UP045 (repo runtime is Python 3.9, `X | None` needs 3.10+)
        None,
        alias="x-api-key",
        description=f"Shared secret; must match {API_KEY_ENV} on the orchestrator",
    ),
) -> None:
    expected = configured_api_key()
    if not expected:
        logger.error("%s is not set; refusing trigger requests", API_KEY_ENV)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"status": "error", "code": 503, "message": "Trigger API key is not configured on the orchestrator"},
        )
    if not x_api_key or not hmac.compare_digest(x_api_key.encode("utf-8"), expected.encode("utf-8")):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"status": "error", "code": 401, "message": "Missing or invalid x-api-key header"},
        )
