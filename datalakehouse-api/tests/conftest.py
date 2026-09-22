# SPDX-License-Identifier: Apache-2.0
"""Shared fixtures for datalakehouse-api tests.

Generates a throwaway RSA keypair per test session (never a file committed
to the repo, and never the real deployment key). auth.py's public-key cache
is module-level state, so tests that need a clean load reset it via
importlib.reload rather than relying on process isolation.
"""
import importlib
import os
import sys

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

TESTS_DIR = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))


def _generate_keypair():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_pem = key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return private_pem, public_pem


_PRIVATE_PEM, _PUBLIC_PEM = _generate_keypair()


class Keys:
    private_pem = _PRIVATE_PEM
    public_pem = _PUBLIC_PEM


@pytest.fixture()
def keys():
    return Keys


@pytest.fixture()
def public_key_path(tmp_path, keys):
    path = tmp_path / "public-key.pem"
    path.write_bytes(keys.public_pem)
    return str(path)


@pytest.fixture()
def auth_module(monkeypatch, public_key_path):
    """Import auth.py fresh with CERT_PATH_PUBLIC pointed at a valid test key.

    auth.py no longer loads the key at import time (that's exactly the bug
    review round 1 fixed), so importing it doesn't require CERT_PATH_PUBLIC
    to be set — but the fixture sets it anyway so tests default to the
    happy-path key unless they override it via monkeypatch themselves.
    """
    monkeypatch.setenv("CERT_PATH_PUBLIC", public_key_path)
    if "auth" in sys.modules:
        module = importlib.reload(sys.modules["auth"])
    else:
        import auth as module
    yield module
    # Reset module-level cache so the next test starts clean regardless of
    # what this test did to _public_key.
    module._public_key = None
    module._public_key_error = None
