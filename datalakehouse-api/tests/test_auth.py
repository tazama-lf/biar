# SPDX-License-Identifier: Apache-2.0
"""Tests for datalakehouse-api/auth.py.

Covers verify_token(), _get_public_key() (including the lazy-load/locking
path flagged in PR #173 review), get_public_key_status(), and
_load_public_key()'s key-format validation. Uses a throwaway RSA keypair
generated fresh per test session (see conftest.py) — never the real
deployment key.

Run with:
    cd datalakehouse-api
    pip install -r requirements.txt
    pytest tests/ -v
"""
import base64
import hashlib
import hmac
import json
import threading
import time

import jwt
import pytest


def make_tazama_token(
    signing_key,
    *,
    claims=("QUERY_LAKEHOUSE",),
    tenant_id="tenant-a",
    exp_offset=3600,
    algorithm="RS256",
    tenant_field="tenantId",
    extra_claims=None,
):
    payload = {
        "exp": int(time.time()) + exp_offset,
        "sid": "test-session",
        "iss": "https://keycloak.example.com/realms/tazama",
        "tokenString": "opaque-upstream-token",
        "clientId": "user-123",
        "claims": list(claims),
    }
    if tenant_id is not None:
        payload[tenant_field] = tenant_id
    if extra_claims:
        payload.update(extra_claims)
    return jwt.encode(payload, signing_key, algorithm=algorithm)


class TestVerifyTokenHappyPath:
    def test_valid_token_verifies(self, auth_module, keys):
        token = make_tazama_token(keys.private_pem)
        result = auth_module.verify_token(token)
        assert result["tenant_id"] == "tenant-a"
        assert "QUERY_LAKEHOUSE" in result["claims"]
        assert result["payload"]["clientId"] == "user-123"

    def test_snake_case_tenant_id_fallback(self, auth_module, keys):
        token = make_tazama_token(keys.private_pem, tenant_field="tenant_id")
        result = auth_module.verify_token(token)
        assert result["tenant_id"] == "tenant-a"

    def test_extra_claims_preserved_alongside_required_one(self, auth_module, keys):
        token = make_tazama_token(keys.private_pem, claims=["QUERY_LAKEHOUSE", "ADMIN", "OTHER_ROLE"])
        result = auth_module.verify_token(token)
        assert set(result["claims"]) == {"QUERY_LAKEHOUSE", "ADMIN", "OTHER_ROLE"}

    def test_clock_skew_leeway_accepts_recently_expired_token(self, auth_module, keys):
        token = make_tazama_token(keys.private_pem, exp_offset=-10)  # expired 10s ago, within 30s leeway
        result = auth_module.verify_token(token)
        assert result["tenant_id"] == "tenant-a"


class TestVerifyTokenRejectsMissingClaims:
    def test_missing_required_claim_raises_permission_error_403(self, auth_module, keys):
        token = make_tazama_token(keys.private_pem, claims=["SOME_OTHER_CLAIM"])
        with pytest.raises(PermissionError, match="QUERY_LAKEHOUSE"):
            auth_module.verify_token(token)

    def test_no_claims_at_all_raises_permission_error(self, auth_module, keys):
        token = make_tazama_token(keys.private_pem, claims=[])
        with pytest.raises(PermissionError):
            auth_module.verify_token(token)

    def test_missing_tenant_id_raises_permission_error_not_401(self, auth_module, keys):
        """Regression test for PR #173 review round 1, Issue 2: a token with a
        valid signature but no tenantId/tenant_id must map to 403
        (PermissionError), matching pre-PR behavior — not 401
        (InvalidTokenError), which an earlier draft introduced as an
        unflagged breaking change.
        """
        token = make_tazama_token(keys.private_pem, tenant_id=None)
        with pytest.raises(PermissionError, match="tenantId"):
            auth_module.verify_token(token)


class TestVerifyTokenRejectsBadSignaturesAndExpiry:
    def test_expired_beyond_leeway_is_rejected(self, auth_module, keys):
        token = make_tazama_token(keys.private_pem, exp_offset=-120)  # beyond 30s leeway
        with pytest.raises(jwt.ExpiredSignatureError):
            auth_module.verify_token(token)

    def test_missing_exp_claim_is_rejected(self, auth_module, keys):
        # options={"require": ["exp"]} — a token with no exp at all must fail closed.
        payload = {
            "sid": "test-session",
            "iss": "https://keycloak.example.com/realms/tazama",
            "tokenString": "opaque-upstream-token",
            "clientId": "user-123",
            "tenantId": "tenant-a",
            "claims": ["QUERY_LAKEHOUSE"],
        }
        token = jwt.encode(payload, keys.private_pem, algorithm="RS256")
        with pytest.raises(jwt.MissingRequiredClaimError):
            auth_module.verify_token(token)

    def test_wrong_key_signature_is_rejected(self, auth_module):
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa

        wrong_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        wrong_private_pem = wrong_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
        token = make_tazama_token(wrong_private_pem)
        with pytest.raises(jwt.InvalidSignatureError):
            auth_module.verify_token(token)

    def test_tampered_payload_is_rejected(self, auth_module, keys):
        token = make_tazama_token(keys.private_pem)
        header_b64, payload_b64, sig_b64 = token.split(".")
        tampered_payload = payload_b64[:-1] + ("A" if payload_b64[-1] != "A" else "B")
        tampered_token = f"{header_b64}.{tampered_payload}.{sig_b64}"
        with pytest.raises(jwt.InvalidTokenError):
            auth_module.verify_token(tampered_token)

    def test_malformed_token_string_is_rejected(self, auth_module):
        with pytest.raises(jwt.InvalidTokenError):
            auth_module.verify_token("not-a-jwt-at-all")


class TestAlgorithmConfusion:
    """Regression tests for the vulnerability class biar#161 was filed for."""

    def test_hs256_using_public_key_as_hmac_secret_is_rejected(self, auth_module, keys):
        # PyJWT's own encode() refuses to treat a PEM-formatted key as an HMAC
        # secret, so a real attacker wouldn't use PyJWT to build the forgery —
        # they'd build the raw JWT bytes by hand. Simulate that directly.
        def b64url(data: bytes) -> str:
            return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

        header = {"alg": "HS256", "typ": "JWT"}
        forged_payload = {
            "exp": int(time.time()) + 3600,
            "sid": "attacker-session",
            "iss": "https://keycloak.example.com/realms/tazama",
            "tokenString": "forged",
            "clientId": "attacker",
            "tenantId": "tenant-a",
            "claims": ["QUERY_LAKEHOUSE"],
        }
        signing_input = f"{b64url(json.dumps(header).encode())}.{b64url(json.dumps(forged_payload).encode())}"
        signature = hmac.new(keys.public_pem, signing_input.encode(), hashlib.sha256).digest()
        forged_token = f"{signing_input}.{b64url(signature)}"

        with pytest.raises(jwt.InvalidTokenError):
            auth_module.verify_token(forged_token)

    def test_none_algorithm_is_rejected(self, auth_module):
        forged_payload = {
            "exp": int(time.time()) + 3600,
            "sid": "attacker-session",
            "iss": "https://keycloak.example.com/realms/tazama",
            "tokenString": "forged",
            "clientId": "attacker",
            "tenantId": "tenant-a",
            "claims": ["QUERY_LAKEHOUSE"],
        }
        forged_token = jwt.encode(forged_payload, "", algorithm="none")
        with pytest.raises(jwt.InvalidTokenError):
            auth_module.verify_token(forged_token)


class TestLoadPublicKeyValidation:
    def test_missing_cert_path_public_env_raises(self, auth_module, monkeypatch):
        monkeypatch.delenv("CERT_PATH_PUBLIC", raising=False)
        with pytest.raises(RuntimeError, match="CERT_PATH_PUBLIC"):
            auth_module._load_public_key()

    def test_unreadable_cert_path_public_raises(self, auth_module, monkeypatch):
        monkeypatch.setenv("CERT_PATH_PUBLIC", "/nonexistent/path/to/key.pem")
        with pytest.raises(RuntimeError, match="Could not read CERT_PATH_PUBLIC"):
            auth_module._load_public_key()

    def test_malformed_pem_raises(self, auth_module, monkeypatch, tmp_path):
        bad_key_path = tmp_path / "not-a-key.pem"
        bad_key_path.write_text("this is not a PEM file")
        monkeypatch.setenv("CERT_PATH_PUBLIC", str(bad_key_path))
        with pytest.raises(RuntimeError, match="not a valid PEM public key"):
            auth_module._load_public_key()

    def test_non_rsa_key_type_raises(self, auth_module, monkeypatch, tmp_path):
        # An EC (elliptic curve) public key is valid PEM but not RSA — auth.py
        # pins algorithms=["RS256"], so a non-RSA key must be rejected at load
        # time rather than failing more confusingly inside jwt.decode() later.
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import ec

        ec_key = ec.generate_private_key(ec.SECP256R1())
        ec_public_pem = ec_key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        ec_key_path = tmp_path / "ec-public-key.pem"
        ec_key_path.write_bytes(ec_public_pem)
        monkeypatch.setenv("CERT_PATH_PUBLIC", str(ec_key_path))

        with pytest.raises(RuntimeError, match="not an RSA public key"):
            auth_module._load_public_key()


class TestGetPublicKeyLazyLoadAndCaching:
    def test_key_not_loaded_until_first_call(self, auth_module):
        assert auth_module._public_key is None

    def test_first_call_loads_and_caches(self, auth_module, keys):
        result = auth_module._get_public_key()
        assert result == keys.public_pem
        assert auth_module._public_key == keys.public_pem

    def test_second_call_returns_cached_value_without_rereading_file(self, auth_module, monkeypatch):
        first = auth_module._get_public_key()

        # If a second call re-read CERT_PATH_PUBLIC it would now explode,
        # since the path no longer points anywhere valid — proves the cache
        # is actually used instead of re-loading from disk every call.
        monkeypatch.setenv("CERT_PATH_PUBLIC", "/nonexistent/path")
        second = auth_module._get_public_key()
        assert second is first

    def test_failed_load_does_not_cache_a_bad_key(self, auth_module, monkeypatch):
        monkeypatch.setenv("CERT_PATH_PUBLIC", "/nonexistent/path")
        with pytest.raises(RuntimeError):
            auth_module._get_public_key()
        assert auth_module._public_key is None
        assert auth_module._public_key_error is not None

    def test_failed_load_then_fixed_config_succeeds_on_retry(self, auth_module, monkeypatch, public_key_path):
        monkeypatch.setenv("CERT_PATH_PUBLIC", "/nonexistent/path")
        with pytest.raises(RuntimeError):
            auth_module._get_public_key()

        # Simulates an operator fixing a misconfigured mount without
        # restarting the process — the next call should succeed once the
        # env var / file is valid, not stay wedged on the earlier failure.
        monkeypatch.setenv("CERT_PATH_PUBLIC", public_key_path)
        result = auth_module._get_public_key()
        assert result is not None
        assert auth_module._public_key_error is None

    def test_concurrent_first_calls_all_get_same_key_no_exception(self, auth_module):
        """Regression test for the first-call-wins race flagged in PR #173
        review round 2: multiple threads calling verify_token()-equivalent
        code before the key is cached must all succeed and agree on the
        loaded key, with no crash from a race between the None-check and the
        lock acquisition in _get_public_key().
        """
        results = []
        errors = []
        barrier = threading.Barrier(8)

        def worker():
            barrier.wait()  # maximize the chance all 8 threads hit _get_public_key() concurrently
            try:
                results.append(auth_module._get_public_key())
            except Exception as exc:  # noqa: BLE001 -- test must capture any thread failure, not just RuntimeError
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        assert not errors, f"concurrent _get_public_key() calls raised: {errors}"
        assert len(results) == 8
        assert len({id(r) for r in results}) == 1, "all threads should observe the same cached key object"


class TestGetPublicKeyStatus:
    def test_reports_loaded_true_after_successful_load(self, auth_module):
        status = auth_module.get_public_key_status()
        assert status == {"loaded": True}

    def test_reports_loaded_false_with_error_on_bad_config(self, auth_module, monkeypatch):
        monkeypatch.setenv("CERT_PATH_PUBLIC", "/nonexistent/path")
        status = auth_module.get_public_key_status()
        assert status["loaded"] is False
        assert "CERT_PATH_PUBLIC" in status["error"]

    def test_does_not_raise_even_when_key_is_missing(self, auth_module, monkeypatch):
        monkeypatch.delenv("CERT_PATH_PUBLIC", raising=False)
        # Must not raise — this is what /health calls, and /health must stay
        # reachable even when the auth key is completely unconfigured.
        status = auth_module.get_public_key_status()
        assert status["loaded"] is False

    def test_reflects_already_cached_key_without_reloading(self, auth_module, monkeypatch):
        auth_module._get_public_key()  # warm the cache
        monkeypatch.setenv("CERT_PATH_PUBLIC", "/nonexistent/path")  # would fail if re-read
        status = auth_module.get_public_key_status()
        assert status == {"loaded": True}
