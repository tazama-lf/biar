# SPDX-License-Identifier: Apache-2.0
import pytest
from api_key_auth import API_KEY_ENV, api_key_status, require_api_key
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

KEY = "s3cret-orchestrator-key"

app = FastAPI()


@app.post("/checksubmit", dependencies=[Depends(require_api_key)])
def submit():
    return {"status": "queued"}


client = TestClient(app)


@pytest.fixture
def key_set(monkeypatch):
    monkeypatch.setenv(API_KEY_ENV, KEY)


@pytest.fixture
def key_unset(monkeypatch):
    monkeypatch.delenv(API_KEY_ENV, raising=False)


def test_accepts_the_configured_key(key_set):
    response = client.post("/checksubmit", headers={"x-api-key": KEY})
    assert response.status_code == 200
    assert response.json() == {"status": "queued"}


def test_rejects_a_request_without_the_header(key_set):
    response = client.post("/checksubmit")
    assert response.status_code == 401
    assert response.json()["detail"]["message"] == "Missing or invalid x-api-key header"


def test_rejects_a_wrong_key(key_set):
    assert client.post("/checksubmit", headers={"x-api-key": "wrong"}).status_code == 401


def test_rejects_an_empty_key(key_set):
    assert client.post("/checksubmit", headers={"x-api-key": ""}).status_code == 401


def test_rejects_a_key_that_only_shares_a_prefix(key_set):
    assert client.post("/checksubmit", headers={"x-api-key": KEY[:-1]}).status_code == 401
    assert client.post("/checksubmit", headers={"x-api-key": KEY + "x"}).status_code == 401


def test_fails_closed_when_no_key_is_configured(key_unset):
    response = client.post("/checksubmit", headers={"x-api-key": "anything"})
    assert response.status_code == 503
    assert response.json()["detail"]["message"] == "Trigger API key is not configured on the orchestrator"


def test_a_blank_configured_key_counts_as_not_configured(monkeypatch):
    monkeypatch.setenv(API_KEY_ENV, "   ")
    assert client.post("/checksubmit", headers={"x-api-key": "   "}).status_code == 503


def test_reports_whether_a_key_is_configured(monkeypatch):
    monkeypatch.setenv(API_KEY_ENV, KEY)
    assert api_key_status() == "configured"
    monkeypatch.delenv(API_KEY_ENV)
    assert api_key_status() == "missing"
