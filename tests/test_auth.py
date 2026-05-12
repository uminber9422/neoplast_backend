"""Auth — login, refresh, role enforcement, hash."""

from __future__ import annotations

from fastapi.testclient import TestClient

from backend.auth.security import (
    create_refresh_token,
    decode_token,
    hash_password,
    verify_password,
)


def test_password_hash_and_verify():
    hashed = hash_password("hunter2-and-then-some")
    assert hashed != "hunter2-and-then-some"
    assert verify_password("hunter2-and-then-some", hashed)
    assert not verify_password("wrong", hashed)


def test_decode_rejects_wrong_token_type():
    refresh = create_refresh_token(subject="alice", role="admin")
    decoded = decode_token(refresh, expected_type="refresh")
    assert decoded["sub"] == "alice"

    import pytest
    from jose import JWTError

    with pytest.raises(JWTError):
        decode_token(refresh, expected_type="access")


def test_login_success_returns_tokens(client: TestClient, admin_user):
    r = client.post(
        "/api/auth/login",
        json={"username": admin_user.username, "password": "test-pw-12345"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["role"] == "admin"
    assert body["username"] == admin_user.username
    assert body["access_token"]
    assert body["refresh_token"]


def test_login_wrong_password_returns_401(client: TestClient, admin_user):
    r = client.post(
        "/api/auth/login",
        json={"username": admin_user.username, "password": "nope"},
    )
    assert r.status_code == 401
    assert "incorrect" in r.json()["detail"].lower()


def test_login_unknown_user_returns_401(client: TestClient):
    r = client.post(
        "/api/auth/login",
        json={"username": "nobody", "password": "whatever"},
    )
    assert r.status_code == 401


def test_refresh_returns_new_access_token(client: TestClient, admin_user):
    r = client.post(
        "/api/auth/login",
        json={"username": admin_user.username, "password": "test-pw-12345"},
    )
    refresh = r.json()["refresh_token"]
    r2 = client.post("/api/auth/refresh", json={"refresh_token": refresh})
    assert r2.status_code == 200
    assert r2.json()["access_token"]


def test_protected_route_rejects_missing_token(client: TestClient):
    r = client.get("/api/dashboard/stats")
    assert r.status_code == 401


def test_admin_only_route_rejects_sales(client: TestClient, sales_token: str):
    r = client.get(
        "/api/settings",
        headers={"Authorization": f"Bearer {sales_token}"},
    )
    assert r.status_code == 403


def test_admin_only_route_allows_admin(client: TestClient, admin_token: str):
    r = client.get(
        "/api/settings",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert r.status_code == 200
