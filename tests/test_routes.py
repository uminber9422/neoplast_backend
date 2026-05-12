"""Smoke tests for HTTP routes — auth required, basic shapes correct."""

from __future__ import annotations

import io

from fastapi.testclient import TestClient


def test_health_is_unauthenticated(client: TestClient):
    r = client.get("/api/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_dashboard_stats_shape(client: TestClient, auth_headers: dict):
    r = client.get("/api/dashboard/stats", headers=auth_headers)
    assert r.status_code == 200
    data = r.json()
    for key in ("total", "valid", "invalid", "risky", "by_industry", "by_state"):
        assert key in data


def test_prospects_list_pagination(client: TestClient, auth_headers: dict):
    r = client.get("/api/prospects?page=1&limit=5", headers=auth_headers)
    assert r.status_code == 200
    body = r.json()
    assert "items" in body
    assert body["page"] == 1
    assert body["limit"] == 5


def test_prospects_filters_endpoint(client: TestClient, auth_headers: dict):
    r = client.get("/api/prospects/filters", headers=auth_headers)
    assert r.status_code == 200
    for key in ("industries", "states", "cities", "source_files", "email_statuses"):
        assert key in r.json()


def test_clusters_list_endpoint(client: TestClient, auth_headers: dict):
    r = client.get("/api/clusters", headers=auth_headers)
    assert r.status_code == 200
    assert isinstance(r.json(), list)


def test_pipeline_status_endpoint(client: TestClient, auth_headers: dict):
    r = client.get("/api/pipeline/status", headers=auth_headers)
    assert r.status_code == 200


def test_settings_admin_only(client: TestClient, sales_token: str):
    r = client.get(
        "/api/settings",
        headers={"Authorization": f"Bearer {sales_token}"},
    )
    assert r.status_code == 403


def test_upload_rejects_non_csv(client: TestClient, auth_headers: dict):
    r = client.post(
        "/api/uploads",
        headers=auth_headers,
        files={"file": ("evil.exe", io.BytesIO(b"MZ"), "application/octet-stream")},
    )
    assert r.status_code == 400


def test_upload_csv_succeeds(client: TestClient, auth_headers: dict):
    csv_body = (
        "Name,Email,Phone,Company,City,State\n"
        "Test User,upload-test@example.com,9876543210,Test Co,Pune,Maharashtra\n"
    )
    r = client.post(
        "/api/uploads",
        headers=auth_headers,
        files={"file": ("test_upload.csv", io.BytesIO(csv_body.encode()), "text/csv")},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["new"] == 1
    assert body["filename"].startswith("test_upload_")


def test_taxonomy_update_rejects_empty(client: TestClient, auth_headers: dict):
    r = client.put(
        "/api/settings/taxonomy",
        headers=auth_headers,
        json={"categories": []},
    )
    assert r.status_code == 422  # Pydantic validation


def test_user_creation_and_listing(client: TestClient, auth_headers: dict):
    r = client.post(
        "/api/settings/users",
        headers=auth_headers,
        json={"username": "new_sales_user", "password": "longenough", "role": "sales"},
    )
    assert r.status_code in (201, 409)  # may already exist if test re-runs

    r2 = client.get("/api/settings/users", headers=auth_headers)
    assert r2.status_code == 200
    usernames = [u["username"] for u in r2.json()]
    assert "new_sales_user" in usernames
