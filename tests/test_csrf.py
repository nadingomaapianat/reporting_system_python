"""
CSRF token signing + middleware (pen test UB-2026-48, finding 4.1 "Missing CSRF Token Validation").

Run: python -m pytest tests/test_csrf.py   (needs fastapi, httpx, pytest)
The key is read once per process, so the secret is set before utils.csrf is imported.
"""
import os

os.environ["JWT_SECRET"] = "unit-test-jwt-secret-at-least-32-characters"
os.environ.pop("CSRF_SECRET", None)

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from utils.csrf import CSRFMiddleware, create_csrf_token, is_valid_csrf_token  # noqa: E402

# HMAC of "0"x64 under the key derived from the JWT_SECRET above. The same token is pinned in
# adib_backend and reporting_system_node (src/csrf/csrf.spec.ts): one secret, one signature.
PINNED_TOKEN = (
    "0000000000000000000000000000000000000000000000000000000000000000"
    ".jGAZrO3EcBqkEa1b1P0SRRoFtcyHxvruBw6eXmZMGRg"
)

# Same list as main.py CSRF_EXEMPT_PATHS.
EXEMPT = ("/csrf/token", "/docs", "/redoc", "/openapi.json", "/api/auth/logout")

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
app.add_middleware(CSRFMiddleware, exempt_paths=EXEMPT)


@app.api_route("/{path:path}", methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
async def echo(path: str):
    return {"ok": True}


client = TestClient(app)


def post(path="/api/grc/reports", cookie=None, header=None, method="POST"):
    client.cookies.clear()
    if cookie is not None:
        client.cookies.set("csrfToken", cookie)
    headers = {} if header is None else {"x-csrf-token": header}
    return client.request(method, path, headers=headers)


def test_issued_tokens_verify():
    token = create_csrf_token()
    assert is_valid_csrf_token(token)
    assert create_csrf_token() != token


def test_pinned_vector_matches_node():
    assert is_valid_csrf_token(PINNED_TOKEN)


def test_unsigned_forged_and_malformed_tokens_are_invalid():
    token = create_csrf_token()
    nonce, signature = token.split(".")
    flipped = signature[:-1] + ("B" if signature.endswith("A") else "A")
    for bad in ["testcsrf", "a" * 64, f"{nonce}.{flipped}", f"{'b' * 64}.{signature}",
                f"{token}\n", f"{token}.extra", "", None]:
        assert not is_valid_csrf_token(bad), bad


def test_pen_test_payload_is_rejected():
    response = post(cookie="testcsrf", header="testcsrf")
    assert response.status_code == 403
    assert response.json() == {"detail": "Invalid CSRF token"}


def test_missing_mismatched_unsigned_and_forged_are_rejected():
    token = create_csrf_token()
    for cookie, header in [
        (None, None),
        (token, None),
        (None, token),
        (token, create_csrf_token()),
        ("a" * 64, "a" * 64),
        (token[:-2] + "AA", token[:-2] + "AA"),
    ]:
        assert post(cookie=cookie, header=header).status_code == 403, (cookie, header)


def test_non_ascii_header_is_403_not_500():
    token = create_csrf_token()
    assert post(cookie=token, header="é".encode("latin-1")).status_code == 403
    assert post(cookie=token, header=(token[:-1] + "é").encode("latin-1")).status_code == 403


def test_valid_signed_token_is_accepted():
    token = create_csrf_token()
    for method in ["POST", "PUT", "PATCH", "DELETE"]:
        assert post(cookie=token, header=token, method=method).status_code == 200, method


def test_safe_methods_and_preflight_pass_without_token():
    for method in ["GET", "HEAD", "OPTIONS"]:
        assert post(method=method).status_code == 200, method


def test_state_changing_export_get_still_needs_token_with_login_cookie():
    client.cookies.clear()
    client.cookies.set("d_c_c_t_p_1", "session")
    assert client.get("/api/grc/abc/export-excel").status_code == 403


def test_exemptions_are_exact():
    for path in ["/csrf/token", "/api/auth/logout", "/api/auth/logout/", "/docs"]:
        assert post(path=path).status_code == 200, path
    for path in ["/csrf/token-x", "/api/auth/logout-all", "/api/auth/logout/x", "/docsx",
                 "/x/api/auth/logout", "/api/auth/logoutx"]:
        assert post(path=path).status_code == 403, path
    # A query string never changes the path that is matched.
    assert post(path="/api/grc/reports?next=/csrf/token").status_code == 403
