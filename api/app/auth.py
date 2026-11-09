import json
import time

import jwt
import requests
from fastapi import HTTPException, Request

from . import config

JWKS_TTL_SECONDS = 300
_jwks_cache = {"keys": None, "fetched_at": 0.0}


def _jwks_url() -> str:
    return f"{config.KEYCLOAK_URL}/realms/{config.KEYCLOAK_REALM}/protocol/openid-connect/certs"


def _get_jwks():
    now = time.time()
    if _jwks_cache["keys"] is None or now - _jwks_cache["fetched_at"] > JWKS_TTL_SECONDS:
        try:
            response = requests.get(_jwks_url(), timeout=10)
            response.raise_for_status()
        except Exception:
            if _jwks_cache["keys"] is None:
                raise
            return _jwks_cache["keys"]
        _jwks_cache["keys"] = response.json()["keys"]
        _jwks_cache["fetched_at"] = now
    return _jwks_cache["keys"]


def current_user(request: Request) -> str:
    if not config.REQUIRE_AUTH:
        user_id = request.query_params.get("user_id")
        if not user_id:
            raise HTTPException(status_code=422, detail="user_id query param is required when auth is disabled")
        return user_id

    header = request.headers.get("Authorization", "")
    if not header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token", headers={"WWW-Authenticate": "Bearer"})
    token = header[len("Bearer "):]

    try:
        kid = jwt.get_unverified_header(token).get("kid")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    try:
        jwks_keys = _get_jwks()
    except Exception:
        raise HTTPException(status_code=503, detail="Unable to load signing keys")

    keys = [k for k in jwks_keys if k.get("kid") == kid] or jwks_keys

    claims = None
    for jwk in keys:
        try:
            public_key = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(jwk))
            claims = jwt.decode(
                token,
                public_key,
                algorithms=["RS256"],
                options={"verify_aud": False, "require": ["exp", "sub"]},
            )
            break
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token expired")
        except Exception:
            continue

    if claims is None:
        raise HTTPException(status_code=401, detail="Invalid token")

    if config.REQUIRED_ROLE:
        realm_roles = claims.get("realm_access", {}).get("roles", [])
        if config.REQUIRED_ROLE not in realm_roles:
            raise HTTPException(
                status_code=403,
                detail=f"Access to reports requires realm role '{config.REQUIRED_ROLE}'",
            )

    return claims.get("user_id") or claims["sub"]
