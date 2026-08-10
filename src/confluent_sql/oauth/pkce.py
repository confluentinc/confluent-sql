"""PKCE (RFC 7636) parameter generation for the interactive-OAuth login's Auth0 authorization-code
hop.

Pure functions only -- no I/O, no config, no state. `generate_verifier`/`challenge_for` implement
the S256 code-challenge method; `generate_state` is unrelated to PKCE proper but shares the same
random-token shape and is generated alongside it, so it lives here rather than in its own
one-function module.
"""

from __future__ import annotations

import base64
import hashlib
import secrets


def _random_base64url_token(byte_length: int) -> str:
    return base64.urlsafe_b64encode(secrets.token_bytes(byte_length)).rstrip(b"=").decode("ascii")


def generate_verifier() -> str:
    """A PKCE code_verifier: 32 random bytes, base64url-encoded without padding (43 chars),
    satisfying RFC 7636's 43-128 character length requirement."""
    return _random_base64url_token(32)


def challenge_for(verifier: str) -> str:
    """The S256 PKCE code_challenge for verifier: base64url(SHA-256(ASCII(verifier))), no
    padding."""
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def generate_state() -> str:
    """A random `state` value for CSRF protection on the Auth0 authorization-code flow. Same
    shape as generate_verifier() (32 random bytes, base64url) but a distinct value -- state and
    verifier must never be the same string."""
    return _random_base64url_token(32)
