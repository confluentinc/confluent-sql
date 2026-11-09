"""Interactive human-login OAuth for Confluent Cloud (#150).

The supported surface of this package is small, and is exactly what `__all__` lists:
`OAuthMetrics` (what `Connection.oauth_metrics` returns) and `discard_login()` (also exported at
the top level as `confluent_sql.oauth_discard_login`). Sign-in itself is `connect(auth="oauth")`.

Everything else in the subpackage is internal machinery (including the per-environment
`config` module: `CCloudOAuthConfig`, `PROD`, `STAG`, `DEVEL`) behind that call and may change
without notice, even in a minor release: the three-hop PKCE token chain (`token_chain`, `pkce`,
`token_set`), the loopback `CallbackServer`, `CCloudOAuth` (the provider that drives a login end
to end and keeps the tokens current), and the process-wide holder (`holder`: `acquire()`,
`reauthenticate()`, `ProcessOAuthHolder`) that makes the login singular across a process. Import
those from their submodules if you must, knowing they are not a supported API.
"""

from __future__ import annotations

from .holder import discard_login
from .provider import OAuthMetrics

__all__ = [
    "OAuthMetrics",
    "discard_login",
]
