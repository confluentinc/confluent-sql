import pytest

import confluent_sql
import confluent_sql.oauth
from confluent_sql import oauth_discard_login
from confluent_sql.__version__ import __version__ as version_module_version
from confluent_sql.oauth import discard_login


@pytest.mark.unit
class TestPackageUnit:
    def test_top_level_version_matches_version_module(self):
        assert confluent_sql.__version__ == version_module_version

    def test_oauth_discard_login_is_importable_from_top_level(self):
        """`oauth_discard_login` is the package-level spelling of `oauth.discard_login`, and part of
        the public `__all__`."""
        assert oauth_discard_login is discard_login
        assert "oauth_discard_login" in confluent_sql.__all__

    def test_paramstyle_is_format(self):
        """The driver interpolates `%s` positional placeholders, which DB-API spells `format`."""
        assert confluent_sql.paramstyle == "format"

    def test_oauth_package_exports_only_the_public_surface(self):
        """`confluent_sql.oauth.__all__` lists only the supported names (#313); the machinery behind
        `connect(auth="oauth")` stays importable from its submodules but is not advertised."""
        assert set(confluent_sql.oauth.__all__) == {
            "OAuthMetrics",
            "discard_login",
        }
        for name in confluent_sql.oauth.__all__:
            assert hasattr(confluent_sql.oauth, name)
