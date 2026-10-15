import pytest

import confluent_sql
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
