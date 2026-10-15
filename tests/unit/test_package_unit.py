import pytest

import confluent_sql
from confluent_sql import oauth_shutdown_all
from confluent_sql.__version__ import __version__ as version_module_version
from confluent_sql.oauth import shutdown_all


@pytest.mark.unit
class TestPackageUnit:
    def test_top_level_version_matches_version_module(self):
        assert confluent_sql.__version__ == version_module_version

    def test_oauth_shutdown_all_is_importable_from_top_level(self):
        """`oauth_shutdown_all` is the package-level spelling of `oauth.shutdown_all`, and part of
        the public `__all__`."""
        assert oauth_shutdown_all is shutdown_all
        assert "oauth_shutdown_all" in confluent_sql.__all__
