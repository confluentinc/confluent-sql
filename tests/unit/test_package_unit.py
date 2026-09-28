import pytest

import confluent_sql
from confluent_sql.__version__ import __version__ as version_module_version


@pytest.mark.unit
class TestPackageUnit:
    def test_top_level_version_matches_version_module(self):
        assert confluent_sql.__version__ == version_module_version
