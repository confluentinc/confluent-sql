"""The unit/integration markers come from a test's directory (#283), never by hand.

These run a nested pytest session whose conftest borrows the real hook from `tests/conftest.py`.
"""

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

pytest_plugins = ["pytester"]

CONFTEST = (
    "from tests.conftest import (  # noqa: F401\n"
    "    pytest_collection_modifyitems,\n"
    "    pytest_make_collect_report,\n"
    ")\n"
)

BARE_TEST = "def test_it():\n    pass\n"


@pytest.fixture
def marked_session(pytester: pytest.Pytester) -> pytest.Pytester:
    pytester.makeini("[pytest]\nmarkers =\n    unit\n    integration\n    integration_with_mocks\n")
    pytester.makeconftest(CONFTEST)
    return pytester


@pytest.mark.parametrize("directory", ["unit", "integration"])
def test_bare_test_gets_its_directory_marker(marked_session: pytest.Pytester, directory: str):
    marked_session.makepyfile(**{f"{directory}/test_a": BARE_TEST})
    for selected, expected in (("unit", directory == "unit"), ("integration", directory != "unit")):
        result = marked_session.runpytest("-m", selected)
        result.assert_outcomes(passed=1 if expected else 0, deselected=0 if expected else 1)


def test_nested_directory_is_classified_by_its_ancestor(marked_session: pytest.Pytester):
    marked_session.makepyfile(**{"unit/oauth/test_a": BARE_TEST})
    marked_session.runpytest("-m", "unit").assert_outcomes(passed=1)


def test_integration_with_mocks_implies_integration(marked_session: pytest.Pytester):
    marked_session.makepyfile(
        **{
            "integration/test_a": "import pytest\n\n@pytest.mark.integration_with_mocks\n"
            "def test_it():\n    pass\n"
        }
    )
    marked_session.runpytest("-m", "integration").assert_outcomes(passed=1)


def test_test_outside_both_directories_is_an_error(marked_session: pytest.Pytester):
    marked_session.makepyfile(**{"elsewhere/test_a": BARE_TEST})
    result = marked_session.runpytest()
    assert result.ret == pytest.ExitCode.INTERRUPTED
    result.stdout.fnmatch_lines(["*ERROR collecting*", "*neither under unit/ nor integration/*"])


@pytest.mark.parametrize("directory", ["unit", "integration"])
@pytest.mark.parametrize("explicit", ["unit", "integration"])
@pytest.mark.parametrize("scope", ["decorator", "module", "class"])
def test_any_explicit_marker_is_an_error(
    marked_session: pytest.Pytester, directory: str, explicit: str, scope: str
):
    """Even an explicit mark agreeing with the directory is rejected: one way to do it."""
    source = {
        "decorator": f"@pytest.mark.{explicit}\ndef test_it():\n    pass\n",
        "module": f"pytestmark = pytest.mark.{explicit}\n\ndef test_it():\n    pass\n",
        "class": f"@pytest.mark.{explicit}\nclass TestIt:\n    def test_it(self):\n        pass\n",
    }[scope]
    marked_session.makepyfile(**{f"{directory}/test_a": f"import pytest\n\n{source}"})
    result = marked_session.runpytest()
    assert result.ret == pytest.ExitCode.INTERRUPTED
    result.stdout.fnmatch_lines(["*ERROR collecting*", f"*explicitly marked `{explicit}`*"])


def test_error_is_legible_under_xdist(
    marked_session: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
):
    """Regression: raised from `pytest_collection_modifyitems`, this killed the xdist worker and
    surfaced as an INTERNALERROR blaming an unrelated test, hiding the actual message. It must come
    out as an ordinary collection error naming the file and the fix. Needs a real subprocess for
    the workers, which must be able to import `tests.conftest` too."""
    monkeypatch.setenv("PYTHONPATH", str(REPO_ROOT))
    marked_session.makepyfile(
        **{"unit/test_a": "import pytest\n\npytestmark = pytest.mark.unit\n\n" + BARE_TEST}
    )
    result = marked_session.runpytest_subprocess("-n2", "-p", "xdist")
    assert result.ret != pytest.ExitCode.OK  # xdist exits 1 here, plain pytest 2
    result.stdout.fnmatch_lines(["*ERROR collecting*", "*explicitly marked `unit`*remove it*"])
    assert "INTERNALERROR" not in result.stdout.str() + result.stderr.str()
