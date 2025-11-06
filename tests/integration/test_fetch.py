from typing import Any, TypeAlias

import pytest

DictResults: TypeAlias = list[dict[str, Any]]
TupleResults: TypeAlias = list[tuple[Any, ...]]


@pytest.mark.parametrize("as_dict", [True, False])
def test_fetchall_gets_all_results(
    as_dict: bool,
    cursor_with_nonstreaming_data_factory,
    expected_nonstreaming_data_dicts: DictResults,
    expected_nonstreaming_data_tuples: TupleResults,
):
    if as_dict:
        expected_results = expected_nonstreaming_data_dicts
    else:
        expected_results = expected_nonstreaming_data_tuples

    cursor = cursor_with_nonstreaming_data_factory(as_dict=as_dict)
    results = cursor.fetchall()
    assert results == expected_results


@pytest.mark.parametrize("as_dict", [True, False])
def test_fetchone_returns_none_at_the_end(
    cursor_with_nonstreaming_data_factory,
    as_dict: bool,
    expected_nonstreaming_data_dicts: DictResults,
    expected_nonstreaming_data_tuples: TupleResults,
):
    if as_dict:
        expected_results = expected_nonstreaming_data_dicts
    else:
        expected_results = expected_nonstreaming_data_tuples

    cursor = cursor_with_nonstreaming_data_factory(as_dict=as_dict)

    # Exhaust all rows first
    for expected_row in expected_results:
        row = cursor.fetchone()
        assert row == expected_row

    # Should get None after that
    row = cursor.fetchone()
    assert row is None


@pytest.mark.parametrize("as_dict", [True, False])
def test_fetchmany_iteration(
    as_dict: bool,
    cursor_with_nonstreaming_data_factory,
    expected_nonstreaming_data_dicts: DictResults,
    expected_nonstreaming_data_tuples: TupleResults,
):
    if as_dict:
        expected_results = expected_nonstreaming_data_dicts
    else:
        expected_results = expected_nonstreaming_data_tuples

    cursor = cursor_with_nonstreaming_data_factory(as_dict=as_dict)

    # Fetch in batches
    batch1 = cursor.fetchmany(4)
    assert batch1 == expected_results[:4]

    batch2 = cursor.fetchmany(4)
    assert batch2 == expected_results[4:8]

    batch3 = cursor.fetchmany(4)
    assert batch3 == expected_results[8:10]

    batch4 = cursor.fetchmany(4)
    assert len(batch4) == 0  # No more rows


@pytest.mark.parametrize("as_dict", [True, False])
def test_cursor_as_iterator(
    as_dict: bool,
    cursor_with_nonstreaming_data_factory,
    expected_nonstreaming_data_dicts: DictResults,
    expected_nonstreaming_data_tuples: TupleResults,
):
    expected_results: DictResults | TupleResults
    if as_dict:
        expected_results = expected_nonstreaming_data_dicts
    else:
        expected_results = expected_nonstreaming_data_tuples

    cursor_with_nonstreaming_data = cursor_with_nonstreaming_data_factory(
        as_dict=as_dict
    )

    for row in cursor_with_nonstreaming_data:
        expected_row = expected_results.pop(0)
        assert row == expected_row
