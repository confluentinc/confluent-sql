# Type Support

The `confluent_sql` dbapi driver supports all Flink types as statement results return types, and most types as parameterized query parameters, with gaps and caveats documented in the following table.

This driver is a REST API client for the [Confluent Cloud Statements v1](<https://docs.confluent.io/cloud/current/api.html#tag/Statements-(sqlv1)>) and [Statement Results](<https://docs.confluent.io/cloud/current/api.html#tag/Statement-Results-(sqlv1)>) APIs. Given that those APIs do not at this time offer a separation between a paramaterized statement and the paramameters, parameter interpolation is done within this driver prior to statement submission, escaping when necessary.

At time of writing, Flink is not able to infer the type of a SQL literal based on the context within the statement (such as "only an integer can be valid for this column"). This implies that bare `NULL` or other "untyped" SQL literals must be explicitly cast to the underlying type. The driver provides a `SqlNull` type to assist with this for `NULL` values.

| Flink Type                                        | Python Type                                              | Flink SQL Literal Example(s)                          | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ------------------------------------------------- | -------------------------------------------------------- | ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `BOOLEAN`                                         | `bool`                                                   | `TRUE`                                                |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `TINYINT` / `SMALLINT` / `INTEGER` / `BIGINT`     | `int`                                                    | `42`                                                  |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `DECIMAL`, `DEC`, `NUMERIC`                       | `decimal.Decimal`                                        | `cast('123.45' as decimal(5,2))`                      | `Decimal` precision/scale must not exceed column definition; driver emits explicit `CAST`.                                                                                                                                                                                                                                                                                                                                                                                           |
| `FLOAT`, `DOUBLE`, `DOUBLE PRECISION`             | `float`                                                  | `3.14159`                                             | Driver rejects `math.isinf`/`math.isnan` inputs even though API may return them, so the mapping is not symmetric, due to a current limitation in the Flink grammar.                                                                                                                                                                                                                                                                                                                  |
| `CHAR`, `VARCHAR`, `STRING`                       | `str`                                                    | `'James'`, `'O''Brien'`                               | Only Python `str` allowed; any `bytes` must be explicitly decoded because string literal escaping is handled assuming Unicode text.                                                                                                                                                                                                                                                                                                                                                  |
| `VARBINARY`, `BINARY`, `BYTES`                    | `bytes`                                                  | `x'7f0203'`                                           | Requires actual `bytes`; Python `bytearray`/`memoryview` must be converted manually.                                                                                                                                                                                                                                                                                                                                                                                                 |
| `DATE`                                            | `datetime.date`                                          | `DATE '2024-01-14'`                                   | Must only provide `datetime.date` instances to be serialized as a Flink DATE -- never `datetime.datetime` instances.                                                                                                                                                                                                                                                                                                                                                                 |
| `TIME`, `TIME_WITHOUT_TIME_ZONE`                  | `datetime.time`                                          | `TIME '12:34:56.123456'`                              | Only tz-naive `time` supported.                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `TIMESTAMP`, `TIMESTAMP_WITHOUT_TIME_ZONE`        | `datetime.datetime` (naive)                              | `cast('2024-01-14 12:34:56.123456' as timestamp)`     | Provide timzeone-lacking `datetime.datetime` instances.                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `TIMESTAMP_LTZ`, `TIMESTAMP_WITH_LOCAL_TIME_ZONE` | `datetime.datetime` (tz-aware)                           | `cast('2024-01-14 12:34:56.123456' as timestamp_ltz)` | Only tz-aware datetimes allowed on input; driver normalizes to UTC and returns UTC-aware datetimes, so original timezone cannot be recovered (asymmetry). Flink lacks a true `TIMESTAMP WITH TIME ZONE` type.                                                                                                                                                                                                                                                                        |
| `INTERVAL YEAR TO MONTH` / `INTERVAL_YEAR_MONTH`  | `YearMonthInterval`                                      | `INTERVAL '+1-06' YEAR TO MONTH`                      | Must supply the driver’s `YearMonthInterval` dataclass; Python lacks native type for this Flink type.                                                                                                                                                                                                                                                                                                                                                                                |
| `INTERVAL DAY TO SECOND` / `INTERVAL_DAY_TIME`    | `datetime.timedelta`                                     | `INTERVAL '+0 04:05:06.000100' DAY TO SECOND(6)`      | `timedelta` Python -> Flink precision capped at microseconds due to Flink parser limitation.                                                                                                                                                                                                                                                                                                                                                                                         |
| `ARRAY`                                           | `list`                                                   | `ARRAY[1, 2, 3]`                                      | Python list must contain at least one non-`None` element so element type can be inferred; all-`None` or empty lists cannot be serialized (asymmetry vs. result decoding) as a parameter. Lists must contain a single uniform supported python type (corresponding to the ARRAY's scalar type) aside from `None` values, assuming the ARRAY scalar type is nullable.                                                                                                                  |
| `MAP`                                             | `dict`                                                   | `MAP['k1', 1, 'k2', 2]`                               | Flink MAPs are strongly typed on both the key and value type, so heterogenous python dicts are disallowed (outside of `None` keys or values, if the MAP key or value type includes nullability). Provided `dict`s cannot contain only-`None` keys or values, due to the need to infer the type for the corresponding key or value `NULL`.                                                                                                                                            |
| `MULTISET`                                        | `collections.Counter`                                    | _(n/a)_                                               | Driver can deserialize results into `Counter`, but Flink cannot accept MULTISET literals, so submitting `Counter` parameters is unsupported. Any supported python type corresponding to the MULTISET's key type is supported.                                                                                                                                                                                                                                                        |
| `ROW`                                             | `tuple`, `namedtuple`, `typing.NamedTuple`, `@dataclass` | `ROW(1, 'foo')`                                       | Parameter `ROW` values require `tuple`, `namedtuple`, `NamedTuple` or `@dataclass` instances, and are transcribed to a literal `ROW` values positionally. Statement results containing `ROW` members will by default be mapped to auto-generated `collections.namedtuple` classes with named fields. Users may register their own `collections.namedtuple`, `typing.NamedTuple`, or `@dataclass` for use for `ROW` result mapping using the `connection.register_row_type()` method. |
| `NULL`                                            | `None`                                                   | _(n/a)_                                               | Only produced by results; cannot be sent because Flink needs typed NULLs—use `SqlNone` instead.                                                                                                                                                                                                                                                                                                                                                                                      |
| _(any typed NULL)_                                | `SqlNone`                                                | `cast(null as INTEGER)`                               | `SqlNone` enforces Flink type names; parameterized types must match regex (e.g., `ROW<...>`). Results never return `SqlNone`, so mapping is one-way.                                                                                                                                                                                                                                                                                                                                 |

## Code Samples Demonstating Parameter Interpolation and Result Typing

Most of the following examples are lifted from `src/tests/integration/test_fetch.py`:

### Decoding Most Scalar Types

```
with connection.closing_cursor(as_dict=True) as cursor:
    cursor.execute("""
        SELECT *
        FROM (VALUES (
                cast(12 AS TINYINT),
                cast(12345 AS SMALLINT),
                cast(123 AS INT),
                cast(12345678901 AS BIGINT),

                cast ('123.323' AS DECIMAL(10,3)),
                cast ('54.5' as DOUBLE),

                TRUE,

                DATE '2024-06-15',
                TIME '12:34:56',

                cast('2024-06-15 12:34:56' as TIMESTAMP),
                cast('2023-06-15 12:34:56' as TIMESTAMP_LTZ), -- will be interp as GMT.

                cast ('c' as CHAR),
                cast ('charn' as CHAR(5)),
                cast ('string' as STRING),
                cast ('varchar' as VARCHAR),
                cast ('varcharn' as VARCHAR(10)),

                cast(x'7f0203' AS VARBINARY),

                cast(NULL AS INTEGER),
                cast(NULL AS BOOLEAN),
                cast(NULL AS STRING)
            ))
        AS t(
                tinyint_value,
                smallint_value,
                int_value,
                bigint_value,

                decimal_value,
                double_value,

                bool_value,
                date_value,
                time_value,

                timestamp_value,
                timestamp_ltz_value,

                char_value,
                charn_value,
                string_value,
                varchar_value,
                varcharn_value,

                varbinary_value,

                null_int_value,
                null_bool_value,
                null_string_value
            )
        """)

    results = cursor.fetchone()

    assert results == {
        # Numeric types
        "tinyint_value": 12,
        "smallint_value": 12345,
        "int_value": 123,
        "bigint_value": 12345678901,
        # Fixed precision type
        "decimal_value": Decimal("123.323"),
        # Floating point type
        "double_value": 54.5,
        # Boolean type
        "bool_value": True,
        # date / time types
        "date_value": date(2024, 6, 15),
        "time_value": time(12, 34, 56),
        "timestamp_value": datetime(2024, 6, 15, 12, 34, 56),
        "timestamp_ltz_value": datetime(
            2023, 6, 15, 12, 34, 56, tzinfo=timezone(timedelta(hours=0))
        ),
        # String types
        "char_value": "c",
        "charn_value": "charn",
        "string_value": "string",
        "varchar_value": "varchar",
        "varcharn_value": "varcharn",
        # VarBinary type
        "varbinary_value": b"\x7f\x02\x03",
        # Various NULLs
        "null_int_value": None,
        "null_bool_value": None,
        "null_string_value": None,
    }
```

### `ARRAY` <--> `list` parameter encoding and results decoding

```
with connection.closing_cursor(as_dict=True) as cursor:
    cursor.execute(
        """
        SELECT
            %s AS int_array_value,
            %s AS string_array_value,
            %s as nested_string_array,
            %s AS null_array_value
        """,
        (
            [1, 2, 3, 4, 5],  # ARRAY of INT
            ["one", "two", "three"],  # ARRAY of STRING
            [["one", "two"], ["three", "four"]],  # NESTED ARRAY of STRING
            SqlNone("Array<int>"),  # NULL INT ARRAY
        ),
    )

    results = cursor.fetchone()

    assert results == {
        "int_array_value": [1, 2, 3, 4, 5],
        "string_array_value": ["one", "two", "three"],
        "nested_string_array": [["one", "two"], ["three", "four"]],
        "null_array_value": None,
    }
```

### `MAP` <--> `dict` Parameter Encoding and Results Decoding

```
with connection.closing_cursor(as_dict=True) as cursor:
    cursor.execute(
        """
        SELECT
            %s AS string_int_map,
            %s as int_string_map,
            %s as string_null_int_map,
            %s AS string_array_of_string_map,
            %s AS nested_map,
            %s AS null_map
        """,
        (
            {"key1": 10, "key2": 20, "key3": 30},  # MAP of string -> INT values
            {10: "ten", 20: "twenty"},  # MAP of INT -> STRING values
            {"key1": 10, "null_key": None},  # MAP string -> NULLABLE INT value
            {
                "fruits": ["apple", "banana"],
                "vegetables": ["carrot", "broccoli"],
            },  # MAP of string -> ARRAY of string
            {
                "outer_key1": {"inner_key1": 1, "inner_key2": 2},
                "outer_key2": {"inner_key3": 3, "inner_key4": 4},
            },  # MAP nesting: MAP<STRING, MAP<STRING, INTEGER>>
            SqlNone("Map<string, int>"),  # NULL MAP
        ),
    )

    results = cursor.fetchone()

    assert results == {
        "string_int_map": {"key1": 10, "key2": 20, "key3": 30},
        "int_string_map": {10: "ten", 20: "twenty"},
        "string_null_int_map": {"key1": 10, "null_key": None},
        "string_array_of_string_map": {
            "fruits": ["apple", "banana"],
            "vegetables": ["carrot", "broccoli"],
        },
        "nested_map": {
            "outer_key1": {"inner_key1": 1, "inner_key2": 2},
            "outer_key2": {"inner_key3": 3, "inner_key4": 4},
        },
        "null_map": None,
    }
```

### `MULTISET` Results -> `collection.Counter`

This example makes use of the Flink `COLLECT` aggregate to produce the `MULTISET` value:

```
with connection.closing_cursor(as_dict=True) as cursor:

    cursor.execute(
        # Two apples, two bananas, one orange.
        """
        WITH fruits AS (
            SELECT *
            FROM (VALUES
                ('apple'),
                ('banana'),
                ('apple'),
                ('orange'),
                ('banana')
            ) AS t(fruit)
        )

        SELECT COLLECT (fruit) AS fruit_multiset
        FROM fruits
        """
    )

    results = cursor.fetchone()

    assert results == {
        "fruit_multiset": Counter({"apple": 2, "banana": 2, "orange": 1}),
    }
```

### `SqlNone` Typed Scalar Constants

These constants will expand to include the `cast` to the proper underlying scalar type and are most useful when needing to `INSERT` such typed values into a table, but here's a simple round-tripping example ...

```
with connection.closing_cursor(as_dict=True) as cursor:
    cursor.execute(
        """
        SELECT
            %s AS null_int,
            %s as null_varchar,
            %s as null_string,
            %s as null_varbinary,
            %s as null_bool,
            %s as null_decimal,
            %s as null_float,
            %s as null_date,
            %s as null_time,
            %s as null_timestamp,
            %s as null_year_month_interval,
            %s as null_day_second_interval
        """,
        (
            SqlNone.INTEGER,
            SqlNone.VARCHAR,
            SqlNone.STRING,
            SqlNone.VARBINARY,
            SqlNone.BOOLEAN,
            SqlNone.DECIMAL,
            SqlNone.FLOAT,
            SqlNone.DATE,
            SqlNone.TIME,
            SqlNone.TIMESTAMP,
            SqlNone.YEAR_MONTH_INTERVAL,
            SqlNone.DAY_SECOND_INTERVAL,
        ),
    )

    results = cursor.fetchone()

    assert results == {
        "null_int": None,
        "null_varchar": None,
        "null_string": None,
        "null_bool": None,
        "null_varbinary": None,
        "null_decimal": None,
        "null_float": None,
        "null_date": None,
        "null_time": None,
        "null_timestamp": None,
        "null_year_month_interval": None,
        "null_day_second_interval": None,
    }
```

To make a `NULL` for a nonscalar type, use the constructor, passing the Flink nonscalar type as a string:

```
    null_int_array = SqlNone("Array<int>")

    with connection.closing_cursor(as_dict=True) as cursor:
        cursor.execute("insert into mytab(nullable_int_array_column) values (%s)", (null_int_array,))
```

### `ROW` Examples

Given the existing table structure:

```
create table table_with_rows(
    simple_row ROW(a INTEGER not null, b STRING not null)
)
```

Parameter values may be provided as any of `tuple`, `collections.namedtuple`, `typing.NamedTuple`, or `@dataclass` values. They will be expanded positionally based on their declared field order:

```
from collections import namedtuple
from typing import NamedTuple
from dataclasses import dataclass

...

SimpleRow = namedtuple("SimpleRow", ["a", "b"])
sr = SimpleRow(a=12, b="foonly")

with connection.closing_cursor(as_dict=True) as cursor:
    cursor.execute("insert into table_with_row(simple_row) values (%s)", (sr,))
```

In the above example, the `sr` value will expand positionally to Flink literal `ROW(12, 'foonly')`. The same exact expansion would have happened had `sr` been a two-valued `tuple`, or a `typing.NamedTuple` subclass, or an `@dataclass` subclass:

```
sr_tuple = (15, 'from a tuple')

class TypedNamedTuple(NamedTuple):
    a: int
    b: str

tnt = TypedNamedTuple(19, 'from a typing.NamedTuple`)

@dataclass
class DataclassRow:
    a: int
    b: str

dcr = DataclassRow(a=21, b="from a dataclass instance')

with connection.closing_cursor(as_dict=True) as cursor:
    # insert three more rows given the varying types, all going into the two field ROW column.
    cursor.execute("insert into table_with_row(simple_row) values (%s), (%s), (%s)", (sr_tuple, tnt, dcr))


```

By default, ROW column results will be returned as an autogenerated `collections.namedtuple` with corresponding field names:

```
...
with connection.closing_cursor(as_dict=True) as cursor:
    cursor.execute('select * from table_with_row where simple_row.a = 12')

    result_dict = cursor.fetchone()
    row_value = result_dict['simple_row']

    assert row_value.a == row.value[0] == 12
    assert row_value.b == row.value[1] == 'foonly'


```

By default, the auto-generated `collections.namedtule` will be reused for all result rows whose field names are identical:

```
    ...
    with connection.closing_cursor(as_dict=True) as cursor:
        cursor.execute('select * from table_with_row where simple_row.a = 15')

        result_dict = cursor.fetchone()
        second_row_value = result_dict['simple_row']

        assert second_row_value.b == 'from a tuple'

        assert type(second_row_value) == type(row_value)
```

But users can register what `namedtuple`, `NamedTuple`, or `@dataclass` to use for the results, matched by field names:

```
    ...

    # Registers to handle any subsequent ROW value decoding based on field names ('a', 'b')
    connection.register_row_type(DataclassRow)

    with connection.closing_cursor(as_dict=True) as cursor:
        cursor.execute('select * from table_with_row where simple_row.a = 15')

        # Will now be expressed as a DataclassRow instance
        second_row_value = result_dict['simple_row']

        assert isinstance(second_row_value, DataclassRow)
        assert second_row_value.b == 'from a tuple'

```
