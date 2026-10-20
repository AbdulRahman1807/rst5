"""Property-based and adversarial edge-case tests for api/ingest_validation.py.

Phase 1 Step 4:
Verifies parse_and_validate_csv against:
1. Universal invariants: function returns list[dict[str, str | None]] or raises CSVValidationError;
   never leaks raw unhandled exceptions (_csv.Error, UnicodeDecodeError, KeyError, etc.).
2. Encodings and line endings: UTF-8 BOM, CRLF, LF, mixed endings, quoted newlines, non-UTF-8 bytes.
3. Structural edge cases: ragged rows (extra columns, short rows), huge fields, NUL bytes, 1-column CSVs.
4. Header validation: blank/whitespace headers, duplicate headers (exact and case/whitespace variations).
5. Explicit dangerous and reserved header names:
   - dataset_id, row_index, _new (bookkeeping properties targeted by loader's ON CREATE SET r += $row)
   - empty string, backtick, 10,000-character header.

Any uncovered gap or defect is written as a strict xfail with a category and reason.
Zero production code is modified in Step 4.
"""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from api.ingest_validation import CSVValidationError, parse_and_validate_csv


# ==============================================================================
# 1. Encodings & Line Endings (Passing & Known Gaps)
# ==============================================================================
def test_csv_empty_bytes_raises_validation_error():
    """Empty payload must raise CSVValidationError."""
    with pytest.raises(CSVValidationError, match="empty file"):
        parse_and_validate_csv(b"")


def test_csv_non_utf8_bytes_raises_validation_error():
    """Non-UTF-8 byte sequences must raise CSVValidationError, not raw UnicodeDecodeError."""
    with pytest.raises(CSVValidationError, match="not valid UTF-8"):
        parse_and_validate_csv(b"\xff\xfe\x00\x00\xff")


def test_csv_crlf_lf_mixed_endings_accepted():
    """CRLF, LF, and mixed line endings must parse identical rows."""
    lf = b"name,age\nAlice,30\nBob,25\n"
    crlf = b"name,age\r\nAlice,30\r\nBob,25\r\n"
    mixed = b"name,age\r\nAlice,30\nBob,25\r\n"

    rows_lf = parse_and_validate_csv(lf)
    rows_crlf = parse_and_validate_csv(crlf)
    rows_mixed = parse_and_validate_csv(mixed)

    assert rows_lf == rows_crlf == rows_mixed
    assert len(rows_lf) == 2
    assert rows_lf[0] == {"name": "Alice", "age": "30"}


def test_csv_quoted_newlines_accepted():
    """Quoted fields containing embedded newlines must be preserved in the field value."""
    raw = b'id,description\n1,"first line\nsecond line"\n2,"simple"\n'
    rows = parse_and_validate_csv(raw)
    assert len(rows) == 2
    assert rows[0]["description"] == "first line\nsecond line"


def test_csv_utf8_bom_stripped():
    """UTF-8 BOM (0xEF 0xBB 0xBF) must be stripped so the first header name is not corrupted."""
    raw = b"\xef\xbb\xbfname,age\nAlice,30\n"
    rows = parse_and_validate_csv(raw)
    # Today, raw.decode('utf-8') produces '\ufeffname', which corrupts downstream Cypher queries
    assert "name" in rows[0], f"Expected 'name' in row keys, found {list(rows[0].keys())}"
    assert "\ufeffname" not in rows[0]


# ==============================================================================
# 2. Header Edge Cases & Explicit Header Analysis
# ==============================================================================
def test_csv_single_column_file_accepted():
    """Single-column CSV with data rows must be accepted."""
    raw = b"single_col\nvalue1\nvalue2\n"
    rows = parse_and_validate_csv(raw)
    assert len(rows) == 2
    assert rows[0] == {"single_col": "value1"}
    assert rows[1] == {"single_col": "value2"}


def test_csv_header_only_raises_validation_error():
    """CSV with only headers and zero data rows must raise CSVValidationError."""
    with pytest.raises(CSVValidationError, match="zero data rows"):
        parse_and_validate_csv(b"col1,col2\n")


def test_csv_blank_and_whitespace_headers_rejected():
    """Headers that are empty or whitespace-only must raise CSVValidationError."""
    # Empty column name in header
    with pytest.raises(CSVValidationError, match="malformed header row"):
        parse_and_validate_csv(b",col2\nval1,val2\n")

    # Whitespace-only column name in header
    with pytest.raises(CSVValidationError, match="malformed header row"):
        parse_and_validate_csv(b"col1,   \nval1,val2\n")


def test_csv_explicit_empty_string_header_rejected():
    """Explicit test: empty string header is rejected today."""
    with pytest.raises(CSVValidationError, match="malformed header row"):
        parse_and_validate_csv(b",val\nfoo,bar\n")


def test_csv_explicit_backtick_header_accepted_today():
    """Explicit test: backtick header is accepted today and mapped to row key."""
    raw = b"`,val\nfoo,bar\n"
    rows = parse_and_validate_csv(raw)
    assert "`" in rows[0]
    assert rows[0]["`"] == "foo"


def test_csv_header_over_max_length_rejected():
    """Column name exceeding MAX_HEADER_LENGTH (128 chars) must be rejected."""
    long_col = "h" * 10000
    raw = f"{long_col},val\nfoo,bar\n".encode("utf-8")
    with pytest.raises(CSVValidationError, match="column name too long"):
        parse_and_validate_csv(raw)


def test_csv_duplicate_headers_exact_rejected():
    """Duplicate header names must raise CSVValidationError (not silently drop earlier columns)."""
    raw = b"name,age,name\nAlice,30,Bob\n"
    with pytest.raises(CSVValidationError, match="duplicate column name"):
        parse_and_validate_csv(raw)


def test_csv_duplicate_headers_case_whitespace_rejected():
    """Headers differing only by case or surrounding whitespace must be rejected."""
    raw = b"Name,age, name \nAlice,30,Bob\n"
    with pytest.raises(CSVValidationError, match="duplicate column name"):
        parse_and_validate_csv(raw)


# ==============================================================================
# 3. Reserved Bookkeeping Header Tests (Loader SET r += $row Overwrite Risk)
# ==============================================================================
def test_csv_reserved_header_dataset_id_rejected():
    """Reserved header 'dataset_id' must be rejected (protects loader node ownership)."""
    raw = b"dataset_id,amount\nattacker_id,100\n"
    with pytest.raises(CSVValidationError, match="reserved"):
        parse_and_validate_csv(raw)


def test_csv_reserved_header_row_index_rejected():
    """Reserved header 'row_index' must be rejected (protects row ordering / idempotency)."""
    raw = b"row_index,amount\n99999,100\n"
    with pytest.raises(CSVValidationError, match="reserved"):
        parse_and_validate_csv(raw)


def test_csv_reserved_header_new_rejected():
    """Reserved header '_new' must be rejected (protects loader's idempotency marker)."""
    raw = b"_new,amount\nfalse,100\n"
    with pytest.raises(CSVValidationError, match="reserved"):
        parse_and_validate_csv(raw)


def test_csv_reserved_headers_case_insensitive():
    """Reserved header check must be case-insensitive: Dataset_ID, ROW_INDEX, _NEW are all blocked."""
    for name in ("Dataset_ID", "ROW_INDEX", "_NEW"):
        raw = f"{name},amount\n1,100\n".encode()
        with pytest.raises(CSVValidationError, match="reserved"):
            parse_and_validate_csv(raw)


# ==============================================================================
# 4. Structural Edge Cases & Unhandled Exception Leaks
# ==============================================================================
def test_csv_ragged_rows_extra_fields_only_rejected():
    """If all rows have extra fields, whole file is rejected with 'no loadable rows'."""
    raw = b"col1,col2\nval1,val2,val3\n"
    with pytest.raises(CSVValidationError, match="no loadable rows"):
        parse_and_validate_csv(raw)


def test_csv_ragged_rows_short_rows_only_rejected():
    """If all rows are short, whole file is rejected with 'no loadable rows'."""
    raw = b"col1,col2,col3\nval1,val2\n"
    with pytest.raises(CSVValidationError, match="no loadable rows"):
        parse_and_validate_csv(raw)


def test_csv_ragged_rows_per_row_classification():
    """Mixed CSV with good and ragged rows classifies rows individually."""
    raw = b"col1,col2\nval1,val2\nextra1,extra2,extra3\nshort1\n"
    parsed = parse_and_validate_csv(raw)
    assert len(parsed) == 3
    assert not parsed[0].failed
    assert parsed[0].row == {"col1": "val1", "col2": "val2"}
    assert parsed[1].failed
    assert parsed[1].reason == "extra_fields"
    assert parsed[1].row == {}
    assert parsed[2].failed
    assert parsed[2].reason == "short_row"
    assert parsed[2].row == {}


def test_csv_quoted_newline_and_tab_cells_accepted():
    """Quoted cell containing newline and cell containing tab parse as valid good rows."""
    raw = b'id,desc,notes\n1,"first line\nsecond line",hello\tworld\n'
    parsed = parse_and_validate_csv(raw)
    assert len(parsed) == 1
    assert not parsed[0].failed
    assert parsed[0].row == {
        "id": "1",
        "desc": "first line\nsecond line",
        "notes": "hello\tworld",
    }


def test_csv_leading_blank_line_rejected():
    """CSV with a leading blank line before the header must raise CSVValidationError."""
    raw = b"\nname,age\nAlice,30\n"
    with pytest.raises(CSVValidationError, match="not a valid CSV"):
        parse_and_validate_csv(raw)


def test_csv_broken_fixture_classification():
    """test_data/broken.csv must yield exactly 6 rows: 2 good and 4 failed."""
    with open("test_data/broken.csv", "rb") as f:
        raw = f.read()
    parsed = parse_and_validate_csv(raw)
    assert len(parsed) == 6
    good_rows = [r for r in parsed if not r.failed]
    failed_rows = [r for r in parsed if r.failed]
    assert len(good_rows) == 2
    assert len(failed_rows) == 4
    assert [r.reason for r in failed_rows] == [
        "extra_fields",
        "short_row",
        "extra_fields",
        "extra_fields",
    ]
    for r in failed_rows:
        assert r.row == {}


def test_csv_huge_field_raises_csv_validation_error():
    """Field exceeding csv.field_size_limit (128KB) must raise CSVValidationError, not unhandled _csv.Error."""
    # 200KB field exceeds default csv.field_size_limit (131072)
    raw = b"col1,col2\nval1," + (b"x" * 200000) + b"\n"
    with pytest.raises(CSVValidationError):
        parse_and_validate_csv(raw)


def test_csv_bare_cr_in_unquoted_field_raises_csv_validation_error():
    """Bare carriage return inside unquoted field must raise CSVValidationError, not unhandled _csv.Error."""
    raw = b"a,b\n1,foo\rbar\n"
    with pytest.raises(CSVValidationError):
        parse_and_validate_csv(raw)


def test_csv_nul_byte_rejected():
    """NUL byte in CSV header must raise CSVValidationError (control-character policy)."""
    raw = b"a\x00b,col2\nval1,val2\n"
    with pytest.raises(CSVValidationError, match="control character"):
        parse_and_validate_csv(raw)


# ==============================================================================
# 5. Hypothesis Fuzzing Property Test
# ==============================================================================
@given(raw=st.binary(max_size=3000))
@settings(max_examples=100)
def test_csv_hypothesis_only_csv_validation_error_or_rows(raw: bytes):
    """Hypothesis property: parse_and_validate_csv strictly returns ParsedCSV or raises CSVValidationError.

    On the success path:
    - Headers and keys are non-empty strings and contain no reserved names.
    - len(parsed) >= 1, matching the number of data rows.
    - At least 1 good row exists (files with 0 good rows raise CSVValidationError).
    - Failed rows carry row={} and a valid reason from FailureReason.
    - Good rows carry dict with keys == headers and string values.
    """
    try:
        parsed = parse_and_validate_csv(raw)
    except CSVValidationError:
        return

    assert len(parsed) >= 1
    good_count = 0
    for parsed_row in parsed:
        assert isinstance(parsed_row.index, int)
        assert isinstance(parsed_row.source_line, int)
        if parsed_row.failed:
            assert parsed_row.row == {}
            assert parsed_row.reason in (
                "extra_fields",
                "short_row",
                "control_char",
            )
        else:
            good_count += 1
            assert parsed_row.reason is None
            assert isinstance(parsed_row.row, dict)
            assert set(parsed_row.row.keys()) == set(parsed.headers)
            for k, v in parsed_row.row.items():
                assert isinstance(k, str) and len(k.strip()) > 0
                assert k.lower() not in ("dataset_id", "row_index", "_new")
                assert isinstance(v, str)

    assert good_count >= 1
