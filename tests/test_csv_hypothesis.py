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


@pytest.mark.xfail(
    strict=True,
    reason="fix in Phase 1: UTF-8 BOM is not stripped by utf-8 decode, leaving \\ufeff in the first header name",
)
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


def test_csv_explicit_10k_char_header_accepted_today():
    """Explicit test: 10,000-character header is accepted today without truncation."""
    long_col = "h" * 10000
    raw = f"{long_col},val\nfoo,bar\n".encode("utf-8")
    rows = parse_and_validate_csv(raw)
    assert long_col in rows[0]
    assert rows[0][long_col] == "foo"


@pytest.mark.xfail(
    strict=True,
    reason="fix in Phase 1: duplicate headers silently drop earlier columns instead of raising CSVValidationError",
)
def test_csv_duplicate_headers_exact_rejected():
    """Duplicate header names cause DictReader to drop data from earlier columns."""
    raw = b"name,age,name\nAlice,30,Bob\n"
    # Today, this passes silently and drops Alice: {'name': 'Bob', 'age': '30'}
    with pytest.raises(CSVValidationError):
        parse_and_validate_csv(raw)


@pytest.mark.xfail(
    strict=True,
    reason="fix in Phase 1: duplicate headers differing only by case or whitespace are not rejected",
)
def test_csv_duplicate_headers_case_whitespace_rejected():
    """Headers differing only by case or surrounding whitespace must be rejected to prevent ambiguous property schemas."""
    raw = b"Name,age, name \nAlice,30,Bob\n"
    # Today, this produces {'Name': 'Alice', 'age': '30', ' name ': 'Bob'}
    with pytest.raises(CSVValidationError):
        parse_and_validate_csv(raw)


# ==============================================================================
# 3. Reserved Bookkeeping Header Tests (Loader SET r += $row Overwrite Risk)
# ==============================================================================
@pytest.mark.xfail(
    strict=True,
    reason=(
        "fix in Phase 1: reserved header 'dataset_id' is accepted by validator; "
        "loader executes 'ON CREATE SET r += $row', overwriting node dataset_id and destroying dataset isolation"
    ),
)
def test_csv_reserved_header_dataset_id_rejected():
    """Reserved header 'dataset_id' must be rejected to prevent overwriting internal node ownership."""
    raw = b"dataset_id,amount\nattacker_id,100\n"
    # Today, accepted by validator -> loader overwrites r.dataset_id
    with pytest.raises(CSVValidationError):
        parse_and_validate_csv(raw)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "fix in Phase 1: reserved header 'row_index' is accepted by validator; "
        "loader executes 'ON CREATE SET r += $row', overwriting node row_index and breaking row ordering/idempotency"
    ),
)
def test_csv_reserved_header_row_index_rejected():
    """Reserved header 'row_index' must be rejected to prevent overwriting internal node index."""
    raw = b"row_index,amount\n99999,100\n"
    # Today, accepted by validator -> loader overwrites r.row_index
    with pytest.raises(CSVValidationError):
        parse_and_validate_csv(raw)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "fix in Phase 1: reserved header '_new' is accepted by validator; "
        "targets internal loader idempotency marker property"
    ),
)
def test_csv_reserved_header_new_rejected():
    """Reserved header '_new' must be rejected to protect loader's internal idempotency marker."""
    raw = b"_new,amount\nfalse,100\n"
    # Today, accepted by validator
    with pytest.raises(CSVValidationError):
        parse_and_validate_csv(raw)


# ==============================================================================
# 4. Structural Edge Cases & Unhandled Exception Leaks
# ==============================================================================
@pytest.mark.xfail(
    strict=True,
    reason=(
        "fix in Phase 1: ragged row with extra fields produces None key in row dict ({None: ['val3']}) "
        "instead of raising CSVValidationError"
    ),
)
def test_csv_ragged_rows_extra_fields_rejected():
    """Ragged row with more columns than header must raise CSVValidationError."""
    raw = b"col1,col2\nval1,val2,val3\n"
    # Today, DictReader creates {None: ['val3']}, violating dict[str, str | None] and corrupting JSON serialization
    with pytest.raises(CSVValidationError):
        parse_and_validate_csv(raw)


@pytest.mark.xfail(
    strict=True,
    reason="fix in Phase 1: ragged row with short rows introduces None values without raising CSVValidationError",
)
def test_csv_ragged_rows_short_rows_rejected():
    """Ragged row with fewer columns than header must raise CSVValidationError."""
    raw = b"col1,col2,col3\nval1,val2\n"
    # Today, DictReader fills missing keys with None: {'col1': 'val1', 'col2': 'val2', 'col3': None}
    with pytest.raises(CSVValidationError):
        parse_and_validate_csv(raw)


@pytest.mark.xfail(
    strict=True,
    reason="fix in Phase 1: huge field raises unhandled _csv.Error instead of CSVValidationError",
)
def test_csv_huge_field_raises_csv_validation_error():
    """Field exceeding csv.field_size_limit (128KB) must raise CSVValidationError, not unhandled _csv.Error."""
    # 200KB field exceeds default csv.field_size_limit (131072)
    raw = b"col1,col2\nval1," + (b"x" * 200000) + b"\n"
    with pytest.raises(CSVValidationError):
        parse_and_validate_csv(raw)


@pytest.mark.xfail(
    strict=True,
    reason="fix in Phase 1: bare CR (\\r) in unquoted field raises unhandled _csv.Error instead of CSVValidationError",
)
def test_csv_bare_cr_in_unquoted_field_raises_csv_validation_error():
    """Bare carriage return inside unquoted field must raise CSVValidationError, not unhandled _csv.Error."""
    raw = b"a,b\n1,foo\rbar\n"
    with pytest.raises(CSVValidationError):
        parse_and_validate_csv(raw)


@pytest.mark.xfail(
    strict=True,
    reason="fix in Phase 1: NUL bytes (\\x00) in CSV header or values must be rejected by CSVValidationError",
)
def test_csv_nul_byte_rejected():
    """NUL byte in CSV header or field values must raise CSVValidationError."""
    raw = b"a\x00b,col2\nval1,val2\n"
    with pytest.raises(CSVValidationError):
        parse_and_validate_csv(raw)


# ==============================================================================
# 5. Hypothesis Fuzzing Property Test
# ==============================================================================
@pytest.mark.xfail(
    strict=True,
    reason="fix in Phase 1: arbitrary binary fuzzing leaks unhandled _csv.Error on bare CR or huge fields",
)
@given(raw=st.binary(max_size=3000))
@settings(max_examples=100)
def test_csv_hypothesis_only_csv_validation_error_or_rows(raw: bytes):
    """Hypothesis property: parse_and_validate_csv only ever returns list[dict] or raises CSVValidationError.

    It must NEVER raise raw unhandled _csv.Error, UnicodeDecodeError, or any other exception.
    """
    res = parse_and_validate_csv(raw)
    assert isinstance(res, list)
    assert len(res) > 0
    for row in res:
        assert isinstance(row, dict)
        for k in row.keys():
            assert isinstance(k, str), f"Row key must be str, got {type(k)}: {k}"
            assert len(k.strip()) > 0
