import csv
import io
import unicodedata

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

from dataclasses import dataclass
from typing import Any, Mapping

# ---------------------------------------------------------------------------
# Constants & Enums
# ---------------------------------------------------------------------------

MAX_FIELD_SIZE_BYTES: int = 128 * 1024  # 128 KB limit per cell
csv.field_size_limit(MAX_FIELD_SIZE_BYTES)

MAX_HEADER_LENGTH: int = 128       # characters per column name
MAX_COLUMN_COUNT: int = 200        # columns per CSV

# Case-insensitive reserved bookkeeping property names injected by the loader.
# A CSV column that matches one of these would overwrite internal node metadata
# via  ON CREATE SET r += $row  and break dataset isolation or idempotency.
_RESERVED_HEADERS: frozenset[str] = frozenset({"dataset_id", "row_index", "_new"})


class RowFailureReason:
    EXTRA_FIELDS = "extra_fields"
    SHORT_ROW = "short_row"
    CONTROL_CHAR = "control_char"


class CSVValidationError(ValueError):
    """Raised when CSV content fails validation."""

    pass


@dataclass
class ClassifiedRow(Mapping[str, Any]):
    """Parsed CSV row with status classification and sanitized dictionary representation.

    Inherits from Mapping so it can be indexed like a dictionary (`row['col']`),
    compared directly to dicts (`row == {'col': 'val'}`), and checked with `in`.
    When failed=True, row is guaranteed to be `{}` (never None keys).
    """

    row_index: int
    failed: bool
    reason: str | None
    row: dict[str, str]

    def __getitem__(self, key: str) -> Any:
        return self.row[key]

    def __iter__(self):
        return iter(self.row)

    def __len__(self) -> int:
        return len(self.row)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, dict):
            return self.row == other
        if isinstance(other, ClassifiedRow):
            return (
                self.row_index == other.row_index
                and self.failed == other.failed
                and self.reason == other.reason
                and self.row == other.row
            )
        return False


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _contains_control_char(s: str, allow_whitespace: bool = False) -> bool:
    """Return True if *s* contains any ASCII control character or NUL.

    When allow_whitespace=True, standard whitespace characters (\\t, \\n, \\r)
    are permitted (e.g. inside quoted multiline cell values).
    """
    for ch in s:
        if allow_whitespace and ch in ("\t", "\n", "\r"):
            continue
        if unicodedata.category(ch) == "Cc":
            return True
    return False


def _validate_headers(fieldnames: list[str]) -> None:
    """Validate column names against the full header policy.

    Raises CSVValidationError with a descriptive message on the first violation.
    """
    # ------------------------------------------------------------------
    # 1. Column count
    # ------------------------------------------------------------------
    if len(fieldnames) > MAX_COLUMN_COUNT:
        raise CSVValidationError(
            f"too many columns: {len(fieldnames)} > {MAX_COLUMN_COUNT}"
        )

    seen_canonical: dict[str, str] = {}  # canonical → first original name

    for raw_name in fieldnames:
        # ------------------------------------------------------------------
        # 2. Blank / whitespace-only
        # ------------------------------------------------------------------
        if not raw_name or not raw_name.strip():
            raise CSVValidationError("not a valid CSV (malformed header row)")

        name: str = raw_name

        # ------------------------------------------------------------------
        # 3. Max header length
        # ------------------------------------------------------------------
        if len(name) > MAX_HEADER_LENGTH:
            raise CSVValidationError(
                f"column name too long ({len(name)} chars > {MAX_HEADER_LENGTH}): "
                f"{name[:32]!r}…"
            )

        # ------------------------------------------------------------------
        # 4. Control characters (incl. NUL \x00, \n, \r, \t)
        # ------------------------------------------------------------------
        if _contains_control_char(name, allow_whitespace=False):
            raise CSVValidationError(
                f"column name contains a control character: {name!r}"
            )

        # ------------------------------------------------------------------
        # 5. Reserved bookkeeping names (case-insensitive)
        # ------------------------------------------------------------------
        if name.strip().lower() in _RESERVED_HEADERS:
            raise CSVValidationError(
                f"column name is reserved and cannot be used in uploaded CSVs: {name!r}"
            )

        # ------------------------------------------------------------------
        # 6. Duplicate detection: exact AND case-insensitive after strip
        # ------------------------------------------------------------------
        canonical = name.strip().lower()
        if canonical in seen_canonical:
            raise CSVValidationError(
                f"duplicate column name (case-insensitive): "
                f"{name!r} conflicts with {seen_canonical[canonical]!r}"
            )
        seen_canonical[canonical] = name


def _is_blank_line(row_fields: list[str]) -> bool:
    """Determine if a row produced by csv.reader is an ignorable blank line.

    A row is blank if empty or containing only whitespace in a single field.
    """
    return len(row_fields) == 0 or (len(row_fields) == 1 and not row_fields[0].strip())


def classify_row(row_fields: list[str], header: list[str], row_index: int) -> ClassifiedRow:
    """Pure classification function for a single CSV data row.

    Determines if row is valid or failed with reason:
    - 'extra_fields': len(row_fields) > len(header)
    - 'short_row': len(row_fields) < len(header)
    - 'control_char': any cell contains ASCII control characters or NUL (excluding \t, \n, \r)
    - valid: returns mapped dict {header_col: val}
    """
    if len(row_fields) > len(header):
        return ClassifiedRow(
            row_index=row_index,
            failed=True,
            reason=RowFailureReason.EXTRA_FIELDS,
            row={},
        )
    if len(row_fields) < len(header):
        return ClassifiedRow(
            row_index=row_index,
            failed=True,
            reason=RowFailureReason.SHORT_ROW,
            row={},
        )
    if any(_contains_control_char(val, allow_whitespace=True) for val in row_fields):
        return ClassifiedRow(
            row_index=row_index,
            failed=True,
            reason=RowFailureReason.CONTROL_CHAR,
            row={},
        )
    return ClassifiedRow(
        row_index=row_index,
        failed=False,
        reason=None,
        row=dict(zip(header, row_fields)),
    )



# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_and_validate_csv(raw: bytes) -> list[ClassifiedRow]:
    """Parse and validate uploaded CSV bytes.

    Returns a list of ClassifiedRow records on success.
    Header errors raise CSVValidationError (reject whole file).
    Row-level problems (extra columns, short rows, control characters)
    are classified as failed rows on a per-row basis.

    Never raises raw csv.Error, UnicodeDecodeError, or KeyError.
    """
    if not raw:
        raise CSVValidationError("empty file")

    # ------------------------------------------------------------------
    # Decode: strip UTF-8 BOM automatically; reject non-UTF-8 blobs.
    # ------------------------------------------------------------------
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise CSVValidationError("file is not valid UTF-8 text/CSV")

    # ------------------------------------------------------------------
    # Parse with csv.reader: wrap any csv module exception.
    # ------------------------------------------------------------------
    try:
        raw_rows = list(csv.reader(io.StringIO(text)))
    except csv.Error as e:
        raise CSVValidationError(f"malformed CSV syntax: {e}") from e

    # ------------------------------------------------------------------
    # Blank-line policy: lines with no content are omitted.
    # ------------------------------------------------------------------
    non_blank_rows = [r for r in raw_rows if not _is_blank_line(r)]
    if not non_blank_rows:
        raise CSVValidationError("not a valid CSV (no header row)")

    header = non_blank_rows[0]
    data_rows = non_blank_rows[1:]

    # ------------------------------------------------------------------
    # Header structural checks and policy.
    # ------------------------------------------------------------------
    if not header or any(f is None or not f.strip() for f in header):
        raise CSVValidationError("not a valid CSV (malformed header row)")

    _validate_headers(header)

    if not data_rows:
        raise CSVValidationError("CSV has a header but zero data rows")

    # ------------------------------------------------------------------
    # Pure row classification.
    # ------------------------------------------------------------------
    return [
        classify_row(row_fields, header, row_index=i)
        for i, row_fields in enumerate(data_rows)
    ]


