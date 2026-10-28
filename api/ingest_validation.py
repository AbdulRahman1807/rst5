import csv
import io
import unicodedata
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_FIELD_SIZE_BYTES: int = 128 * 1024  # 128 KB limit per cell
csv.field_size_limit(MAX_FIELD_SIZE_BYTES)

MAX_HEADER_LENGTH: int = 128  # characters per column name
MAX_COLUMN_COUNT: int = 200  # columns per CSV

# Case-insensitive reserved bookkeeping property names injected by the loader.
# A CSV column that matches one of these would overwrite internal node metadata
# via  ON CREATE SET r += $row  and break dataset isolation or idempotency.
_RESERVED_HEADERS: frozenset[str] = frozenset({"dataset_id", "row_index", "_new"})


class FailureReason(str, Enum):
    EXTRA_FIELDS = "extra_fields"
    SHORT_ROW = "short_row"
    CONTROL_CHAR = "control_char"


class CSVValidationError(ValueError):
    """Raised when CSV content fails validation."""

    pass


@dataclass(frozen=True)
class ParsedRow:
    index: int
    row: dict[str, str]
    failed: bool
    reason: str | None
    source_line: int

    def __getitem__(self, key: str) -> str:
        return self.row[key]

    def __contains__(self, key: str) -> bool:
        return key in self.row

    def keys(self):
        return self.row.keys()

    def values(self):
        return self.row.values()

    def items(self):
        return self.row.items()

    def get(self, key: str, default=None):
        return self.row.get(key, default)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, dict):
            return self.row == other
        if isinstance(other, ParsedRow):
            return (
                self.index == other.index
                and self.row == other.row
                and self.failed == other.failed
                and self.reason == other.reason
                and self.source_line == other.source_line
            )
        return False


@dataclass(frozen=True)
class ParsedCSV:
    headers: list[str]
    rows: list[ParsedRow]

    def __iter__(self):
        return iter(self.rows)

    def __len__(self) -> int:
        return len(self.rows)

    def __getitem__(self, idx):
        return self.rows[idx]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _contains_control_char(s: str) -> bool:
    """Return True if *s* contains any ASCII control character or NUL."""
    return any(unicodedata.category(ch) == "Cc" for ch in s)


def _is_value_control_char(ch: str) -> bool:
    """Return True if *ch* is NUL or a C0 control char, EXCEPT \t, \n, and \r."""
    if ch in ("\t", "\n", "\r"):
        return False
    cp = ord(ch)
    return cp < 0x20 or cp == 0x7F


def _has_value_control_char(s: str) -> bool:
    return any(_is_value_control_char(ch) for ch in s)


def _validate_headers(fieldnames: Sequence[str | None]) -> None:
    """Validate column names against the full header policy.

    Raises CSVValidationError with a descriptive message on the first violation.
    """
    # ------------------------------------------------------------------
    # 1. Column count
    # ------------------------------------------------------------------
    if len(fieldnames) > MAX_COLUMN_COUNT:
        raise CSVValidationError(f"too many columns: {len(fieldnames)} > {MAX_COLUMN_COUNT}")

    seen_canonical: dict[str, str] = {}  # canonical → first original name

    for raw_name in fieldnames:
        # ------------------------------------------------------------------
        # 2. Blank / None / whitespace-only  (also caught upstream, kept for
        #    defence-in-depth)
        # ------------------------------------------------------------------
        if raw_name is None or not raw_name.strip():
            raise CSVValidationError("not a valid CSV (malformed header row)")

        name: str = raw_name  # raw_name is str past this point

        # ------------------------------------------------------------------
        # 3. Max header length
        # ------------------------------------------------------------------
        if len(name) > MAX_HEADER_LENGTH:
            raise CSVValidationError(
                f"column name too long ({len(name)} chars > {MAX_HEADER_LENGTH}): " f"{name[:32]!r}…"
            )

        # ------------------------------------------------------------------
        # 4. Control characters (incl. NUL \x00)
        # ------------------------------------------------------------------
        if _contains_control_char(name):
            raise CSVValidationError(f"column name contains a control character: {name!r}")

        # ------------------------------------------------------------------
        # 5. Reserved bookkeeping names (case-insensitive)
        # ------------------------------------------------------------------
        if name.strip().lower() in _RESERVED_HEADERS:
            raise CSVValidationError(f"column name is reserved and cannot be used in uploaded CSVs: {name!r}")

        # ------------------------------------------------------------------
        # 6. Duplicate detection: exact AND case-insensitive after strip
        # ------------------------------------------------------------------
        canonical = name.strip().lower()
        if canonical in seen_canonical:
            raise CSVValidationError(
                f"duplicate column name (case-insensitive): " f"{name!r} conflicts with {seen_canonical[canonical]!r}"
            )
        seen_canonical[canonical] = name


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_and_validate_csv(raw: bytes) -> ParsedCSV:
    """Parse and validate uploaded CSV bytes.

    Returns a :class:`ParsedCSV` dataclass containing headers and a list of :class:`ParsedRow`.
    Raises :class:`CSVValidationError` for any whole-file structural or policy violation.
    Never raises raw :exc:`csv.Error`, :exc:`UnicodeDecodeError`, or
    :exc:`KeyError` — these are always wrapped in CSVValidationError.

    Blank lines between data rows (completely empty lines) are skipped and not counted toward rows_total.
    A whitespace-only data line in a multi-column file is NOT treated as blank by csv.reader
    (it parses as a single-element record like ['   ']), so it is classified as a failed row
    with reason 'short_row'.

    row_index is the 0-based sequential index assigned strictly to non-blank records.
    source_line is the 1-based line number on which the record ENDS (per csv.reader.line_num).
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
    # Blank first line / empty header -> whole-file 400
    # ------------------------------------------------------------------
    first_line = text.split("\n", 1)[0].rstrip("\r")
    if not first_line.strip():
        raise CSVValidationError("not a valid CSV (no header row)")

    # ------------------------------------------------------------------
    # Syntax probe: catch bare CR in unquoted fields or csv module errors
    # ------------------------------------------------------------------
    try:
        list(csv.reader(io.StringIO(text)))
    except csv.Error as e:
        raise CSVValidationError(f"malformed CSV syntax: {e}") from e

    # ------------------------------------------------------------------
    # Parse with io.StringIO(text, newline=""), NOT splitlines.
    # ------------------------------------------------------------------
    try:
        reader = csv.reader(io.StringIO(text, newline=""))
        raw_headers = next(reader)
    except StopIteration:
        raise CSVValidationError("not a valid CSV (no header row)")
    except csv.Error as e:
        raise CSVValidationError(f"malformed CSV syntax: {e}") from e

    # ------------------------------------------------------------------
    # Structural header checks.
    # ------------------------------------------------------------------
    if not raw_headers or any(f is None or not f.strip() for f in raw_headers):
        raise CSVValidationError("not a valid CSV (malformed header row)")

    _validate_headers(raw_headers)
    headers = list(raw_headers)
    header_len = len(headers)

    # ------------------------------------------------------------------
    # Data row parsing & classification.
    # Blank data lines are skipped and not counted.
    # row_index is 0-based among non-blank records.
    # ------------------------------------------------------------------
    parsed_rows: list[ParsedRow] = []
    good_count = 0
    row_index = 0

    try:
        for record in reader:
            if not record:
                continue

            source_line = reader.line_num

            if len(record) > header_len:
                failed = True
                reason: str | None = FailureReason.EXTRA_FIELDS.value
                row_dict: dict[str, str] = {}
            elif len(record) < header_len:
                failed = True
                reason = FailureReason.SHORT_ROW.value
                row_dict = {}
            elif any(_has_value_control_char(f) for f in record):
                failed = True
                reason = FailureReason.CONTROL_CHAR.value
                row_dict = {}
            else:
                failed = False
                reason = None
                row_dict = {headers[i]: record[i] for i in range(header_len)}
                good_count += 1

            parsed_rows.append(
                ParsedRow(
                    index=row_index,
                    row=row_dict,
                    failed=failed,
                    reason=reason,
                    source_line=source_line,
                )
            )
            row_index += 1
    except csv.Error as e:
        raise CSVValidationError(f"malformed CSV syntax: {e}") from e

    if not parsed_rows:
        raise CSVValidationError("CSV has a header but zero data rows")

    if good_count == 0:
        raise CSVValidationError("no loadable rows")

    return ParsedCSV(headers=headers, rows=parsed_rows)
