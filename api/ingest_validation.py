import csv
import io
import unicodedata

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_FIELD_SIZE_BYTES: int = 128 * 1024  # 128 KB limit per cell
csv.field_size_limit(MAX_FIELD_SIZE_BYTES)

MAX_HEADER_LENGTH: int = 128       # characters per column name
MAX_COLUMN_COUNT: int = 200        # columns per CSV

# Case-insensitive reserved bookkeeping property names injected by the loader.
# A CSV column that matches one of these would overwrite internal node metadata
# via  ON CREATE SET r += $row  and break dataset isolation or idempotency.
_RESERVED_HEADERS: frozenset[str] = frozenset({"dataset_id", "row_index", "_new"})


class CSVValidationError(ValueError):
    """Raised when CSV content fails validation."""

    pass


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _contains_control_char(s: str) -> bool:
    """Return True if *s* contains any ASCII control character or NUL."""
    return any(unicodedata.category(ch) == "Cc" for ch in s)


def _validate_headers(fieldnames: list[str | None]) -> None:
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
                f"column name too long ({len(name)} chars > {MAX_HEADER_LENGTH}): "
                f"{name[:32]!r}…"
            )

        # ------------------------------------------------------------------
        # 4. Control characters (incl. NUL \x00)
        # ------------------------------------------------------------------
        if _contains_control_char(name):
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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_and_validate_csv(raw: bytes) -> list[dict[str, str | None]]:
    """Parse and validate uploaded CSV bytes.

    Returns a list of row dicts on success.
    Raises :class:`CSVValidationError` for any structural or policy violation.
    Never raises raw :exc:`csv.Error`, :exc:`UnicodeDecodeError`, or
    :exc:`KeyError` — these are always wrapped.
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
    # Parse: wrap any csv module exception.
    # ------------------------------------------------------------------
    try:
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
    except csv.Error as e:
        raise CSVValidationError(f"malformed CSV syntax: {e}") from e

    # ------------------------------------------------------------------
    # Structural checks.
    # ------------------------------------------------------------------
    if reader.fieldnames is None:
        raise CSVValidationError("not a valid CSV (no header row)")

    if any(f is None or not f.strip() for f in reader.fieldnames):
        raise CSVValidationError("not a valid CSV (malformed header row)")

    if not rows:
        raise CSVValidationError("CSV has a header but zero data rows")

    # ------------------------------------------------------------------
    # Header policy (duplicates, reserved names, control chars, limits).
    # ------------------------------------------------------------------
    _validate_headers(list(reader.fieldnames))

    # ------------------------------------------------------------------
    # Ragged-row check: DictReader maps extra fields to None key.
    # ------------------------------------------------------------------
    for i, row in enumerate(rows):
        if None in row:
            raise CSVValidationError(
                f"ragged row at line {i + 2}: more fields than header columns"
            )

    return rows

