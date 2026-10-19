import csv
import io


class CSVValidationError(ValueError):
    """Raised when CSV content fails validation."""

    pass


def parse_and_validate_csv(raw: bytes) -> list[dict[str, str | None]]:
    """Pure validation and parsing function for uploaded CSV bytes.

    Raises CSVValidationError on any malformed or invalid CSV data.
    """
    if not raw:
        raise CSVValidationError("empty file")

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise CSVValidationError("file is not valid UTF-8 text/CSV")

    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    if reader.fieldnames is None:
        raise CSVValidationError("not a valid CSV (no header row)")
    if any(f is None or not f.strip() for f in reader.fieldnames):
        raise CSVValidationError("not a valid CSV (malformed header row)")
    if not rows:
        raise CSVValidationError("CSV has a header but zero data rows")

    return rows
