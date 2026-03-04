import re
import csv
import io
import json
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass


@dataclass
class NormalizationResult:
    """Результат нормализации"""
    success: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


def normalize_phone(
    value: str,
    default_country_code: str = "7",
) -> Optional[str]:
    """
    Normalize phone number to E.164 format.
    """
    if not value or not isinstance(value, str):
        return None
    
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', value)
    
    if not digits:
        return None
    
    # Handle Kazakhstan prefix
    if digits.startswith('8'):
        digits = '7' + digits[1:]
    elif not digits.startswith(default_country_code) and not digits.startswith('7'):
        digits = default_country_code + digits
    
    # Validate length
    if len(digits) < 10 or len(digits) > 15:
        return None
    
    return f"+{digits}"


def normalize_date(
    value: str,
    formats: Optional[list] = None
) -> Optional[str]:
    """
    Normalize date to ISO 8601 format.
    """
    if not value or not isinstance(value, str):
        return None
    
    value = value.strip()
    
    if not formats:
        formats = [
            "%Y-%m-%d",
            "%d.%m.%Y",
            "%d/%m/%Y",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%Y-%m-%d %H:%M:%S",
            "%d.%m.%Y %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M",
            "%d.%m.%Y %H:%M",
        ]
    
    for fmt in formats:
        try:
            parsed = datetime.strptime(value, fmt)
            if '%H' not in fmt and '%M' not in fmt:
                return parsed.strftime("%Y-%m-%d")
            else:
                return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue
    
    return None


def normalize_inn(value: str) -> Optional[str]:
    """
    Normalize INN (tax identifier).
    """
    if not value or not isinstance(value, str):
        return None
    
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', value)
    
    if not digits:
        return None
    
    # Validate length
    if len(digits) not in (10, 12):
        return None
    
    return digits


def normalize_row(
    row: Dict[str, str],
    field_mappings: Optional[Dict[str, str]] = None,
    default_country_code: str = "7"
) -> Dict[str, Any]:
    """
    Normalize a row of data applying rules based on field name.
    
    Args:
        row: Dictionary of field names to values
        field_mappings: Optional mapping of field names to normalization types
        default_country_code: Country code for phone normalization
    
    Returns:
        Row with normalized values
    """
    normalized = {}
    
    for field_name, value in row.items():
        field_lower = field_name.lower().strip()
        
        # Determine normalization type
        if any(p in field_lower for p in ['phone', 'tel', 'мобильный', 'telephone']):
            normalized_value = normalize_phone(value, default_country_code)
        elif any(d in field_lower for d in ['date', 'дата', 'birth']):
            normalized_value = normalize_date(value)
        elif any(i in field_lower for i in ['inn', 'иnn']):
            normalized_value = normalize_inn(value)
        else:
            # Return unchanged if no normalization rule applies
            normalized_value = value.strip() if isinstance(value, str) else value
        
        normalized[field_name] = normalized_value
    
    return normalized


def parse_csv_chunk(chunk_bytes: bytes, encoding: str = 'utf-8') -> List[Dict[str, str]]:
    """
    Parse CSV chunk into list of dicts.
    
    Args:
        chunk_bytes: Raw chunk bytes
        encoding: Text encoding
    
    Returns:
        List of dictionaries (rows)
    """
    try:
        text = chunk_bytes.decode(encoding)
        reader = csv.DictReader(io.StringIO(text))
        if reader.fieldnames is None:
            return []
        return list(reader)
    except Exception as e:
        raise ValueError(f"Failed to parse CSV: {e}")


def parse_tsv_chunk(chunk_bytes: bytes, encoding: str = 'utf-8') -> List[Dict[str, str]]:
    """
    Parse TSV chunk into list of dicts.
    """
    try:
        text = chunk_bytes.decode(encoding)
        reader = csv.DictReader(io.StringIO(text), delimiter='\t')
        if reader.fieldnames is None:
            return []
        return list(reader)
    except Exception as e:
        raise ValueError(f"Failed to parse TSV: {e}")


def parse_jsonl_chunk(chunk_bytes: bytes, encoding: str = 'utf-8') -> List[Dict[str, Any]]:
    """
    Parse JSONL chunk (one JSON object per line) into list of dicts.
    """
    try:
        text = chunk_bytes.decode(encoding)
        rows = []
        for line in text.strip().split('\n'):
            if line.strip():
                rows.append(json.loads(line))
        return rows
    except Exception as e:
        raise ValueError(f"Failed to parse JSONL: {e}")


def detect_format(filename: str) -> str:
    """
    Detect file format from filename.
    """
    filename_lower = filename.lower()
    if filename_lower.endswith('.csv'):
        return 'csv'
    elif filename_lower.endswith('.tsv'):
        return 'tsv'
    elif filename_lower.endswith('.jsonl') or filename_lower.endswith('.ndjson'):
        return 'jsonl'
    elif filename_lower.endswith('.json'):
        return 'json'
    else:
        return 'csv'  # Default


def parse_chunk(
    chunk_bytes: bytes,
    format: str = 'csv',
    encoding: str = 'utf-8'
) -> List[Dict[str, Any]]:
    """
    Parse chunk based on format.
    """
    if format == 'csv':
        return parse_csv_chunk(chunk_bytes, encoding)
    elif format == 'tsv':
        return parse_tsv_chunk(chunk_bytes, encoding)
    elif format == 'jsonl':
        return parse_jsonl_chunk(chunk_bytes, encoding)
    else:
        return parse_csv_chunk(chunk_bytes, encoding)


def serialize_rows_to_csv(rows: List[Dict[str, Any]]) -> bytes:
    """
    Serialize list of dicts back to CSV bytes.
    """
    if not rows:
        return b""
    
    output = io.StringIO()
    fieldnames = list(rows[0].keys())
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    
    return output.getvalue().encode('utf-8')


def process_chunk(
    chunk_bytes: bytes,
    format: str = 'csv',
    default_country_code: str = "7",
    encoding: str = 'utf-8'
) -> Tuple[bytes, Dict[str, Any]]:
    """
    Process chunk: parse, normalize, serialize back.
    
    Returns:
        Tuple of (processed_chunk_bytes, metadata)
    """
    # Parse chunk
    rows = parse_chunk(chunk_bytes, format, encoding)
    
    # Normalize rows
    normalized_rows = []
    errors = []
    
    for idx, row in enumerate(rows):
        try:
            normalized_row = normalize_row(row, default_country_code=default_country_code)
            normalized_rows.append(normalized_row)
        except Exception as e:
            errors.append({"row": idx, "error": str(e)})
    
    # Serialize back to CSV
    processed_bytes = serialize_rows_to_csv(normalized_rows)
    
    metadata = {
        "rows_processed": len(normalized_rows),
        "errors": errors,
        "success": len(errors) == 0
    }
    
    return processed_bytes, metadata
