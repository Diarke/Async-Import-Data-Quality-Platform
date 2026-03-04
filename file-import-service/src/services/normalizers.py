import re
from datetime import datetime
from typing import Optional


def normalize_phone(
    value: str,
    default_country_code: str = "7",  # Russia default, can be overridden to "KZ"
) -> Optional[str]:
    """
    Normalize phone number to E.164 format.
    
    Args:
        value: Phone number string
        default_country_code: Country code to prepend if not present (e.g., "7" for Russia, "7" for KZ)
    
    Returns:
        Normalized phone in E.164 format (e.g., "+77123456789") or None if invalid
    """
    if not value or not isinstance(value, str):
        return None
    
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', value)
    
    if not digits:
        return None
    
    # If starts with country code, keep it; otherwise prepend default
    if digits.startswith('7') or digits.startswith('80'):
        # Handle Kazakhstan +7 vs 8 prefix
        if digits.startswith('8'):
            digits = '7' + digits[1:]
    elif not digits.startswith(default_country_code):
        digits = default_country_code + digits
    
    # Validate length (7 should be ~10-11 digits, KZ should be 11)
    if len(digits) < 10 or len(digits) > 15:
        return None
    
    # Format as +{country_code}{rest}
    return f"+{digits}"


def normalize_date(
    value: str,
    formats: Optional[list] = None
) -> Optional[str]:
    """
    Normalize date to ISO 8601 format.
    
    Args:
        value: Date string in various formats
        formats: List of datetime format strings to try
    
    Returns:
        Date in YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ format, or None if invalid
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
            # Return date only if no time component specified
            if '%H' not in fmt and '%M' not in fmt:
                return parsed.strftime("%Y-%m-%d")
            else:
                # Return ISO 8601 with Z suffix
                return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue
    
    return None


def normalize_inn(value: str) -> Optional[str]:
    """
    Normalize INN (Russian/Kazakhstan tax identifier).
    
    Args:
        value: INN string
    
    Returns:
        Cleaned INN (digits only) or None if invalid
    """
    if not value or not isinstance(value, str):
        return None
    
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', value)
    
    if not digits:
        return None
    
    # Validate length (typically 10 or 12 for Russia, 12 for Kazakhstan)
    if len(digits) not in (10, 12):
        return None
    
    return digits


def normalize_field(field_name: str, value: str, default_country_code: str = "7") -> Optional[str]:
    """
    Normalize a field based on its name.
    
    Args:
        field_name: Name of the field (phone, date, inn, etc.)
        value: Value to normalize
        default_country_code: Country code for phone numbers
    
    Returns:
        Normalized value or None if invalid
    """
    field_lower = field_name.lower().strip()
    
    if any(p in field_lower for p in ['phone', 'tel', 'мобильный', 'telephone']):
        return normalize_phone(value, default_country_code)
    elif any(d in field_lower for d in ['date', 'дата', 'birth']):
        return normalize_date(value)
    elif any(i in field_lower for i in ['inn', 'иnn']):
        return normalize_inn(value)
    
    return value
