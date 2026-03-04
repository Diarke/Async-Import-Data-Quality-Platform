import pytest
from src.normalizers import (
    normalize_phone,
    normalize_date,
    normalize_inn,
    normalize_row,
    parse_csv_chunk,
    parse_jsonl_chunk,
    process_chunk,
)


class TestNormalizePhone:
    """Tests for phone number normalization"""
    
    def test_valid_phone_russian(self):
        """Test valid Russian phone number"""
        result = normalize_phone("79991234567")
        assert result == "+79991234567"
    
    def test_valid_phone_kazakhstan(self):
        """Test valid Kazakhstan phone number"""
        result = normalize_phone("87764567890", default_country_code="7")
        assert result == "+77764567890"
    
    def test_phone_with_spaces(self):
        """Test phone with spaces and special characters"""
        result = normalize_phone("+7 (999) 123-45-67")
        assert result == "+79991234567"
    
    def test_phone_with_prefix_8(self):
        """Test phone with 8 prefix (Kazakhstan)"""
        result = normalize_phone("89991234567")
        assert result == "+79991234567"
    
    def test_phone_too_short(self):
        """Test phone that's too short"""
        result = normalize_phone("123")
        assert result is None
    
    def test_phone_empty(self):
        """Test empty phone"""
        result = normalize_phone("")
        assert result is None
    
    def test_phone_none(self):
        """Test None phone"""
        result = normalize_phone(None)
        assert result is None
    
    def test_phone_only_letters(self):
        """Test phone with only letters"""
        result = normalize_phone("abcdefgh")
        assert result is None


class TestNormalizeDate:
    """Tests for date normalization"""
    
    def test_date_iso_format(self):
        """Test ISO format date"""
        result = normalize_date("2022-12-25")
        assert result == "2022-12-25"
    
    def test_date_russian_format(self):
        """Test Russian format date (DD.MM.YYYY)"""
        result = normalize_date("25.12.2022")
        assert result == "2022-12-25"
    
    def test_date_with_time(self):
        """Test date with time"""
        result = normalize_date("2022-12-25 14:30:45")
        assert result == "2022-12-25T14:30:45Z"
    
    def test_date_invalid(self):
        """Test invalid date"""
        result = normalize_date("32.13.2022")
        assert result is None
    
    def test_date_empty(self):
        """Test empty date"""
        result = normalize_date("")
        assert result is None
    
    def test_date_slash_format(self):
        """Test slash format date"""
        result = normalize_date("12/25/2022")
        assert result == "2022-12-25"


class TestNormalizeINN:
    """Tests for INN normalization"""
    
    def test_inn_valid_10_digits(self):
        """Test valid 10-digit INN"""
        result = normalize_inn("5038001293")
        assert result == "5038001293"
    
    def test_inn_valid_12_digits(self):
        """Test valid 12-digit INN"""
        result = normalize_inn("503800129300")
        assert result == "503800129300"
    
    def test_inn_with_special_chars(self):
        """Test INN with special characters"""
        result = normalize_inn("50-380-012-93")
        assert result == "5038001293"
    
    def test_inn_invalid_length(self):
        """Test INN with invalid length"""
        result = normalize_inn("123456789")  # 9 digits
        assert result is None
    
    def test_inn_empty(self):
        """Test empty INN"""
        result = normalize_inn("")
        assert result is None
    
    def test_inn_none(self):
        """Test None INN"""
        result = normalize_inn(None)
        assert result is None


class TestNormalizeRow:
    """Tests for row normalization"""
    
    def test_normalize_row_all_fields(self):
        """Test normalizing row with all types of fields"""
        row = {
            "phone": "79991234567",
            "birth_date": "25.12.1990",
            "inn": "5038001293",
            "name": "John Doe"
        }
        
        result = normalize_row(row)
        
        assert result["phone"] == "+79991234567"
        assert result["birth_date"] == "1990-12-25"
        assert result["inn"] == "5038001293"
        assert result["name"] == "John Doe"
    
    def test_normalize_row_with_invalid_values(self):
        """Test row with some invalid values"""
        row = {
            "phone": "invalid",
            "date": "32.13.2022",
            "name": "Test"
        }
        
        result = normalize_row(row)
        
        assert result["phone"] is None
        assert result["date"] is None
        assert result["name"] == "Test"


class TestParseChunks:
    """Tests for chunk parsing"""
    
    def test_parse_csv_chunk(self):
        """Test parsing CSV chunk"""
        csv_data = b"name,phone,date\nJohn,79991234567,2022-12-25\n"
        
        rows = parse_csv_chunk(csv_data)
        
        assert len(rows) == 1
        assert rows[0]["name"] == "John"
        assert rows[0]["phone"] == "79991234567"
    
    def test_parse_jsonl_chunk(self):
        """Test parsing JSONL chunk"""
        jsonl_data = b'{"name":"John","phone":"79991234567"}\n{"name":"Jane","phone":"79876543210"}\n'
        
        rows = parse_jsonl_chunk(jsonl_data)
        
        assert len(rows) == 2
        assert rows[0]["name"] == "John"
        assert rows[1]["phone"] == "79876543210"


class TestProcessChunk:
    """Tests for full chunk processing"""
    
    def test_process_chunk_csv(self):
        """Test processing CSV chunk"""
        csv_data = b"name,phone\nJohn,79991234567\nJane,87764567890\n"
        
        processed, metadata = process_chunk(csv_data, format='csv')
        
        assert metadata["rows_processed"] == 2
        assert metadata["success"] is True
        assert b"+7999" in processed  # Phone should be normalized
    
    def test_process_chunk_with_errors(self):
        """Test processing chunk with some errors"""
        csv_data = b"name,phone\nJohn,invalid_phone\nJane,79876543210\n"
        
        processed, metadata = process_chunk(csv_data, format='csv')
        
        assert metadata["rows_processed"] == 2
        # Processing should continue even with errors


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
