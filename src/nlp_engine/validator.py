from typing import Dict

class DataValidator:
    """
    Validates data records to ensure quality for LLM training.
    """
    
    def __init__(self, min_length: int = 10):
        self.min_length = min_length
        
    def validate(self, record: Dict[str, str]) -> bool:
        """
        Returns True if the record is valid, False otherwise.
        """
        text = record.get("text", "").strip()
        
        # Rule 1: Must not be empty
        if not text:
            return False
            
        # Rule 2: Must meet minimum length (removes noise)
        if len(text) < self.min_length:
            return False
            
        return True