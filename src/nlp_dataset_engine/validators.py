from typing import Dict, Any

class DataValidator:
    """
    Validates dictionary rows from the stream.
    """
    def __init__(self, min_length: int = 10):
        self.min_length = min_length

    def validate(self, item: Dict[str, Any]) -> bool:
        """
        Returns True if item is valid.
        Checks:
        1. 'text' field exists
        2. 'text' is not empty
        3. 'text' length >= min_length
        """
        text = item.get("text", "")
        
        if not isinstance(text, str):
            return False
            
        if not text.strip():
            return False
            
        if len(text) < self.min_length:
            return False
            
        return True
    