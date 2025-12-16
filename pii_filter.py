"""
PII (Personally Identifiable Information) filtering module
Removes texts containing sensitive personal information
"""
import re
from typing import List, Optional
from config import config


def pii_filter(text: str, custom_patterns: Optional[List[str]] = None) -> bool:
    """
    Check if text contains PII patterns
    
    Args:
        text: Input text
        custom_patterns: Optional custom PII patterns (default: from config)
        
    Returns:
        True if text does NOT contain PII, False if PII found
    """
    patterns = custom_patterns or config.pii_patterns
    
    for pattern in patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return False  # PII found, reject text
    
    return True  # No PII found, accept text


def remove_pii_from_text(text: str, replacement: str = "[REDACTED]") -> str:
    """
    Remove PII from text by replacing with placeholder
    
    Args:
        text: Input text
        replacement: Replacement string for PII
        
    Returns:
        Text with PII replaced
    """
    patterns = config.pii_patterns
    cleaned_text = text
    
    for pattern in patterns:
        cleaned_text = re.sub(pattern, replacement, cleaned_text, flags=re.IGNORECASE)
    
    return cleaned_text


# Canary string for testing
CANARY_STRING = "ZXQJ_CANARY_492837"


def add_canary_to_text(text: str) -> str:
    """
    Add canary string to text for post-training testing
    This helps detect if model memorized PII
    
    Args:
        text: Input text
        
    Returns:
        Text with canary string appended
    """
    return text + f"\n{CANARY_STRING}"

