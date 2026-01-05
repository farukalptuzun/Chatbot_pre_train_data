"""
Basic cleaning module
Removes HTML, normalizes whitespace, and filters very short/long texts
"""
import re
from typing import Optional
from config import config



def detect_code_content(text: str) -> bool:
    """
    Detect if text contains HTML/JavaScript code or programming-like content
    
    Args:
        text: Input text
        
    Returns:
        True if code-like content detected, False otherwise
    """
    text_lower = text.lower()
    
    # HTML/JavaScript tag patterns
    script_patterns = [
        r"<script[^>]*>",
        r"</script>",
        r"<style[^>]*>",
        r"</style>",
        r"<iframe[^>]*>",
        r"javascript:",
        r"onclick\s*=",
        r"onerror\s*=",
    ]
    
    for pattern in script_patterns:
        if re.search(pattern, text_lower):
            return True
    
    # JavaScript code patterns
    js_patterns = [
        r"\bfunction\s*\(",
        r"\bvar\s+\w+\s*=",
        r"\bconst\s+\w+\s*=",
        r"\blet\s+\w+\s*=",
        r"\bclass\s+\w+",
        r"\.getElementById\(",
        r"\.addEventListener\(",
        r"console\.log\(",
        r"document\.",
        r"window\.",
    ]
    
    js_match_count = sum(1 for pattern in js_patterns if re.search(pattern, text))
    if js_match_count >= 2:  # At least 2 JS patterns
        return True
    
    # Base64 encoded content (long base64 strings)
    base64_pattern = r"[A-Za-z0-9+/]{50,}={0,2}"  # Base64 strings of 50+ chars
    base64_matches = len(re.findall(base64_pattern, text))
    if base64_matches >= 2:
        return True
    
    # Excessive special characters (likely code)
    # Count brackets, braces, parentheses
    bracket_count = text.count('{') + text.count('}')
    paren_count = text.count('(') + text.count(')')
    square_count = text.count('[') + text.count(']')
    
    total_special = bracket_count + paren_count + square_count
    text_length = len(text)
    
    # If special chars are more than 5% of text, likely code
    if text_length > 0 and (total_special / text_length) > 0.05:
        return True
    
    # Excessive semicolons (common in code)
    semicolon_count = text.count(';')
    if text_length > 0 and (semicolon_count / text_length) > 0.03:
        return True
    
    return False


def basic_clean(text: str) -> str:
    """
    Basic text cleaning: remove HTML, normalize whitespace
    
    Args:
        text: Input text
        
    Returns:
        Cleaned text
    """
    # Remove HTML tags
    text = re.sub(r"<[^>]+>", " ", text)
    
    # Remove excessive whitespace
    text = re.sub(r"\s+", " ", text)
    
    # Strip leading/trailing whitespace
    text = text.strip()
    
    return text


def basic_filter(text: str) -> bool:
    """
    Filter texts based on length and content quality
    
    Args:
        text: Input text
        
    Returns:
        True if text passes filters, False otherwise
    """
    # Check minimum length
    if len(text) < config.min_text_length:
        return False
    
    # Check maximum length
    if len(text) > config.max_text_length:
        return False
    
    # Filter out texts with too many URLs (likely spam)
    if text.count("http") > config.max_http_count:
        return False
    
    # Filter out code-like content (HTML/JavaScript)
    if detect_code_content(text):
        return False
    
    return True


def clean_and_filter(text: str) -> Optional[str]:
    """
    Combine cleaning and filtering in one step
    
    Args:
        text: Input text
        
    Returns:
        Cleaned text if it passes filters, None otherwise
    """
    cleaned = basic_clean(text)
    if basic_filter(cleaned):
        return cleaned
    return None

