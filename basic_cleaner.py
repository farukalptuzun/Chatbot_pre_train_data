"""
Basic cleaning module
Removes HTML, normalizes whitespace, and filters very short/long texts
"""
import re
from typing import Optional
from config import config


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

