"""
Quality filtering module
Filters out low-quality texts (random word sequences, meaningless repetitions, etc.)
"""
from config import config


def quality_filter(text: str) -> bool:
    """
    Check if text meets quality standards
    
    Args:
        text: Input text
        
    Returns:
        True if text passes quality filters, False otherwise
    """
    words = text.split()
    
    # Check if text has enough words
    if len(words) < 10:
        return False
    
    # Calculate unique word ratio (diversity measure)
    unique_words = len(set(words))
    unique_ratio = unique_words / len(words) if len(words) > 0 else 0
    
    if unique_ratio < config.min_unique_ratio:
        return False  # Too repetitive
    
    # Check for minimum sentence count (basic quality indicator)
    sentence_count = text.count(".") + text.count("!") + text.count("?")
    if sentence_count < config.min_sentence_count:
        return False
    
    return True

