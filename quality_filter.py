"""
Quality filtering module
Filters out low-quality texts (random word sequences, meaningless repetitions, etc.)
"""
import re
from config import config


def check_paragraph_structure(text: str) -> bool:
    """
    Check if text has proper paragraph structure
    
    Args:
        text: Input text
        
    Returns:
        True if text has good paragraph structure, False otherwise
    """
    # Split by double newlines (paragraph breaks)
    paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
    
    # Also check single newlines as potential paragraph breaks
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    
    # If text has multiple paragraphs (separated by double newlines)
    if len(paragraphs) >= 2:
        return True
    
    # If text has multiple substantial lines (at least 3 lines with content)
    if len(lines) >= 3:
        # Check if lines are substantial (not just short fragments)
        substantial_lines = [line for line in lines if len(line) > 50]
        if len(substantial_lines) >= 2:
            return True
    
    # For single paragraph texts, check if it's not too long
    # Very long single paragraphs are often low quality
    if len(paragraphs) == 1 or len(lines) == 1:
        if len(text) > 2000:  # Very long single paragraph is suspicious
            return False
    
    # If we have at least one paragraph, accept it
    if len(paragraphs) >= 1 or len(lines) >= 1:
        return True
    
    return False


def check_sentence_quality(text: str) -> bool:
    """
    Check sentence quality metrics
    
    Args:
        text: Input text
        
    Returns:
        True if sentences are of good quality, False otherwise
    """
    # Split text into sentences
    sentence_endings = r'[.!?]+'
    sentences = [s.strip() for s in re.split(sentence_endings, text) if s.strip()]
    
    if len(sentences) < config.min_sentence_count:
        return False
    
    # Calculate average sentence length
    total_length = sum(len(s) for s in sentences)
    avg_length = total_length / len(sentences) if len(sentences) > 0 else 0
    
    # Sentences should be reasonably long (not too short, not too long)
    # Too short sentences (< 20 chars) suggest fragments
    # Too long sentences (> 500 chars) suggest run-on sentences
    short_sentences = sum(1 for s in sentences if len(s) < 20)
    long_sentences = sum(1 for s in sentences if len(s) > 500)
    
    # If more than 30% are very short, reject
    if len(sentences) > 0 and (short_sentences / len(sentences)) > 0.3:
        return False
    
    # If many sentences are too long, reject
    if len(sentences) > 0 and (long_sentences / len(sentences)) > 0.2:
        return False
    
    # Check if sentences start with capital letters (basic grammar check)
    # Allow some exceptions (quotes, etc.)
    proper_start_count = 0
    for sentence in sentences[:10]:  # Check first 10 sentences
        if sentence:
            first_char = sentence[0]
            # Check if starts with capital or number or special char
            if first_char.isupper() or first_char.isdigit() or first_char in '"\'':
                proper_start_count += 1
    
    # At least 60% of sentences should start properly
    if len(sentences) > 0:
        checked = min(10, len(sentences))
        if checked > 0 and (proper_start_count / checked) < 0.6:
            return False
    
    return True


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
    
    # Check paragraph structure
    if not check_paragraph_structure(text):
        return False
    
    # Check sentence quality
    if not check_sentence_quality(text):
        return False
    
    return True

