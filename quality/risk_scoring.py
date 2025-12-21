import re
from .rules import (
    PII_REGEX, 
    BOILERPLATE_KEYWORDS, 
    TOXIC_KEYWORDS, 
    E_COMMERCE_PATTERNS, 
    SEO_PATTERNS,
    ASTROLOGY_KEYWORDS,
    FORUM_SPAM_PATTERNS,
    SOCIAL_MEDIA_KEYWORDS,
)


def _has_chinese_characters(text: str) -> bool:
    """Check if text contains Chinese characters (CJK unified ideographs)"""
    # CJK Unified Ideographs range: U+4E00 to U+9FFF
    # Also includes CJK Extension A: U+3400 to U+4DBF
    for char in text:
        code_point = ord(char)
        if (0x4E00 <= code_point <= 0x9FFF) or (0x3400 <= code_point <= 0x4DBF):
            return True
    return False


def _chinese_character_ratio(text: str) -> float:
    """Calculate ratio of Chinese characters in text"""
    if not text:
        return 0.0
    
    chinese_count = 0
    total_chars = 0
    
    for char in text:
        if char.strip():  # Skip whitespace
            total_chars += 1
            code_point = ord(char)
            if (0x4E00 <= code_point <= 0x9FFF) or (0x3400 <= code_point <= 0x4DBF):
                chinese_count += 1
    
    if total_chars == 0:
        return 0.0
    
    return chinese_count / total_chars


def _detect_ecommerce_spam(text: str) -> float:
    """Detect e-commerce spam patterns"""
    t = text.lower()
    score = 0.0
    
    # Count e-commerce keywords
    ecommerce_matches = sum(1 for pattern in E_COMMERCE_PATTERNS if pattern.lower() in t)
    
    # High density of e-commerce keywords suggests spam
    if ecommerce_matches >= 5:
        score += 0.4
    elif ecommerce_matches >= 3:
        score += 0.2
    
    # Product listing patterns (size charts, specifications)
    if any(keyword in t for keyword in ["boyut grafiği", "size chart", "cm/inç", "cm/inch"]):
        score += 0.3
    
    # Price patterns (multiple prices suggest product listing)
    price_pattern = r'\d+[.,]\d+\s*(tl|₺|usd|\$|eur|€)'
    price_count = len(re.findall(price_pattern, t, re.IGNORECASE))
    if price_count >= 3:
        score += 0.3
    elif price_count >= 2:
        score += 0.15
    
    # Shipping/delivery info (common in e-commerce spam)
    if any(keyword in t for keyword in ["kargo", "shipping", "teslimat", "delivery"]):
        if "ücretsiz" in t or "free" in t:
            score += 0.2
    
    return min(score, 0.5)  # Cap e-commerce score at 0.5


def _detect_seo_spam(text: str) -> float:
    """Detect SEO spam patterns"""
    t = text.lower()
    score = 0.0
    
    # SEO pattern matches
    seo_matches = sum(1 for pattern in SEO_PATTERNS if pattern.lower() in t)
    if seo_matches >= 3:
        score += 0.3
    elif seo_matches >= 2:
        score += 0.15
    
    # Excessive links
    link_count = t.count("http") + t.count("www.")
    if link_count >= 5:
        score += 0.4
    elif link_count >= 3:
        score += 0.2
    
    # Keyword density (repetitive keywords)
    words = t.split()
    if len(words) > 0:
        word_freq = {}
        for word in words:
            if len(word) > 3:  # Ignore short words
                word_freq[word] = word_freq.get(word, 0) + 1
        
        # Check for excessive repetition of same word
        max_freq = max(word_freq.values()) if word_freq else 0
        if max_freq >= len(words) * 0.1:  # Same word appears >10% of time
            score += 0.3
    
    return min(score, 0.4)  # Cap SEO score at 0.4


def _detect_mixed_language(text: str) -> float:
    """Detect mixed language issues (non-TR/EN characters)"""
    score = 0.0
    
    # Chinese character ratio
    chinese_ratio = _chinese_character_ratio(text)
    if chinese_ratio > 0.1:  # More than 10% Chinese characters
        score += 0.5
    elif chinese_ratio > 0.05:  # More than 5% Chinese characters
        score += 0.3
    
    # Check for other non-Latin scripts (Arabic, Cyrillic, etc.)
    # But be careful - Turkish has some special characters
    non_latin_count = 0
    total_chars = 0
    
    for char in text:
        if char.strip():
            total_chars += 1
            code_point = ord(char)
            # Allow Latin, Turkish special chars, numbers, punctuation
            if not (
                (0x0000 <= code_point <= 0x007F) or  # Basic Latin
                (0x0080 <= code_point <= 0x00FF) or  # Latin-1 Supplement (includes Turkish)
                (0x0100 <= code_point <= 0x017F) or  # Latin Extended-A
                (0x0180 <= code_point <= 0x024F) or  # Latin Extended-B
                (0x1E00 <= code_point <= 0x1EFF) or  # Latin Extended Additional
                (0x0300 <= code_point <= 0x036F)     # Combining Diacritical Marks
            ):
                # Check if it's a known Turkish character (ş, ğ, ü, ö, ç, ı, İ)
                if char.lower() not in ['ş', 'ğ', 'ü', 'ö', 'ç', 'ı', 'i']:
                    non_latin_count += 1
    
    if total_chars > 0:
        non_latin_ratio = non_latin_count / total_chars
        if non_latin_ratio > 0.15:  # More than 15% non-Latin
            score += 0.3
        elif non_latin_ratio > 0.10:  # More than 10% non-Latin
            score += 0.2
    
    return min(score, 0.5)  # Cap mixed language score at 0.5


def _detect_astrology_content(text: str) -> float:
    """Detect astrology/horoscope content"""
    t = text.lower()
    score = 0.0
    
    astrology_matches = sum(1 for keyword in ASTROLOGY_KEYWORDS if keyword in t)
    if astrology_matches >= 2:
        score += 0.4
    elif astrology_matches >= 1:
        score += 0.2
    
    return min(score, 0.4)


def _detect_forum_spam(text: str) -> float:
    """Detect forum/comment spam"""
    t = text.lower()
    score = 0.0
    
    # Forum spam patterns
    forum_matches = sum(1 for pattern in FORUM_SPAM_PATTERNS if pattern in t)
    if forum_matches >= 3:
        score += 0.3
    elif forum_matches >= 2:
        score += 0.15
    
    # Very short comments (likely forum spam)
    words = t.split()
    if len(words) < 15 and any(pattern in t for pattern in ["teşekkürler", "thanks", "güzel", "nice"]):
        score += 0.3
    
    return min(score, 0.3)


def _detect_repetitive_characters(text: str) -> float:
    """Detect repetitive characters (aaaa, !!!!, ...)"""
    score = 0.0
    
    # Check for 4+ consecutive same characters
    repetitive_pattern = r'(.)\1{3,}'
    matches = len(re.findall(repetitive_pattern, text))
    if matches >= 3:
        score += 0.4
    elif matches >= 2:
        score += 0.2
    elif matches >= 1:
        score += 0.1
    
    return min(score, 0.4)


def _detect_social_media_spam(text: str) -> float:
    """Detect social media spam (hashtags, mentions)"""
    score = 0.0
    
    # Hashtag spam
    hashtag_count = text.count("#")
    if hashtag_count >= 5:
        score += 0.4
    elif hashtag_count >= 3:
        score += 0.2
    
    # Mention spam
    mention_count = text.count("@")
    if mention_count >= 5:
        score += 0.3
    elif mention_count >= 3:
        score += 0.15
    
    # Social media keywords
    t = text.lower()
    social_matches = sum(1 for keyword in SOCIAL_MEDIA_KEYWORDS if keyword in t)
    if social_matches >= 3:
        score += 0.2
    
    return min(score, 0.4)


def _detect_special_characters(text: str) -> float:
    """Detect excessive special characters/emoji"""
    score = 0.0
    
    # Count special characters (non-alphanumeric, non-space, non-punctuation)
    special_count = 0
    total_chars = 0
    
    for char in text:
        if char.strip():
            total_chars += 1
            # Check if it's a special character (emoji, symbols)
            code_point = ord(char)
            if not (
                (0x0000 <= code_point <= 0x007F) or  # ASCII
                (0x0080 <= code_point <= 0x00FF) or  # Latin-1
                (0x0100 <= code_point <= 0x017F) or  # Latin Extended-A
                char in ['ş', 'ğ', 'ü', 'ö', 'ç', 'ı', 'İ', 'Ş', 'Ğ', 'Ü', 'Ö', 'Ç']
            ):
                if char not in ['.', ',', '!', '?', ';', ':', '-', '_', '(', ')', '[', ']', '{', '}', '"', "'"]:
                    special_count += 1
    
    if total_chars > 0:
        special_ratio = special_count / total_chars
        if special_ratio > 0.15:  # More than 15% special chars
            score += 0.3
        elif special_ratio > 0.10:  # More than 10% special chars
            score += 0.2
    
    return min(score, 0.3)


def _detect_low_information_density(text: str) -> float:
    """Detect low information density (too much whitespace/punctuation)"""
    score = 0.0
    
    # Whitespace ratio
    whitespace_count = text.count(' ') + text.count('\n') + text.count('\t')
    total_chars = len(text)
    if total_chars > 0:
        whitespace_ratio = whitespace_count / total_chars
        if whitespace_ratio > 0.3:  # More than 30% whitespace
            score += 0.2
    
    # Punctuation ratio
    punctuation = '.,!?;:'
    punct_count = sum(text.count(p) for p in punctuation)
    if total_chars > 0:
        punct_ratio = punct_count / total_chars
        if punct_ratio > 0.2:  # More than 20% punctuation
            score += 0.2
    
    return min(score, 0.3)


def compute_risk_score(text: str) -> float:
    """
    Compute risk score for text (0.0 = safe, 1.0 = high risk)
    
    Args:
        text: Input text to score
        
    Returns:
        Risk score between 0.0 and 1.0
    """
    score = 0.0
    t = text.lower()

    # PII
    for r in PII_REGEX:
        if re.search(r, text):
            score += 0.5

    # Boilerplate
    if any(k in t for k in BOILERPLATE_KEYWORDS):
        score += 0.3

    # Toxic content (high priority)
    if any(k in t for k in TOXIC_KEYWORDS):
        score += 0.6

    # Low information
    if len(text) < 80:
        score += 0.2

    # SEO / link spam
    if t.count("http") >= 2:
        score += 0.3
    
    # Enhanced SEO spam detection
    seo_score = _detect_seo_spam(text)
    score += seo_score

    # Aşırı tekrar
    words = t.split()
    if words:
        uniq_ratio = len(set(words)) / len(words)
        if uniq_ratio < 0.4:
            score += 0.3

    # Chinese character detection
    if _has_chinese_characters(text):
        chinese_ratio = _chinese_character_ratio(text)
        if chinese_ratio > 0.1:
            score += 0.5  # High Chinese content
        elif chinese_ratio > 0.05:
            score += 0.3  # Moderate Chinese content
    
    # Mixed language detection
    mixed_lang_score = _detect_mixed_language(text)
    score += mixed_lang_score
    
    # E-commerce spam detection
    ecommerce_score = _detect_ecommerce_spam(text)
    score += ecommerce_score
    
    # Astrology content detection
    astrology_score = _detect_astrology_content(text)
    score += astrology_score
    
    # Forum spam detection
    forum_score = _detect_forum_spam(text)
    score += forum_score
    
    # Repetitive characters
    repetitive_score = _detect_repetitive_characters(text)
    score += repetitive_score
    
    # Social media spam
    social_score = _detect_social_media_spam(text)
    score += social_score
    
    # Special characters
    special_score = _detect_special_characters(text)
    score += special_score
    
    # Low information density
    info_score = _detect_low_information_density(text)
    score += info_score

    return min(score, 1.0)

