import re
from .rules import PII_REGEX, BOILERPLATE_KEYWORDS, TOXIC_KEYWORDS


def compute_risk_score(text: str) -> float:
    score = 0.0
    t = text.lower()

    # PII
    for r in PII_REGEX:
        if re.search(r, text):
            score += 0.5

    # Boilerplate
    if any(k in t for k in BOILERPLATE_KEYWORDS):
        score += 0.3

    # Toxic
    if any(k in t for k in TOXIC_KEYWORDS):
        score += 0.6

    # Low information
    if len(text) < 80:
        score += 0.2

    # SEO / link spam
    if t.count("http") >= 2:
        score += 0.3

    # Aşırı tekrar
    words = t.split()
    if words:
        uniq_ratio = len(set(words)) / len(words)
        if uniq_ratio < 0.4:
            score += 0.3

    return min(score, 1.0)

