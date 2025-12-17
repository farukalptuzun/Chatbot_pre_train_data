PII_REGEX = [
    r"\b\d{10,11}\b",  # telefon
    r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",  # email
    r"\bTR\d{24}\b",  # IBAN
]

BOILERPLATE_KEYWORDS = [
    "çerez",
    "cookie",
    "privacy",
    "gizlilik",
    "terms",
    "koşullar",
    "hakları saklıdır",
    "all rights reserved",
]

TOXIC_KEYWORDS = [
    "porn",
    "sex",
    "fuck",
    "shit",
    "amk",
    "orospu",
]

