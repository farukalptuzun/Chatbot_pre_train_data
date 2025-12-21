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
    # English adult content
    "porn",
    "porno",
    "sex",
    "xxx",
    "18+",
    "adult",
    "fuck",
    "shit",
    # Turkish adult content
    "seks",
    "yetiskin",
    "amk",
    "orospu",
    "sik",
    "yarrak",
    "amcık",
    # Additional patterns
    "pornhub",
    "xvideos",
    "xnxx",
]

# E-commerce spam patterns
E_COMMERCE_PATTERNS = [
    # Product listing keywords
    "ürün boyutu",
    "product size",
    "boyut grafiği",
    "size chart",
    "sipariş",
    "order",
    "teslimat",
    "delivery",
    "kargo",
    "shipping",
    "fiyat",
    "price",
    "indirim",
    "discount",
    "kampanya",
    "campaign",
    "satın al",
    "buy now",
    "sepete ekle",
    "add to cart",
    # E-commerce spam indicators
    "ücretsiz kargo",
    "free shipping",
    "hızlı teslimat",
    "fast delivery",
    "stokta",
    "in stock",
    "stokta yok",
    "out of stock",
    # Product specifications
    "cm/inç",
    "cm/inch",
    "malzeme",
    "material",
    "renk",
    "color",
    "model",
    "marka",
    "brand",
]

# SEO spam indicators
SEO_PATTERNS = [
    # Excessive keyword patterns
    "keyword",
    "anahtar kelime",
    "meta",
    "description",
    "açıklama",
    # Link farm indicators
    "backlink",
    "link exchange",
    "link değişimi",
    # Spam indicators
    "click here",
    "buraya tıkla",
    "daha fazla bilgi",
    "more info",
    "read more",
    "devamını oku",
]

# Astroloji/Burç içeriği
ASTROLOGY_KEYWORDS = [
    "burç",
    "horoscope",
    "yengeç burcu",
    "cancer",
    "aslan burcu",
    "leo",
    "başak burcu",
    "virgo",
    "terazi burcu",
    "libra",
    "akrep burcu",
    "scorpio",
    "yay burcu",
    "sagittarius",
    "oğlak burcu",
    "capricorn",
    "kova burcu",
    "aquarius",
    "balık burcu",
    "pisces",
    "koç burcu",
    "aries",
    "boğa burcu",
    "taurus",
    "ikizler burcu",
    "gemini",
    "fal",
    "tarot",
    "astroloji",
    "astrology",
]

# Forum/Comment spam patterns
FORUM_SPAM_PATTERNS = [
    "teşekkürler",
    "thanks",
    "güzel",
    "nice",
    "harika",
    "great",
    "çok güzel",
    "very nice",
    "beğendim",
    "liked",
    "tavsiye ederim",
    "recommend",
    "çok iyi",
    "very good",
    "mükemmel",
    "perfect",
    # Forum comment indicators
    "yorum",
    "comment",
    "forum",
    "mesaj",
    "message",
    "paylaşım",
    "share",
]

# Sosyal medya spam keywords
SOCIAL_MEDIA_KEYWORDS = [
    "follow",
    "takip",
    "like",
    "beğen",
    "share",
    "paylaş",
    "retweet",
    "rt",
]

