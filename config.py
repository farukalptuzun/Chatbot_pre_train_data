"""
Configuration file for pretrain data processing pipeline
"""
from dataclasses import dataclass
from typing import List

@dataclass
class Config:
    """Main configuration class"""
    
    # Paths
    output_dir: str = "output"
    train_output_file: str = "train.jsonl"
    
    # Basic cleaning thresholds
    min_text_length: int = 400  # Increased for longer, more meaningful texts
    max_text_length: int = 30000  # Reduced to filter very long texts
    max_http_count: int = 1  # Reduced to filter spam links
    
    # Language filter settings
    lang_model_path: str = "lid.176.bin"  # fasttext language model
    allowed_languages: List[str] = None
    min_lang_confidence: float = 0.7
    
    # Deduplication settings
    exact_dedup_enabled: bool = True
    fuzzy_dedup_enabled: bool = False  # Disabled for speed (~10-50x faster), exact dedup still active
    fuzzy_similarity_threshold: float = 0.9
    minhash_num_perm: int = 128
    
    # PII filter patterns
    pii_patterns: List[str] = None
    
    # Quality filter settings
    min_unique_ratio: float = 0.45  # Increased for more diverse content
    min_sentence_count: int = 5  # Increased for more structured texts
    
    # Quality module settings (risk scoring)
    use_quality_module: bool = True  # Enable advanced quality filtering via risk scoring
    quality_risk_threshold: float = 0.25  # Reduced for more aggressive filtering
    reject_chinese_chars: bool = True  # Reject texts with Chinese characters
    
    # Data mix ratios (for final composition)
    tr_ratio: float = 0.35  # Turkish sources
    en_ratio: float = 0.35  # English sources
    tech_docs_ratio: float = 0.20  # Technical documents
    curated_ratio: float = 0.10  # High-quality curated
    
    def __post_init__(self):
        """Initialize default values"""
        if self.allowed_languages is None:
            self.allowed_languages = ["tr", "en"]
        
        if self.pii_patterns is None:
            # Default PII patterns
            self.pii_patterns = [
                r"\b\d{11}\b",  # TC Kimlik No (11 digits)
                r"\b\d{10}\b",  # Phone number (simple)
                r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z]{2,}",  # Email
                r"\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b",  # Credit card
                r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b",  # Credit card alternative
            ]

# Global config instance
config = Config()

# ============================================================================
# DATASET MIX CONFIGURATION (SINGLE SOURCE OF TRUTH)
# ============================================================================

# Dataset mix ratios (target percentages for final output)
DATASET_MIX = {
    "mc4_tr": {
        "target": 0.30,      # %30
        "min": 0.25,
        "max": 0.35,
    },
    "wiki_tr": {
        "target": 0.125,     # %12.5
        "min": 0.10,
        "max": 0.15,
    },
    "wiki_en": {
        "target": 0.225,     # %22.5
        "min": 0.20,
        "max": 0.25,
    },
    "tech_docs": {
        "target": 0.175,     # %17.5
        "min": 0.15,
        "max": 0.20,
    },
    "c4_en": {
        "target": 0.075,     # %7.5
        "min": 0.05,
        "max": 0.10,
    },
}

# Total target examples (adjustable)
TOTAL_TARGET_EXAMPLES = 2_500_000  # 2.5M examples (reduced for smaller, higher quality dataset)

# Overfetch factor (to compensate for dedup/filter losses)
# Fetch 2.0x target to account for aggressive filtering and deduplication losses
OVERFETCH_FACTOR = 2.0

# Source-specific language filter settings
# Trusted sources (e.g., Wikipedia) can skip language detection for speed without quality loss.
LANGUAGE_FILTER_BY_SOURCE = {
    "mc4_tr": True,
    "wiki_tr": False,  # Wikipedia TR is already Turkish, fasttext check unnecessary
    "wiki_en": False,  # Wikipedia EN is already English, fasttext check unnecessary
    "c4_en": True,
    "tech_docs": True,
}

