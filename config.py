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
    min_text_length: int = 200
    max_text_length: int = 50000
    max_http_count: int = 3
    
    # Language filter settings
    lang_model_path: str = "lid.176.bin"  # fasttext language model
    allowed_languages: List[str] = None
    min_lang_confidence: float = 0.7
    
    # Deduplication settings
    exact_dedup_enabled: bool = True
    fuzzy_dedup_enabled: bool = True
    fuzzy_similarity_threshold: float = 0.9
    minhash_num_perm: int = 128
    
    # PII filter patterns
    pii_patterns: List[str] = None
    
    # Quality filter settings
    min_unique_ratio: float = 0.3
    min_sentence_count: int = 3
    
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

