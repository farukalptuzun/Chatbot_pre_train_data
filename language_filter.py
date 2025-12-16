"""
Language detection and filtering module
Filters texts to only include specified languages using fasttext
"""
import os
from typing import Tuple, Optional
from config import config


# Global language model (lazy loading)
_lang_model = None


def load_language_model(model_path: Optional[str] = None):
    """
    Load fasttext language detection model
    
    Args:
        model_path: Path to fasttext model file (default: from config)
    """
    global _lang_model
    
    if _lang_model is not None:
        return _lang_model
    
    try:
        import fasttext
    except ImportError:
        raise ImportError(
            "fasttext is required for language detection. "
            "Install with: pip install fasttext"
        )
    
    model_path = model_path or config.lang_model_path
    
    if not os.path.exists(model_path):
        print(f"Warning: Language model not found at {model_path}")
        print("Downloading lid.176.bin...")
        # Fasttext will download automatically, or download manually:
        # wget https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin
        try:
            # Try to load from fasttext's default location
            _lang_model = fasttext.load_model("lid.176.bin")
        except:
            raise FileNotFoundError(
                f"Language model not found. Please download lid.176.bin and place it at {model_path}"
            )
    else:
        _lang_model = fasttext.load_model(model_path)
    
    return _lang_model


def detect_language(text: str, model_path: Optional[str] = None) -> Tuple[str, float]:
    """
    Detect language of text using fasttext
    
    Args:
        text: Input text
        model_path: Path to fasttext model (optional)
        
    Returns:
        Tuple of (language_code, confidence_score)
    """
    model = load_language_model(model_path)
    
    # Prepare text (first 1000 chars, replace newlines)
    text_sample = text.replace("\n", " ")[:1000]
    
    if not text_sample.strip():
        return "unknown", 0.0
    
    # Predict language
    label, prob = model.predict(text_sample, k=1)
    
    # Extract language code (remove __label__ prefix)
    lang = label[0].replace("__label__", "")
    confidence = float(prob[0])
    
    return lang, confidence


def language_filter(text: str, model_path: Optional[str] = None) -> bool:
    """
    Check if text is in one of the allowed languages
    
    Args:
        text: Input text
        model_path: Path to fasttext model (optional)
        
    Returns:
        True if text is in allowed language with sufficient confidence
    """
    try:
        lang, prob = detect_language(text, model_path)
        return (lang in config.allowed_languages) and (prob >= config.min_lang_confidence)
    except Exception as e:
        # If language detection fails, skip the text for safety
        print(f"Language detection error: {e}")
        return False

