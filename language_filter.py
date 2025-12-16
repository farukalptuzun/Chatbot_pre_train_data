"""
Language detection and filtering module
Filters texts to only include specified languages using fasttext
"""
import os
from typing import Tuple, Optional
from config import config


# Global language model (lazy loading)
_lang_model = None
_model_download_attempted = False


def _download_language_model(model_path: str) -> bool:
    """
    Download fasttext language model from official source
    
    Args:
        model_path: Path where to save the model
        
    Returns:
        True if download successful, False otherwise
    """
    import urllib.request
    import sys
    
    model_url = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"
    
    try:
        print(f"Downloading language model from {model_url}...")
        print("This may take a few minutes (~1.3 GB)...")
        
        def show_progress(block_num, block_size, total_size):
            downloaded = block_num * block_size
            percent = min(100, (downloaded / total_size) * 100)
            sys.stdout.write(f"\rProgress: {percent:.1f}% ({downloaded / (1024*1024):.1f} MB / {total_size / (1024*1024):.1f} MB)")
            sys.stdout.flush()
        
        urllib.request.urlretrieve(model_url, model_path, show_progress)
        print(f"\nModel downloaded successfully to {model_path}")
        return True
    except Exception as e:
        print(f"\nError downloading model: {e}")
        return False


def load_language_model(model_path: Optional[str] = None):
    """
    Load fasttext language detection model
    
    Args:
        model_path: Path to fasttext model file (default: from config)
    """
    global _lang_model, _model_download_attempted
    
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
    
    # If model doesn't exist and we haven't tried downloading yet, download it
    if not os.path.exists(model_path):
        if not _model_download_attempted:
            _model_download_attempted = True
            print(f"Language model not found at {model_path}")
            if _download_language_model(model_path):
                # Model downloaded successfully, continue to load
                pass
            else:
                raise FileNotFoundError(
                    f"Failed to download language model. "
                    f"Please download manually from: "
                    f"https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin "
                    f"and place it at {model_path}"
                )
        else:
            # We already tried downloading, model still doesn't exist
            raise FileNotFoundError(
                f"Language model not found at {model_path}. "
                f"Download failed or was cancelled."
            )
    
    # Load the model
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

