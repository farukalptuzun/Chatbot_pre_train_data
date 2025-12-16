"""
Data loaders for different data sources
Converts various formats to standardized {"text": "..."} format
"""
import json
from typing import Iterator, Dict
from datasets import load_dataset


def load_oscar_tr(output_file: str = "oscar_tr_raw.jsonl", max_examples: int = None) -> str:
    """
    Load Turkish text from mC4 dataset (alternative to OSCAR-TR)
    Uses allenai/mc4 with Turkish language subset
    
    Args:
        output_file: Output file path for normalized data
        max_examples: Maximum number of examples to load (None = no limit)
        
    Returns:
        Path to the normalized file
    """
    limit_str = f" (max: {max_examples:,} examples)" if max_examples else ""
    print(f"Loading Turkish mC4 dataset{limit_str}...")
    ds = load_dataset("allenai/mc4", "tr", split="train", streaming=True)
    
    count = 0
    with open(output_file, "w", encoding="utf-8") as f:
        for x in ds:
            text = x.get("text", "").strip()
            if len(text) > 0:
                json_line = json.dumps({"text": text}, ensure_ascii=False)
                f.write(json_line + "\n")
                count += 1
                if count % 10000 == 0:
                    print(f"  Processed {count:,} examples...")
                if max_examples and count >= max_examples:
                    break
    
    print(f"mC4-TR: Saved {count:,} examples to {output_file}")
    return output_file


def load_wikipedia_tr(output_file: str = "wiki_tr_raw.jsonl", max_examples: int = None) -> str:
    """
    Load Turkish Wikipedia dataset and convert to standardized format
    
    Args:
        output_file: Output file path for normalized data
        max_examples: Maximum number of examples to load (None = no limit)
        
    Returns:
        Path to the normalized file
    """
    limit_str = f" (max: {max_examples:,} examples)" if max_examples else ""
    print(f"Loading Turkish Wikipedia dataset{limit_str}...")
    ds = load_dataset("wikipedia", "20220301.tr", split="train")
    
    count = 0
    with open(output_file, "w", encoding="utf-8") as f:
        for x in ds:
            text = x.get("text", "").strip()
            if len(text) > 0:
                json_line = json.dumps({"text": text}, ensure_ascii=False)
                f.write(json_line + "\n")
                count += 1
                if count % 10000 == 0:
                    print(f"  Processed {count:,} examples...")
                if max_examples and count >= max_examples:
                    break
    
    print(f"Wiki-TR: Saved {count:,} examples to {output_file}")
    return output_file


def load_wikipedia_en(output_file: str = "wiki_en_raw.jsonl", max_examples: int = None) -> str:
    """
    Load English Wikipedia dataset and convert to standardized format
    
    Args:
        output_file: Output file path for normalized data
        max_examples: Maximum number of examples to load (None = no limit)
        
    Returns:
        Path to the normalized file
    """
    limit_str = f" (max: {max_examples:,} examples)" if max_examples else ""
    print(f"Loading English Wikipedia dataset{limit_str}...")
    ds = load_dataset("wikipedia", "20220301.en", split="train")
    
    count = 0
    with open(output_file, "w", encoding="utf-8") as f:
        for x in ds:
            text = x.get("text", "").strip()
            if len(text) > 0:
                json_line = json.dumps({"text": text}, ensure_ascii=False)
                f.write(json_line + "\n")
                count += 1
                if count % 10000 == 0:
                    print(f"  Processed {count:,} examples...")
                if max_examples and count >= max_examples:
                    break
    
    print(f"Wiki-EN: Saved {count:,} examples to {output_file}")
    return output_file


def load_common_crawl(output_file: str = "cc_raw.jsonl", language: str = "en", max_examples: int = None) -> str:
    """
    Load Common Crawl dataset and convert to standardized format
    
    Args:
        output_file: Output file path for normalized data
        language: Language code (e.g., "en", "tr")
        max_examples: Maximum number of examples to load (None = no limit, but defaults to 1M for streaming)
        
    Returns:
        Path to the normalized file
    """
    # Default limit for streaming datasets if not specified
    if max_examples is None:
        max_examples = 1_000_000  # Default 1M for safety
    
    limit_str = f" (max: {max_examples:,} examples)"
    print(f"Loading Common Crawl dataset (language: {language}){limit_str}...")
    # Note: Common Crawl datasets vary - adjust dataset name as needed
    # Example: "allenai/c4" for English Common Crawl
    try:
        if language == "en":
            ds = load_dataset("allenai/c4", "en", split="train", streaming=True)
        else:
            # Adjust for Turkish Common Crawl if available
            ds = load_dataset(f"oscar-corpus/OSCAR-2301", language, split="train")
        
        count = 0
        with open(output_file, "w", encoding="utf-8") as f:
            for x in ds:
                if isinstance(x, dict):
                    text = x.get("text", "").strip()
                else:
                    text = str(x).strip()
                
                if len(text) > 0:
                    json_line = json.dumps({"text": text}, ensure_ascii=False)
                    f.write(json_line + "\n")
                    count += 1
                    if count % 10000 == 0:
                        print(f"  Processed {count:,} examples...")
                    if count >= max_examples:
                        break
        
        print(f"Common Crawl: Saved {count:,} examples to {output_file}")
        return output_file
    except Exception as e:
        print(f"Error loading Common Crawl: {e}")
        return None


def load_jsonl_file(file_path: str) -> Iterator[Dict]:
    """
    Load a JSONL file and yield each line as a dictionary
    
    Args:
        file_path: Path to JSONL file
        
    Yields:
        Dictionary with "text" key
    """
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def load_text_file(file_path: str) -> Iterator[Dict]:
    """
    Load a plain text file and convert to standardized format
    
    Args:
        file_path: Path to text file
        
    Yields:
        Dictionary with "text" key for each paragraph/line
    """
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if text:
                yield {"text": text}

