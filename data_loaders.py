"""
Data loaders for different data sources
Converts various formats to standardized {"text": "..."} format
"""
import json
from typing import Iterator, Dict
from datasets import load_dataset


def load_oscar_tr(output_file: str = "oscar_tr_raw.jsonl") -> str:
    """
    Load OSCAR-TR dataset and convert to standardized format
    
    Args:
        output_file: Output file path for normalized data
        
    Returns:
        Path to the normalized file
    """
    print(f"Loading OSCAR-TR dataset...")
    ds = load_dataset("oscar-corpus/OSCAR-2301", "tr", split="train")
    
    count = 0
    with open(output_file, "w", encoding="utf-8") as f:
        for x in ds:
            text = x.get("text", "").strip()
            if len(text) > 0:
                json_line = json.dumps({"text": text}, ensure_ascii=False)
                f.write(json_line + "\n")
                count += 1
                if count % 10000 == 0:
                    print(f"  Processed {count} examples...")
    
    print(f"OSCAR-TR: Saved {count} examples to {output_file}")
    return output_file


def load_wikipedia_tr(output_file: str = "wiki_tr_raw.jsonl") -> str:
    """
    Load Turkish Wikipedia dataset and convert to standardized format
    
    Args:
        output_file: Output file path for normalized data
        
    Returns:
        Path to the normalized file
    """
    print(f"Loading Turkish Wikipedia dataset...")
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
                    print(f"  Processed {count} examples...")
    
    print(f"Wiki-TR: Saved {count} examples to {output_file}")
    return output_file


def load_wikipedia_en(output_file: str = "wiki_en_raw.jsonl") -> str:
    """
    Load English Wikipedia dataset and convert to standardized format
    
    Args:
        output_file: Output file path for normalized data
        
    Returns:
        Path to the normalized file
    """
    print(f"Loading English Wikipedia dataset...")
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
                    print(f"  Processed {count} examples...")
    
    print(f"Wiki-EN: Saved {count} examples to {output_file}")
    return output_file


def load_common_crawl(output_file: str = "cc_raw.jsonl", language: str = "en") -> str:
    """
    Load Common Crawl dataset and convert to standardized format
    
    Args:
        output_file: Output file path for normalized data
        language: Language code (e.g., "en", "tr")
        
    Returns:
        Path to the normalized file
    """
    print(f"Loading Common Crawl dataset (language: {language})...")
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
                        print(f"  Processed {count} examples...")
                    if count >= 1000000:  # Limit for streaming datasets
                        break
        
        print(f"Common Crawl: Saved {count} examples to {output_file}")
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

