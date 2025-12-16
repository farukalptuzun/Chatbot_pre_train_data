"""
Format normalization module
Converts various input formats to standardized {"text": "..."} format
"""
import json
from typing import Dict, Iterator, Any


def normalize_to_jsonl(input_data: Any) -> str:
    """
    Normalize input data to JSONL format with {"text": "..."} structure
    
    Args:
        input_data: Can be string, dict, or list
        
    Returns:
        JSONL string line
    """
    if isinstance(input_data, dict):
        # If already has "text" key, use it
        if "text" in input_data:
            text = input_data["text"]
        else:
            # Try to extract text from common keys
            text = input_data.get("content", input_data.get("body", str(input_data)))
    elif isinstance(input_data, str):
        text = input_data
    else:
        text = str(input_data)
    
    # Ensure text is a string and strip
    text = str(text).strip()
    
    # Create standardized format
    normalized = {"text": text}
    return json.dumps(normalized, ensure_ascii=False)


def normalize_dataset(input_file: str, output_file: str):
    """
    Normalize an entire dataset file to standard format
    
    Args:
        input_file: Input file path (JSONL, JSON, or text)
        output_file: Output JSONL file path
    """
    import os
    
    file_ext = os.path.splitext(input_file)[1].lower()
    
    with open(output_file, "w", encoding="utf-8") as out_f:
        if file_ext == ".jsonl":
            # Already JSONL format, just normalize structure
            with open(input_file, "r", encoding="utf-8") as in_f:
                for line in in_f:
                    line = line.strip()
                    if line:
                        try:
                            data = json.loads(line)
                            normalized = normalize_to_jsonl(data)
                            out_f.write(normalized + "\n")
                        except json.JSONDecodeError:
                            continue
        elif file_ext == ".json":
            # JSON array or object
            with open(input_file, "r", encoding="utf-8") as in_f:
                data = json.load(in_f)
                if isinstance(data, list):
                    for item in data:
                        normalized = normalize_to_jsonl(item)
                        out_f.write(normalized + "\n")
                elif isinstance(data, dict):
                    normalized = normalize_to_jsonl(data)
                    out_f.write(normalized + "\n")
        else:
            # Plain text file
            with open(input_file, "r", encoding="utf-8") as in_f:
                for line in in_f:
                    text = line.strip()
                    if text:
                        normalized = normalize_to_jsonl(text)
                        out_f.write(normalized + "\n")

