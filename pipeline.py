"""
Main pipeline module
Combines all processing steps into a single pipeline
"""
import json
import os
from typing import Iterator, Dict, Optional
from pathlib import Path

from config import config
from basic_cleaner import clean_and_filter
from language_filter import language_filter
from deduplication import get_deduplicator, reset_deduplicator
from pii_filter import pii_filter
from quality_filter import quality_filter


def process_text(text: str, deduplicator=None) -> Optional[str]:
    """
    Process a single text through the entire pipeline
    
    Args:
        text: Input text
        deduplicator: Deduplicator instance (optional, will create if not provided)
        
    Returns:
        Processed text if it passes all filters, None otherwise
    """
    # Step 1: Basic cleaning and filtering
    cleaned = clean_and_filter(text)
    if cleaned is None:
        return None
    
    # Step 2: Language filter
    if not language_filter(cleaned):
        return None
    
    # Step 3: Deduplication
    if deduplicator is None:
        deduplicator = get_deduplicator()
    
    if deduplicator.is_duplicate(cleaned):
        return None
    
    # Step 4: PII filter
    if not pii_filter(cleaned):
        return None
    
    # Step 5: Quality filter
    if not quality_filter(cleaned):
        return None
    
    return cleaned


def process_jsonl_file(
    input_file: str,
    output_file: str,
    reset_dedup: bool = True,
    progress_interval: int = 10000
):
    """
    Process a JSONL file through the entire pipeline
    
    Args:
        input_file: Input JSONL file path
        output_file: Output JSONL file path
        reset_dedup: Whether to reset deduplicator before processing
        progress_interval: Print progress every N examples
    """
    if reset_dedup:
        reset_deduplicator()
    
    deduplicator = get_deduplicator()
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else ".", exist_ok=True)
    
    total = 0
    passed = 0
    
    with open(input_file, "r", encoding="utf-8") as in_f, \
         open(output_file, "w", encoding="utf-8") as out_f:
        
        for line in in_f:
            line = line.strip()
            if not line:
                continue
            
            try:
                data = json.loads(line)
                text = data.get("text", "")
                
                if not text:
                    continue
                
                total += 1
                
                # Process through pipeline
                processed = process_text(text, deduplicator)
                
                if processed is not None:
                    # Write to output
                    output_data = {"text": processed}
                    out_f.write(json.dumps(output_data, ensure_ascii=False) + "\n")
                    passed += 1
                
                # Progress reporting
                if total % progress_interval == 0:
                    print(f"Processed: {total:,} | Passed: {passed:,} | Rate: {passed/total*100:.1f}%")
                    
            except json.JSONDecodeError:
                continue
            except Exception as e:
                print(f"Error processing line: {e}")
                continue
    
    print(f"\nFinal Stats:")
    print(f"  Total processed: {total:,}")
    print(f"  Passed filters: {passed:,}")
    print(f"  Filter rate: {passed/total*100:.1f}%")
    print(f"  Output saved to: {output_file}")


def process_multiple_files(
    input_files: list,
    output_file: str,
    reset_dedup_between: bool = False
):
    """
    Process multiple input files and combine into single output
    
    Args:
        input_files: List of input JSONL file paths
        output_file: Output JSONL file path
        reset_dedup_between: Whether to reset dedup between files (False = dedup across all files)
    """
    print(f"Processing {len(input_files)} files...")
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else ".", exist_ok=True)
    
    if not reset_dedup_between:
        reset_deduplicator()
    
    total_all = 0
    passed_all = 0
    
    with open(output_file, "w", encoding="utf-8") as out_f:
        for i, input_file in enumerate(input_files):
            print(f"\nProcessing file {i+1}/{len(input_files)}: {input_file}")
            
            if reset_dedup_between:
                reset_deduplicator()
            
            deduplicator = get_deduplicator()
            
            total = 0
            passed = 0
            
            with open(input_file, "r", encoding="utf-8") as in_f:
                for line in in_f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        data = json.loads(line)
                        text = data.get("text", "")
                        
                        if not text:
                            continue
                        
                        total += 1
                        total_all += 1
                        
                        # Process through pipeline
                        processed = process_text(text, deduplicator)
                        
                        if processed is not None:
                            # Write to output
                            output_data = {"text": processed}
                            out_f.write(json.dumps(output_data, ensure_ascii=False) + "\n")
                            passed += 1
                            passed_all += 1
                            
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        print(f"Error processing line: {e}")
                        continue
            
            print(f"  File stats: {passed:,}/{total:,} passed ({passed/total*100:.1f}%)")
    
    print(f"\nOverall Stats:")
    print(f"  Total processed: {total_all:,}")
    print(f"  Passed filters: {passed_all:,}")
    print(f"  Filter rate: {passed_all/total_all*100:.1f}%")
    print(f"  Output saved to: {output_file}")


def run_full_pipeline(
    data_sources: dict,
    output_file: str = None
):
    """
    Run the full pipeline from data loading to final output
    
    Args:
        data_sources: Dict with source names and their file paths or loader functions
        output_file: Output file path (default: config.train_output_file)
    """
    from data_loaders import (
        load_oscar_tr, load_wikipedia_tr, load_wikipedia_en, load_common_crawl
    )
    
    output_file = output_file or os.path.join(config.output_dir, config.train_output_file)
    
    # Step 1: Load and normalize data sources
    normalized_files = []
    
    if "oscar_tr" in data_sources:
        file_path = data_sources["oscar_tr"]
        if callable(file_path):
            normalized_files.append(file_path())
        else:
            normalized_files.append(file_path)
    
    if "wiki_tr" in data_sources:
        file_path = data_sources["wiki_tr"]
        if callable(file_path):
            normalized_files.append(file_path())
        else:
            normalized_files.append(file_path)
    
    if "wiki_en" in data_sources:
        file_path = data_sources["wiki_en"]
        if callable(file_path):
            normalized_files.append(file_path())
        else:
            normalized_files.append(file_path)
    
    if "common_crawl" in data_sources:
        file_path = data_sources["common_crawl"]
        if callable(file_path):
            normalized_files.append(file_path())
        else:
            normalized_files.append(file_path)
    
    if "tech_docs" in data_sources:
        normalized_files.append(data_sources["tech_docs"])
    
    if "curated" in data_sources:
        normalized_files.append(data_sources["curated"])
    
    # Step 2: Process all files through pipeline
    print(f"\n{'='*60}")
    print(f"Starting pipeline processing...")
    print(f"{'='*60}\n")
    
    process_multiple_files(normalized_files, output_file, reset_dedup_between=False)
    
    print(f"\n{'='*60}")
    print(f"Pipeline completed!")
    print(f"{'='*60}")

