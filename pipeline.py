"""
Main pipeline module
Combines all processing steps into a single pipeline
"""
import json
import os
import random
from typing import Iterator, Dict, Optional, List, Tuple
from pathlib import Path
import multiprocessing as mp

from config import (
    config,
    DATASET_MIX,
    TOTAL_TARGET_EXAMPLES,
    OVERFETCH_FACTOR,
    LANGUAGE_FILTER_BY_SOURCE,
)
from basic_cleaner import clean_and_filter
from language_filter import language_filter
from deduplication import get_deduplicator, reset_deduplicator
from pii_filter import pii_filter
from quality_filter import quality_filter


def process_text(
    text: str,
    deduplicator=None,
    language_filter_enabled: bool = True,
    dedup_enabled: bool = True,
) -> Optional[str]:
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
    if language_filter_enabled:
        if not language_filter(cleaned):
            return None
    
    # Step 3: Deduplication
    if dedup_enabled:
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
    progress_interval: int = 10000,
    language_filter_enabled: bool = True,
    dedup_enabled: bool = True,
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
                processed = process_text(
                    text,
                    deduplicator,
                    language_filter_enabled=language_filter_enabled,
                    dedup_enabled=dedup_enabled,
                )
                
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


# ============================================================================
# DATASET MIXING FUNCTIONS (Ratio-aware mixing after cleaning/dedup)
# ============================================================================

def compute_target_counts():
    """
    Calculate target counts for each dataset based on ratios
    
    Returns:
        Dict mapping source names to target example counts
    """
    return {
        k: int(v["target"] * TOTAL_TARGET_EXAMPLES)
        for k, v in DATASET_MIX.items()
    }


def compute_fetch_counts():
    """
    Calculate how much to fetch initially (overfetch to compensate for dedup/filter losses)
    
    Returns:
        Dict mapping source names to fetch counts (with overfetch factor)
    """
    targets = compute_target_counts()
    return {
        k: int(count * OVERFETCH_FACTOR)
        for k, count in targets.items()
    }


def mix_datasets(cleaned_datasets: dict, output_file: str):
    """
    Mix datasets according to target ratios after cleaning/dedup
    
    Args:
        cleaned_datasets: Dict of {source_name: [text1, text2, ...]}
        output_file: Output file path
        
    Returns:
        Dict with statistics about the mixing process
    """
    targets = compute_target_counts()
    
    print(f"\n{'='*60}")
    print(f"Mixing datasets to target ratios...")
    print(f"{'='*60}")
    
    # Statistics
    stats = {}
    total_written = 0
    
    with open(output_file, "w", encoding="utf-8") as out_f:
        for source, texts in cleaned_datasets.items():
            target_count = targets.get(source, 0)
            
            # Shuffle for randomness
            random.shuffle(texts)
            
            # Take up to target count
            keep_n = min(len(texts), target_count)
            selected_texts = texts[:keep_n]
            
            # Write to output
            for text in selected_texts:
                out_f.write(json.dumps({"text": text}, ensure_ascii=False) + "\n")
                total_written += 1
            
            stats[source] = {
                "available": len(texts),
                "target": target_count,
                "selected": keep_n,
                "ratio": keep_n / total_written if total_written > 0 else 0
            }
            
            print(f"  {source:12s}: {keep_n:>8,}/{target_count:>8,} target ({keep_n/target_count*100:.1f}% of target)")
    
    # Print final ratio report
    print(f"\nFinal Mix Ratios:")
    for source, stat in stats.items():
        actual_ratio = stat["selected"] / total_written if total_written > 0 else 0
        target_ratio = DATASET_MIX[source]["target"]
        print(f"  {source:12s}: {actual_ratio*100:>5.2f}% (target: {target_ratio*100:>5.2f}%)")
    
    print(f"\nTotal examples in final dataset: {total_written:,}")
    return stats


def process_and_mix_files(
    input_files_with_sources: List[Tuple[str, str]],
    output_file: str,
    reset_dedup_between: bool = False,
    progress_interval: int = 10000
):
    """
    Process files through pipeline and mix according to target ratios
    
    Args:
        input_files_with_sources: List of (source_name, file_path) tuples
        output_file: Output file path
        reset_dedup_between: Whether to reset dedup between files
        progress_interval: Print progress every N examples
        
    Returns:
        Dict with statistics about the mixing process
    """
    # Step 1: Process all files through cleaning pipeline
    print(f"\n{'='*60}")
    print(f"Step 1: Processing files through cleaning pipeline...")
    print(f"{'='*60}")
    
    if not reset_dedup_between:
        reset_deduplicator()
    
    # Store cleaned texts by source
    cleaned_by_source = {source: [] for source, _ in input_files_with_sources}
    
    for source, input_file in input_files_with_sources:
        print(f"\nProcessing: {source} ({input_file})")
        
        if reset_dedup_between:
            reset_deduplicator()
        
        deduplicator = get_deduplicator()
        lang_enabled = LANGUAGE_FILTER_BY_SOURCE.get(source, True)
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
                    
                    # Process through pipeline
                    processed = process_text(
                        text,
                        deduplicator,
                        language_filter_enabled=lang_enabled,
                        dedup_enabled=True,
                    )
                    
                    if processed is not None:
                        cleaned_by_source[source].append(processed)
                        passed += 1
                    
                    # Progress reporting
                    if total % progress_interval == 0:
                        print(f"  Progress: {total:,} processed | {passed:,} passed | Rate: {passed/total*100:.1f}%")
                
                except (json.JSONDecodeError, Exception) as e:
                    continue
        
        print(f"  {source}: {passed:,}/{total:,} passed ({passed/total*100:.1f}%)")
    
    # Step 2: Mix according to target ratios
    print(f"\n{'='*60}")
    print(f"Step 2: Mixing datasets to target ratios...")
    print(f"{'='*60}")
    
    stats = mix_datasets(cleaned_by_source, output_file)
    
    return stats


def _clean_file_no_dedup(args):
    """
    Worker for parallel cleaning without dedup.
    Args tuple: (source, input_file, temp_output, progress_interval)
    """
    source, input_file, temp_output, progress_interval = args
    lang_enabled = LANGUAGE_FILTER_BY_SOURCE.get(source, True)
    # Clean with language filter, PII, quality; skip dedup for global dedup later
    process_jsonl_file(
        input_file=input_file,
        output_file=temp_output,
        reset_dedup=True,
        progress_interval=progress_interval,
        language_filter_enabled=lang_enabled,
        dedup_enabled=False,
    )
    return source, temp_output


def process_and_mix_files_parallel(
    input_files_with_sources: List[Tuple[str, str]],
    output_file: str,
    progress_interval: int = 10000,
    processes: Optional[int] = None,
):
    """
    Parallel cleaning (per source) without dedup, then global dedup + mix.
    Keeps quality: language filter ON for all sources; dedup is applied globally after cleaning.
    """
    print(f"\n{'='*60}")
    print("Parallel cleaning per source (dedup deferred)...")
    print(f"{'='*60}")

    # Prepare temp outputs
    base_tmp_dir = Path("tmp")
    base_tmp_dir.mkdir(exist_ok=True)
    tasks = []
    for source, input_file in input_files_with_sources:
        temp_output = base_tmp_dir / f"cleaned_{source}.jsonl"
        tasks.append((source, input_file, str(temp_output), progress_interval))

    # Run per-source cleaning in parallel
    procs = processes or min(len(tasks), mp.cpu_count())
    with mp.Pool(processes=procs) as pool:
        results = pool.map(_clean_file_no_dedup, tasks)

    # Global dedup + mix
    print(f"\n{'='*60}")
    print("Global deduplication and mixing...")
    print(f"{'='*60}")

    reset_deduplicator()
    deduplicator = get_deduplicator()
    cleaned_by_source = {source: [] for source, _ in input_files_with_sources}

    for source, temp_output in results:
        total = 0
        kept = 0
        with open(temp_output, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    text = data.get("text", "")
                    if not text:
                        continue
                    total += 1
                    if deduplicator.is_duplicate(text):
                        continue
                    cleaned_by_source[source].append(text)
                    kept += 1
                except Exception:
                    continue
        print(f"  {source}: {kept:,}/{total:,} kept after global dedup")

    # Mix to target ratios
    stats = mix_datasets(cleaned_by_source, output_file)
    return stats


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
    Run the full pipeline from data loading to final output with ratio-aware mixing
    
    Args:
        data_sources: Dict with source names and their file paths or loader functions
                     Keys should match DATASET_MIX keys: "mc4_tr", "wiki_tr", "wiki_en", "c4_en", "tech_docs"
        output_file: Output file path (default: config.train_output_file)
    """
    from data_loaders import (
        load_oscar_tr, load_wikipedia_tr, load_wikipedia_en, load_common_crawl
    )
    
    output_file = output_file or os.path.join(config.output_dir, config.train_output_file)
    
    # Calculate how much to fetch (with overfetch to compensate for dedup/filter losses)
    fetch_counts = compute_fetch_counts()
    
    print(f"\n{'='*60}")
    print(f"Dataset Loading Plan (with {OVERFETCH_FACTOR}x overfetch):")
    print(f"{'='*60}")
    for source, count in fetch_counts.items():
        target = int(DATASET_MIX[source]["target"] * TOTAL_TARGET_EXAMPLES)
        print(f"  {source:12s}: {count:>10,} examples (target: {target:,})")
    print()
    
    # Step 1: Load datasets with calculated limits
    loaded_files_with_sources = []
    
    # Map source keys (support both old and new naming)
    # None = use default loader, file path = use existing file, callable = call it
    if "oscar_tr" in data_sources or "mc4_tr" in data_sources:
        source_name = "mc4_tr"
        file_source = data_sources.get("oscar_tr") or data_sources.get("mc4_tr")
        max_examples = fetch_counts.get(source_name)
        if file_source is None:
            # Use default loader
            file_path = load_oscar_tr("raw_data/mc4_tr_raw.jsonl", max_examples=max_examples)
        elif callable(file_source):
            # Call provided function
            file_path = file_source()
        else:
            # Use provided file path
            file_path = file_source
        loaded_files_with_sources.append((source_name, file_path))
    
    if "wiki_tr" in data_sources:
        source_name = "wiki_tr"
        file_source = data_sources["wiki_tr"]
        max_examples = fetch_counts.get(source_name)
        if file_source is None:
            file_path = load_wikipedia_tr("raw_data/wiki_tr_raw.jsonl", max_examples=max_examples)
        elif callable(file_source):
            file_path = file_source()
        else:
            file_path = file_source
        loaded_files_with_sources.append((source_name, file_path))
    
    if "wiki_en" in data_sources:
        source_name = "wiki_en"
        file_source = data_sources["wiki_en"]
        max_examples = fetch_counts.get(source_name)
        if file_source is None:
            file_path = load_wikipedia_en("raw_data/wiki_en_raw.jsonl", max_examples=max_examples)
        elif callable(file_source):
            file_path = file_source()
        else:
            file_path = file_source
        loaded_files_with_sources.append((source_name, file_path))
    
    if "common_crawl" in data_sources or "c4_en" in data_sources:
        source_name = "c4_en"
        file_source = data_sources.get("common_crawl") or data_sources.get("c4_en")
        max_examples = fetch_counts.get(source_name)
        if file_source is None:
            file_path = load_common_crawl("raw_data/c4_en_raw.jsonl", language="en", max_examples=max_examples)
        elif callable(file_source):
            file_path = file_source()
        else:
            file_path = file_source
        loaded_files_with_sources.append((source_name, file_path))
    
    if "tech_docs" in data_sources:
        source_name = "tech_docs"
        file_path = data_sources["tech_docs"]
        loaded_files_with_sources.append((source_name, file_path))
    
    # Step 2: Process and mix according to target ratios
    print(f"\n{'='*60}")
    print(f"Starting ratio-aware pipeline processing...")
    print(f"{'='*60}\n")
    
    stats = process_and_mix_files(loaded_files_with_sources, output_file, reset_dedup_between=False)
    
    print(f"\n{'='*60}")
    print(f"Pipeline completed!")
    print(f"{'='*60}")

