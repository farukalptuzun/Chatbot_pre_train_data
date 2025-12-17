"""
Example usage script for the pretrain data pipeline
"""
import os
from data_loaders import (
    load_oscar_tr,
    load_wikipedia_tr,
    load_wikipedia_en,
    load_common_crawl
)
from pipeline import run_full_pipeline


def main():
    """
    Example: Run full pipeline with ratio-aware mixing
    
    The pipeline will:
    1. Calculate fetch counts based on DATASET_MIX ratios (with overfetch)
    2. Load datasets with calculated limits
    3. Process through cleaning/dedup/filters
    4. Mix final output to match target ratios
    """
    from config import DATASET_MIX, TOTAL_TARGET_EXAMPLES
    
    print(f"Target dataset size: {TOTAL_TARGET_EXAMPLES:,} examples")
    print(f"Dataset mix ratios:")
    for source, ratios in DATASET_MIX.items():
        print(f"  {source:12s}: {ratios['target']*100:>5.2f}%")
    print()
    
    # Prepare data sources dictionary
    # Pass None to use default loader with calculated limits, or pass file path if already loaded
    data_sources = {
        "mc4_tr": None,  # None = use default loader with calculated max_examples
        "wiki_tr": None,  # Pipeline will call load_wikipedia_tr with calculated limit
        "wiki_en": None,
        "c4_en": None,
        # Or pass file paths if data is already loaded:
        # "tech_docs": "path/to/tech_docs.jsonl",
    }
    
    # Run full pipeline (will handle loading with limits and ratio-aware mixing)
    # Set use_parallel=True to enable per-source parallel cleaning with global dedup/mix
    run_full_pipeline(
        data_sources=data_sources,
        output_file="output/train.jsonl",
        use_parallel=True,         # parallel per-source cleaning
        processes=4,               # adjust to CPU cores / RAM (4–6 recommended)
        progress_interval=10000,   # lower to 1000 for finer logs
    )


def example_single_file():
    """Example: Process a single file"""
    from pipeline import process_jsonl_file
    
    process_jsonl_file(
        input_file="raw_data/my_data.jsonl",
        output_file="output/my_data_cleaned.jsonl"
    )


def example_multiple_files():
    """Example: Process multiple files"""
    from pipeline import process_multiple_files
    
    input_files = [
        "raw_data/file1.jsonl",
        "raw_data/file2.jsonl",
        "raw_data/file3.jsonl",
    ]
    
    process_multiple_files(
        input_files=input_files,
        output_file="output/combined.jsonl",
        reset_dedup_between=False  # Dedup across all files
    )


def example_custom_config():
    """Example: Custom configuration"""
    from config import config
    
    # Modify config before running pipeline
    config.min_text_length = 300  # Increase minimum length
    config.fuzzy_similarity_threshold = 0.85  # Stricter fuzzy dedup
    config.allowed_languages = ["tr", "en", "de"]  # Add more languages
    
    # Then run pipeline normally
    # ...


if __name__ == "__main__":
    # Create necessary directories
    os.makedirs("raw_data", exist_ok=True)
    os.makedirs("output", exist_ok=True)
    
    # Run example
    main()

