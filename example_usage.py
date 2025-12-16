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
    """Example: Run full pipeline with different data sources"""
    
    # Option 1: Load data from HuggingFace datasets
    print("Loading data sources...")
    
    # Load Turkish sources
    oscar_tr_file = load_oscar_tr("raw_data/oscar_tr_raw.jsonl")
    wiki_tr_file = load_wikipedia_tr("raw_data/wiki_tr_raw.jsonl")
    
    # Load English sources
    wiki_en_file = load_wikipedia_en("raw_data/wiki_en_raw.jsonl")
    cc_en_file = load_common_crawl("raw_data/cc_en_raw.jsonl", language="en")
    
    # Prepare data sources dictionary
    # You can either provide file paths (if already loaded) or loader functions
    data_sources = {
        "oscar_tr": oscar_tr_file,
        "wiki_tr": wiki_tr_file,
        "wiki_en": wiki_en_file,
        "common_crawl": cc_en_file,
        # Add your custom data sources here:
        # "tech_docs": "path/to/tech_docs.jsonl",
        # "curated": "path/to/curated.jsonl",
    }
    
    # Run full pipeline
    run_full_pipeline(
        data_sources=data_sources,
        output_file="output/train.jsonl"
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

