import argparse
import sys
import os
from .streamer import DatasetStreamer
from .validators import DataValidator
from .jsonl_writer import JSONLWriter
from .stats import DatasetStats
from .crawler import FileCrawler

def ingest_command(args):
    """
    Handles recursive ingestion of files or folders.
    """
    print(f"🚀 Starting Multi-File Ingestion...")
    print(f"   Input Path: {args.input}")
    print(f"   Output:     {args.output}")
    print(f"   Filter:     English Only = {args.english}")
    
    # 1. Initialize Global Components
    crawler = FileCrawler()
    stats = DatasetStats() # One stats counter for ALL files
    validator = DataValidator(min_length=10, check_english=args.english)
    writer = JSONLWriter(args.output)
    
    # 2. Find all files
    files = list(crawler.find_files(args.input))
    print(f"   Found {len(files)} file(s) to process.")
    
    if not files:
        print("❌ No CSV or TXT files found.")
        sys.exit(1)

    # 3. Process Loop (Iterate over files -> Iterate over rows)
    buffer = []
    batch_size = 1000
    
    print("\n⏳ Processing...", end="", flush=True)
    
    for file_path in files:
        try:
            # New streamer for each file
            streamer = DatasetStreamer(file_path, text_column=args.col)
            
            for row in streamer.stream():
                is_valid = validator.validate(row)
                stats.update(is_valid)
                
                if is_valid:
                    buffer.append(row)
                
                # Write batch
                if len(buffer) >= batch_size:
                    writer.write_stream(iter(buffer))
                    buffer = []
                    
        except Exception as e:
            print(f"\n⚠️  Error reading {file_path}: {e}")
            continue # Skip bad file, keep going

    # Write remaining buffer
    if buffer:
        writer.write_stream(iter(buffer))
            
    # 4. Final Report
    report = stats.get_report()
    
    print(f"\n\n📊 AGGREGATED DATASET REPORT")
    print(f"--------------------------")
    print(f"📂 Files Processed: {len(files)}")
    print(f"✅ Valid Rows:      {report['valid_rows']}")
    print(f"🗑️  Dropped Rows:    {report['dropped_rows']}")
    print(f"📉 Drop Rate:       {report['drop_rate_percent']}%")
    print(f"⏱️  Time Elapsed:    {report['elapsed_seconds']}s")
    print(f"⚡ Speed:           {report['speed_rows_per_sec']} rows/sec")
    print(f"--------------------------")
    print(f"Saved to: {os.path.abspath(args.output)}")

def main():
    parser = argparse.ArgumentParser(description="NLP Dataset Engine CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Command: ingest
    ingest_parser = subparsers.add_parser("ingest", help="Ingest CSV/TXT recursively")
    ingest_parser.add_argument("--input", required=True, help="Path to input file OR folder")
    ingest_parser.add_argument("--output", required=True, help="Path to output JSONL file")
    ingest_parser.add_argument("--col", default="text", help="Name of text column")
    ingest_parser.add_argument("--english", action="store_true", default=True, help="Filter for English text only")
    ingest_parser.add_argument("--no-english", action="store_false", dest="english", help="Disable English filtering")

    args = parser.parse_args()

    if args.command == "ingest":
        ingest_command(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()