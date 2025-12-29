import argparse
import sys
import os
import json
from .streamer import DatasetStreamer
from .validators import DataValidator
from .jsonl_writer import JSONLWriter
from .stats import DatasetStats

def ingest_command(args):
    """
    Handles the 'ingest' command logic with statistics and language filtering.
    """
    print(f"🚀 Starting ingestion pipeline...")
    print(f"   Input:  {args.input}")
    print(f"   Output: {args.output}")
    print(f"   Column: {args.col}")
    print(f"   English Only: {args.english}")
    
    # 1. Initialize Components
    try:
        streamer = DatasetStreamer(args.input, text_column=args.col)
        validator = DataValidator(min_length=10, check_english=args.english)
        writer = JSONLWriter(args.output)
        stats = DatasetStats()
        
        # 2. Process Loop (Modified to track stats)
        buffer = []
        batch_size = 1000 # Write in chunks for efficiency
        
        print("\n⏳ Processing...", end="", flush=True)
        
        for row in streamer.stream():
            is_valid = validator.validate(row)
            stats.update(is_valid)
            
            if is_valid:
                buffer.append(row)
                
            # Write batch
            if len(buffer) >= batch_size:
                writer.write_stream(iter(buffer))
                buffer = []
                
        # Write remaining
        if buffer:
            writer.write_stream(iter(buffer))
            
        # 3. Final Report
        report = stats.get_report()
        
        print(f"\n\n📊 DATASET HEALTH REPORT")
        print(f"--------------------------")
        print(f"✅ Valid Rows:    {report['valid_rows']}")
        print(f"🗑️  Dropped Rows:  {report['dropped_rows']}")
        print(f"📉 Drop Rate:     {report['drop_rate_percent']}%")
        print(f"⏱️  Time Elapsed:  {report['elapsed_seconds']}s")
        print(f"⚡ Speed:         {report['speed_rows_per_sec']} rows/sec")
        print(f"--------------------------")
        print(f"Saved to: {os.path.abspath(args.output)}")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="NLP Dataset Engine CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Command: ingest
    ingest_parser = subparsers.add_parser("ingest", help="Ingest CSV and convert to JSONL")
    ingest_parser.add_argument("--input", required=True, help="Path to input CSV file")
    ingest_parser.add_argument("--output", required=True, help="Path to output JSONL file")
    ingest_parser.add_argument("--col", default="text", help="Name of text column")
    
    # New flag: --english / --no-english
    ingest_parser.add_argument("--english", action="store_true", default=True, help="Filter for English text only (default: True)")
    ingest_parser.add_argument("--no-english", action="store_false", dest="english", help="Disable English filtering")

    args = parser.parse_args()

    if args.command == "ingest":
        ingest_command(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()