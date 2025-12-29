import argparse
import sys
import os
from .streamer import DatasetStreamer
from .validators import DataValidator
from .jsonl_writer import JSONLWriter

def ingest_command(args):
    """
    Handles the 'ingest' command logic.
    """
    print(f"🚀 Starting ingestion pipeline...")
    print(f"   Input:  {args.input}")
    print(f"   Output: {args.output}")
    print(f"   Column: {args.col}")
    
    # 1. Initialize Components
    try:
        streamer = DatasetStreamer(args.input, text_column=args.col)
        validator = DataValidator(min_length=10)
        writer = JSONLWriter(args.output)
        
        # 2. Build the Pipeline Generator
        # We create a generator expression that filters valid rows on the fly
        valid_stream = (
            row for row in streamer.stream() 
            if validator.validate(row)
        )

        # 3. Execute Pipeline (The Writer pulls the data)
        count = writer.write_stream(valid_stream)
        
        print(f"\n✅ Success! Processed and saved {count} valid records.")
        print(f"   Saved to: {os.path.abspath(args.output)}")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="NLP Dataset Engine CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Command: ingest
    # Usage: nlp-engine ingest --input data.csv --output clean.jsonl --col review
    ingest_parser = subparsers.add_parser("ingest", help="Ingest CSV and convert to JSONL")
    ingest_parser.add_argument("--input", required=True, help="Path to input CSV file")
    ingest_parser.add_argument("--output", required=True, help="Path to output JSONL file")
    ingest_parser.add_argument("--col", default="text", help="Name of the text column in CSV (default: text)")

    args = parser.parse_args()

    if args.command == "ingest":
        ingest_command(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()