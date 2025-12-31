import argparse
import sys
import os
import random
from .streamer import DatasetStreamer
from .validators import DataValidator
from .stats import DatasetStats
from .crawler import FileCrawler
from .sharder import ShardedWriter
from .checkpoint import CheckpointManager

def ingest_command(args):
    print(f"🚀 Starting Engine (Production Mode)...")
    print(f"   Input:      {args.input}")
    print(f"   Output:     {args.output}-XXXX.jsonl")
    
    # 0. Deterministic Seed
    random.seed(42)
    
    # 1. Initialize Components
    crawler = FileCrawler()
    stats = DatasetStats()
    validator = DataValidator(
        min_length=10, 
        check_english=args.english, 
        max_symbol_ratio=0.3
    )
    
    output_prefix = args.output.replace(".jsonl", "")
    writer = ShardedWriter(output_prefix, shard_size=args.shard_size)
    
    # --- CHECKPOINT INITIALIZATION ---
    # We create a hidden file like ".checkpoint_my_job.txt"
    ckpt_path = f".checkpoint_{os.path.basename(output_prefix)}.txt"
    checkpoint = CheckpointManager(ckpt_path)
    
    if not args.resume and os.path.exists(ckpt_path):
        print("⚠️  Fresh run: Removing old checkpoint history.")
        os.remove(ckpt_path)
        checkpoint = CheckpointManager(ckpt_path) # Re-init empty
    # ---------------------------------
    
    # 2. Find files
    files = list(crawler.find_files(args.input))
    print(f"   Found {len(files)} file(s).")
    
    if not files:
        print("❌ No files found.")
        sys.exit(1)

    # 3. Processing Loop
    print("\n⏳ Processing...", end="", flush=True)
    
    stop_processing = False

    try:
        for file_path in files:
            if stop_processing: break
            
            # --- SKIP CHECK ---
            if args.resume and checkpoint.is_done(file_path):
                print(f"\n⏩ Skipping (already done): {os.path.basename(file_path)}")
                continue
            # ------------------
            
            try:
                streamer = DatasetStreamer(file_path, text_column=args.col)
                for row in streamer.stream():
                    
                    if args.sample < 1.0 and random.random() > args.sample:
                        continue 

                    is_valid = validator.validate(row)
                    stats.update(is_valid)
                    
                    if is_valid:
                        writer.write_item(row)
                        
                        if args.limit > 0 and stats.valid_count >= args.limit:
                            print(f"\n🛑 Limit of {args.limit} rows reached.")
                            stop_processing = True
                            break
                
                # --- MARK DONE ---
                if not stop_processing:
                    checkpoint.mark_done(file_path)
                # -----------------
                        
            except Exception as e:
                print(f"\n⚠️  Error reading {file_path}: {e}")
                continue
                
    finally:
        writer.close()

    # 4. Final Report
    report = stats.get_report()
    print(f"\n\n📊 SESSION REPORT")
    print(f"--------------------------")
    print(f"✅ Valid Rows:    {report['valid_rows']}")
    print(f"📂 Shards Created: {writer.current_shard_index + 1}")
    print(f"--------------------------")
    if args.resume:
        print(f"💾 Checkpoint:    Active ({ckpt_path})")

def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")

    ingest_parser = subparsers.add_parser("ingest")
    ingest_parser.add_argument("--input", required=True)
    ingest_parser.add_argument("--output", required=True)
    ingest_parser.add_argument("--col", default="text")
    ingest_parser.add_argument("--english", action="store_true", default=True)
    ingest_parser.add_argument("--no-english", action="store_false", dest="english")
    ingest_parser.add_argument("--shard-size", type=int, default=10000)
    ingest_parser.add_argument("--limit", type=int, default=0)
    ingest_parser.add_argument("--sample", type=float, default=1.0)
    
    # NEW FLAG
    ingest_parser.add_argument("--resume", action="store_true", help="Resume from last checkpoint")

    args = parser.parse_args()

    if args.command == "ingest":
        ingest_command(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()