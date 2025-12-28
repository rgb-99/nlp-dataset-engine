import argparse
import sys
from nlp_dataset_engine.data_loader import DatasetLoader

def main():
    parser = argparse.ArgumentParser(description="NLP Dataset Engine: High-performance pre-processing")
    
    # Define the arguments the user can pass
    parser.add_argument("input_path", help="Path to raw text file or directory")
    parser.add_argument("output_path", help="Path to save the JSONL file")
    parser.add_argument("--min-len", type=int, default=10, help="Minimum text length to keep (default: 10)")

    args = parser.parse_args()

    print(f"🚀 Starting engine on: {args.input_path}")
    
    try:
        loader = DatasetLoader(args.input_path, min_length=args.min_len)
        stats = loader.export_to_jsonl(args.output_path)
        
        print("\n✅ Processing Complete!")
        print("-" * 30)
        print(f"Total Lines Scanned:  {stats['total_processed']}")
        print(f"Valid Lines Kept:     {stats['valid_yielded']}")
        print(f"Dropped (Too Short):  {stats['dropped_too_short']}")
        print("-" * 30)
        print(f"Output saved to: {args.output_path}")

    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
