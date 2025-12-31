import os
import json
from typing import Dict, Any, Iterable

class ShardedWriter:
    """
    Writes data into multiple split files (shards) to keep sizes manageable.
    Example: data-0000.jsonl, data-0001.jsonl
    """
    def __init__(self, output_prefix: str, shard_size: int = 10000):
        self.output_prefix = output_prefix
        self.shard_size = shard_size
        self.current_shard_index = 0
        self.current_count = 0
        self.file_handle = None
        self._open_new_shard()

    def _get_shard_filename(self) -> str:
        # If prefix is "data/clean", result is "data/clean-0000.jsonl"
        return f"{self.output_prefix}-{self.current_shard_index:04d}.jsonl"

    def _open_new_shard(self):
        """Closes current file and opens the next shard."""
        if self.file_handle:
            self.file_handle.close()
        
        filename = self._get_shard_filename()
        # Ensure directory exists
        os.makedirs(os.path.dirname(os.path.abspath(filename)), exist_ok=True)
        
        self.file_handle = open(filename, "w", encoding="utf-8")
        self.current_count = 0
        print(f"   --> Writing to shard: {os.path.basename(filename)}")

    def write_item(self, item: Dict[str, Any]):
        """Writes a single item, rotating shards if necessary."""
        if self.current_count >= self.shard_size:
            self.current_shard_index += 1
            self._open_new_shard()
            
        self.file_handle.write(json.dumps(item) + "\n")
        self.current_count += 1

    def close(self):
        if self.file_handle:
            self.file_handle.close()