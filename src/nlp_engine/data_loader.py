import json
from pathlib import Path
from typing import Generator, Dict, Union, List

class DatasetLoader:
    """
    A memory-efficient loader for NLP datasets.
    Converts raw text files into JSONL format compatible with Hugging Face.
    """

    def __init__(self, source_path: str):
        self.source_path = Path(source_path)
        if not self.source_path.exists():
            raise FileNotFoundError(f"Path not found: {source_path}")

    def stream_data(self) -> Generator[Dict[str, str], None, None]:
        """Lazy-loads data line by line."""
        if self.source_path.is_file():
            yield from self._process_file(self.source_path)
        else:
            for file_path in self.source_path.glob("*.txt"):
                yield from self._process_file(file_path)

    def _process_file(self, file_path: Path) -> Generator[Dict[str, str], None, None]:
        """Helper to read a single file efficiently."""
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                clean_line = line.strip()
                if clean_line:
                    yield {"text": clean_line, "source": file_path.name}

    def export_to_jsonl(self, output_path: str) -> None:
        """
        Writes the dataset to a JSONL file (One JSON object per line).
        This is the standard format for LLM fine-tuning.
        """
        out = Path(output_path)
        # Ensure parent directory exists
        out.parent.mkdir(parents=True, exist_ok=True)
        
        with open(out, "w", encoding="utf-8") as f:
            for record in self.stream_data():
                f.write(json.dumps(record) + "\n")