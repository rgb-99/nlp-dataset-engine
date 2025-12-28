import json
import pytest
from nlp_engine.data_loader import DatasetLoader

def test_file_not_found():
    with pytest.raises(FileNotFoundError):
        DatasetLoader("fake_path.txt")

def test_stats_tracking(tmp_path):
    """It should correctly count dropped vs valid lines."""
    # 1. Setup: 3 lines total. 
    # - "A" (Too short)
    # - "B" (Too short)
    # - "Hello World GSoC" (Valid)
    d = tmp_path / "data"
    d.mkdir()
    (d / "stats.txt").write_text("A\nB\nHello World GSoC", encoding="utf-8")

    # 2. Run with min_length=5
    loader = DatasetLoader(str(d), min_length=5)
    # Consume the generator to trigger counting
    list(loader.stream_data())

    # 3. Verify Stats
    assert loader.stats["total_processed"] == 3
    assert loader.stats["dropped_too_short"] == 2
    assert loader.stats["valid_yielded"] == 1

def test_export_returns_stats(tmp_path):
    """Export function should return the final report."""
    d = tmp_path / "data"
    d.mkdir()
    (d / "source.txt").write_text("Valid Line One\nTooShort", encoding="utf-8")
    
    loader = DatasetLoader(str(d), min_length=10)
    output_file = tmp_path / "output.jsonl"
    
    # Capture the return value
    final_stats = loader.export_to_jsonl(str(output_file))
    
    # Verify return value
    assert final_stats["valid_yielded"] == 1
    assert final_stats["dropped_too_short"] == 1