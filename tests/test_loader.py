import json
import pytest
from nlp_engine.data_loader import DatasetLoader

def test_file_not_found():
    """It should raise an error if path doesn't exist."""
    with pytest.raises(FileNotFoundError):
        DatasetLoader("fake_path.txt")

def test_stream_data(tmp_path):
    """It should correctly read a dummy file."""
    # 1. Create a dummy file in a temp directory
    d = tmp_path / "data"
    d.mkdir()
    p = d / "hello.txt"
    p.write_text("Hello GSoC\nLine 2", encoding="utf-8")

    # 2. Run our loader
    loader = DatasetLoader(str(p))
    results = list(loader.stream_data())

    # 3. Assertions
    assert len(results) == 2
    assert results[0]["text"] == "Hello GSoC"
    assert results[0]["source"] == "hello.txt"

def test_export_jsonl(tmp_path):
    """It should create a valid JSONL file."""
    # 1. Setup dummy source
    d = tmp_path / "data"
    d.mkdir()
    (d / "source.txt").write_text("Line 1\nLine 2", encoding="utf-8")
    
    # 2. Run export
    loader = DatasetLoader(str(d))
    output_file = tmp_path / "output.jsonl"
    loader.export_to_jsonl(str(output_file))
    
    # 3. Verify output
    assert output_file.exists()
    # Read the file back and parse the first line
    lines = output_file.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    
    first_record = json.loads(lines[0])
    assert first_record["text"] == "Line 1"