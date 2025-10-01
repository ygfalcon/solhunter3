from solhunter_zero.datasets.alien_artifact import load_patterns, get_encoding_by_glyphs


def test_load_patterns_structure():
    patterns = load_patterns()
    assert isinstance(patterns, list)
    assert patterns, "dataset should not be empty"
    first = patterns[0]
    assert "glyphs" in first and "encoding" in first
    assert isinstance(first["glyphs"], str)
    assert isinstance(first["encoding"], list)


def test_get_encoding_lookup():
    patterns = load_patterns()
    sample = patterns[0]
    enc = get_encoding_by_glyphs(sample["glyphs"])
    assert enc == sample["encoding"]
