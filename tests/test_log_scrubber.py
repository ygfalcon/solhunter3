import math
import re
from pathlib import Path
from typing import Iterable, List, Tuple

HIGH_ENTROPY_THRESHOLD = 4.0
MIN_TOKEN_LENGTH = 20

SECRET_PATTERNS = [
    re.compile(r"sk_[a-zA-Z0-9]{16,}"),
    re.compile(r"sk-[a-zA-Z0-9]{16,}"),
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"ASIA[0-9A-Z]{16}"),
    re.compile(r"(?i)gh[pous]_[0-9a-zA-Z]{36,}"),
    re.compile(r"xox[aboprst]-[0-9A-Za-z-]{10,}"),
    re.compile(r"(?i)-----BEGIN [^-]+ PRIVATE KEY-----"),
    re.compile(r"(?i)aws_secret_access_key\s*[:=]\s*[0-9A-Za-z/+]{20,}"),
]

TOKEN_REGEX = re.compile(r"[A-Za-z0-9+/=_-]{%d,}" % MIN_TOKEN_LENGTH)
HEX_REGEX = re.compile(r"\b[0-9a-fA-F]{32,}\b")

SAFE_SUBSTRINGS = {
    "[redacted]",
    "<REDACTED>",
    "***",
}

SAFE_PREFIXES = (
    "trace-",
    "demo-",
    "placeholder-",
)

SAMPLE_DIRECTORIES = [
    Path(__file__).parent / "data" / "log_samples",
    Path(__file__).parent / "data" / "uiPayloadSamples",
]


def shannon_entropy(value: str) -> float:
    if not value:
        return 0.0
    counts = {}
    for char in value:
        counts[char] = counts.get(char, 0) + 1
    length = len(value)
    entropy = 0.0
    for count in counts.values():
        probability = count / length
        entropy -= probability * math.log2(probability)
    return entropy


def _iter_sample_files() -> Iterable[Path]:
    for directory in SAMPLE_DIRECTORIES:
        if not directory.exists():
            continue
        for path in directory.rglob("*"):
            if path.is_file():
                yield path


def _collect_secret_matches(path: Path, text: str) -> List[Tuple[str, str]]:
    hits: List[Tuple[str, str]] = []
    for pattern in SECRET_PATTERNS:
        for match in pattern.finditer(text):
            token = match.group(0)
            if any(safe in token for safe in SAFE_SUBSTRINGS):
                continue
            hits.append((str(path), token))
    return hits


def _collect_entropy_matches(path: Path, text: str) -> List[Tuple[str, str]]:
    hits: List[Tuple[str, str]] = []
    for match in TOKEN_REGEX.finditer(text):
        token = match.group(0)
        if any(token.startswith(prefix) for prefix in SAFE_PREFIXES):
            continue
        if any(safe in token for safe in SAFE_SUBSTRINGS):
            continue
        if "=" in token and not token.endswith("=") and not token.endswith("=="):
            # Skip obvious key=value pairs such as retry_backoff_ms=250.
            continue
        entropy = shannon_entropy(token)
        if entropy >= HIGH_ENTROPY_THRESHOLD:
            hits.append((str(path), token))
    for match in HEX_REGEX.finditer(text):
        token = match.group(0)
        if any(safe in token for safe in SAFE_SUBSTRINGS):
            continue
        hits.append((str(path), token))
    return hits


def test_log_and_ui_samples_are_scrubbed():
    offenders: List[Tuple[str, str]] = []
    for path in _iter_sample_files():
        text = path.read_text(encoding="utf-8", errors="ignore")
        offenders.extend(_collect_secret_matches(path, text))
        offenders.extend(_collect_entropy_matches(path, text))
    assert not offenders, (
        "Potential secret material detected in observability fixtures:\n" +
        "\n".join(f"{location}: {token}" for location, token in offenders)
    )
