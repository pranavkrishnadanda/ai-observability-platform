import re

_STRIP_PATTERNS = [
    # UUID (must come before general numbers)
    re.compile(
        r'\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b',
        re.I,
    ),
    # IPv4 addresses
    re.compile(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b'),
    # Standalone integers
    re.compile(r'\b\d+\b'),
    # Long quoted strings (tokens, base64, etc.)
    re.compile(r'"[^"]{32,}"'),
]


def normalize_error_template(message: str) -> str:
    """Strip variable parts (UUIDs, IPs, numbers, long tokens) to get error structure.

    Used by the anomaly detector to bucket messages into error templates for
    new_error_pattern detection — two messages with the same template are the
    same error class regardless of the concrete IDs/values they contain.
    """
    result = message
    for pattern in _STRIP_PATTERNS:
        result = pattern.sub("<VAR>", result)
    # Collapse consecutive whitespace introduced by substitution and cap length.
    result = re.sub(r'\s+', ' ', result).strip()
    return result[:512]
