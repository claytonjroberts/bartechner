from pathlib import Path

PATH_DATA_RAW = Path() / "data" / "raw"

REGEX_NAME_TERMS = r"(?:(?<=[a-zA-Z])|(?<=^))([A-Z]+|[A-Z][a-z]+)(?:(?=[A-Z])|(?=$))"
