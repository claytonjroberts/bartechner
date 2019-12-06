from pathlib import Path
import typing
import re


def get_path_from_name(name: str, path: Path) -> typing.List[Path]:
    list_name_file_template = [
        pth
        for pth in path.iterdir()
        if (not pth.is_dir() and re.search(rf"^{name}\.", pth.name, re.IGNORECASE))
    ]

    if len(list_name_file_template) > 1:
        raise AssertionError(
            f"Multiple template files found for {name}: {list_name_file_template}"
        )
    elif len(list_name_file_template) == 0:
        raise AssertionError(f"No template files found for {name}")

    return list_name_file_template[0]
