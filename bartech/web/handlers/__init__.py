import sys
import re
from pathlib import Path


from .base import HandlerPage, HandlerAPI, HandlerWebsocket
from ...core.console import output
from ..constants import PATH_TEMPLATES

# CREATE HandlerPages based on what is in the templates folder

for path_template in [
    x
    for x in PATH_TEMPLATES.iterdir()
    if (not x.is_dir() and re.search(r"\.html(?:\.j2)?", x.name, re.IGNORECASE))
]:
    l = path_template.stem
    # if (
    #     not re.search(
    #         (r"((?:\Wold\W)|(?:\Wnew\W)|(?:(?:\w_)?test(?:_\w)?)|_$)"), l, re.IGNORECASE
    #     )
    #     or _debug
    # ):

    tlist = path_template.name.split(".")[0]
    t = "".join([a if a.isupper() else b for a, b in zip(tlist, tlist.title())])

    _class_handler = type(f"PH_{t}", (HandlerPage,), {})
    output(f"Created handler {_class_handler}")

    setattr(sys.modules[__name__], _class_handler.__name__, _class_handler)

# Allow overwrite of handlers
from .handlers import *
