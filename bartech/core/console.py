import datetime
import enum
import math
import os
import re
import typing
import warnings
from collections import defaultdict
from dataclasses import dataclass, field, fields
from statistics import median
from threading import Lock, RLock, Thread
from typing import List

import colorama
import urwid
from colorama import Back, Fore, Style

from . import helpers
from .locks import SLock

colorama.init(autoreset=True)

COLORS = {
    "CRITICAL": Fore.RED,
    "WARNING": Fore.RED,
    "CAUTION": Fore.YELLOW,
    "IMPORTANT": Fore.CYAN,
    "GOOD": Fore.GREEN,
}


def getDimentions() -> typing.Tuple[int]:
    return (int(x) for x in os.popen("stty size", "r").read().split())


def getRows() -> int:
    try:
        return getDimentions()[0]
    except IndexError:
        return 40


def getCols() -> int:
    try:
        return getDimentions()[1]
    except IndexError:
        return 80


_LOCK_OUTPUT = RLock()


def output(
    message: str,
    source: object = None,
    option_prompt: bool = False,
    option_confirm: bool = False,
    option_pause: bool = False,
    end: str = "\n",
    # fill: str = None,
    option_line_clear: bool = False,
    option_timeStamp: bool = True,
    tag: bool = True,
    # option_critical: bool = False,
    # option_warning: bool = False,
    # option_caution: bool = False,
    # option_good: bool = False,
    # option_important: bool = False,
    option_status: str = None,
):
    # if (
    #     cls in cls.silence or not getattr(cls, "_debug", True)
    # ) and option_status not in [ConsoleInterfaceStatus.CRITICAL]:
    #     return

    # if fill == " ":
    #     option_line_clear = True
    #     fill = None

    # if option_critical or option_status in ["crit", "critical"]:
    _color = COLORS.get(
        (
            option_status
            if not isinstance(option_status, str)
            else option_status.upper()
        ),
        Fore.BLUE,
    )

    try:
        option_status = option_status.lower()
    except AttributeError:
        pass

    if option_status == "critical":
        _tagType = f"{_color}!CRIT!"
    elif option_status == "warning":
        _tagType = f"{_color}{{{Fore.RESET}WARN{_color}}}"
    elif option_status == "caution":
        _tagType = f"{_color}-{Fore.RESET}CAUT{_color}-"
    elif option_status == "important":
        _tagType = f"{_color}+{Fore.RESET}IMPT{_color}+"
    elif option_status == "good":
        _tagType = f"{_color}({Fore.RESET}GOOD{_color})"
    elif end == "\r":
        # Asssume temporary
        _color = Fore.RESET
        _tagType = None
    elif option_prompt or option_pause:
        _color = COLORS.get("important", Fore.BLUE)
        _tagType = f"{_color}?{Fore.RESET}PRMT{_color}>"
    else:
        _tagType = f"INFO"

    _tagLength = 25
    try:
        tag = source.__name__
    except AttributeError:
        tag = str(source)

    if len(tag) > _tagLength:

        tagParts = helpers.get_name_terms(tag)
        i = 1
        while len(tag) > _tagLength:
            tag = "".join(
                [f"{x[0]}." for x in tagParts[:i]] + tagParts[i:]
            )  # Get first letter of each term

    _tagClass = (
        f"{Fore.RESET}[{Fore.MAGENTA}{tag :^{_tagLength}}{Fore.RESET}]" if tag else None
    )

    msg = f" {Fore.RESET}".join(
        x
        for x in [
            (
                f"{Fore.BLACK}({datetime.datetime.now().strftime('%b-%d %H:%M:%S')})"
                if option_timeStamp
                else None
            ),
            f"{Fore.WHITE}{_tagType :^6}" if _tagType else None,
            f"{_tagClass}" if _tagClass else None,
            f"{_color}{message}",
            Fore.RESET,
        ]
        if x is not None
    )
    # cols = cls.getCols()
    # cols = cols * math.ceil(len(msg) / cols)
    # if fill:
    #     msg += fill * int((cols - (len(msg) % cols)) // len(fill))

    if option_line_clear:
        # "Clearline escape sequence"
        msg = "\033[2K" + msg

    # if option_warning:
    #     warnings.warn(msg)

    # Determine color

    with _LOCK_OUTPUT:
        if option_prompt or option_pause:
            return input(msg)

        elif option_confirm:
            tries = 0
            while True:
                i = output(
                    message=f"{message} [Y/n]", option_prompt=True, source=source
                )
                if i == "Y":
                    return True
                elif i == "n":
                    return False
                else:
                    output(
                        message=f"{i !r} not a valid response, try again.",
                        option_caution=True,
                        source=source,
                    )
                tries += 1
                if tries >= 2:
                    output(
                        message="Tries exceeded, quitting",
                        option_warning=True,
                        source=source,
                    )
                    break
        else:
            print(msg, end=end)
