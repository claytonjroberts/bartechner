import re
import sys
import uuid
from pathlib import Path

from tornado.web import UIModule

# from dataclasses import dataclass

from .helpers import get_path_from_name
from .constants import PATH_TEMPLATES
from ..core.console import output


class HandlerUI(UIModule):
    # def render(self, html=None, **kwargs):
    #     if html is None:
    #         html = self.__class__.__name__
    #     return self.render_string("ui/{}.html".format(html),
    #                               id=uuid.uuid4().__int__(), **kwargs)

    def render(self):
        self.id = str(uuid.uuid4()).replace("-", "")

        return self.render_string(
            get_path_from_name(
                name=self.__class__.title(), path=PATH_TEMPLATES / "ui"
            ).name,
            ui=self,
        )

    @classmethod
    def title(cls):
        return "_".join(cls.__name__.split("_")[1:]).lower()


# NOTE: SECTION -> DYNAMICALLY CREATED UIs
for path_template in [
    x
    for x in (PATH_TEMPLATES / "ui").iterdir()
    if (not x.is_dir() and re.search(r"\.html(?:\.j2)?", x.name, re.IGNORECASE))
]:
    # Dont worry about not including some, everything is controlled by the PH anyway
    # if not re.search("(old|new|_?test_?|_$)", f.stem, re.IGNORECASE):
    name_path_template = path_template.name.split(".")[0]

    # TODO: Add __admin for admin-only pages, add __auth for authenticated-only pages

    t = "".join(
        [
            a if a.isupper() else b
            for a, b in zip(name_path_template, name_path_template.title())
        ]
    )

    _class_handler = type(f"UI_{t}", (HandlerUI,), {})
    output(f"Created ui {_class_handler}")
    setattr(sys.modules[__name__], _class_handler.__name__, _class_handler)


class UI_Generic_Vue(HandlerUI):
    def render(self, model):
        self.model = model
        return super().render()

    @property
    def splitName(self) -> str:
        matches = re.findall("[A-Z][a-z]+", self.model.__name__)
        return "-".join([str(x).lower() for x in matches])
