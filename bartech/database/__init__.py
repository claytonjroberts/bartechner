from .base import Base
from .binder import Binder, Relator, accessible
from .models import *
from .sources import Source
from .commands import commands


# XXX -> BIND THE MODELS
Binder(modelBase=Base).bind(option_verbose=True)
