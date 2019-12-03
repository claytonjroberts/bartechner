from sqlalchemy import Column, Integer, String, Boolean, Float
from sqlalchemy_utils import EmailType, PhoneNumberType

from . import column_generators as col_gen
from .base import Base
from .binder import Relator

import numpy as np


class Drink(Base):
    name = Column(String, unique=True)


class DrinkNameAlt(Base):
    _defined = [Drink]

    name = Column(String, unique=True)


class Ingredient(Base):
    name = Column(String, unique=True)

    _related = [Relator(is_self=True, name_append="parent")]


class DrinkIngredient(Base):
    _defined = [Drink, Ingredient]
