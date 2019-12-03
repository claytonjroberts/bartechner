import enum
import inspect
import re
import types
from dataclasses import dataclass, field
from typing import Dict, List
import sqlalchemy as sa

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    Interval,
    Sequence,
    String,
    Table,
    Text,
    and_,
    or_,
)
from sqlalchemy.orm import backref, relationship
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.collections import attribute_mapped_collection, collection
from sqlalchemy.schema import ForeignKeyConstraint, Index, UniqueConstraint

from ..core.helpers import get_name_related
from ..core.console import output


def accessible(func):
    """Property that sets the _accessible attribute on functions so that Binder can add
    them to functions that can be used from the client side"""
    func._accessible = True
    return func


@dataclass
class Relator:
    """Class for defining relations.
    Supports target as subclass of Base, or the InstrumentedAttribute (column) of another class
    """

    _debug = False

    target: type = None

    name_alt: str = None
    name_back: str = None
    name_append: str = ""
    name_back_append: str = ""

    column_target: Column = None
    nullable: bool = True
    is_self: bool = False
    option_doBack: bool = True
    model: type = None

    # option_snake_case: bool = True # Whether or not names are

    _is_executed: bool = False

    # def relate(self):
    #     if self.is_self:
    #         pass

    def __repr__(self):
        return (
            f"<{self.__class__.__name__}({self.target}->{self.model}"
            + (f" as {self.name_alt}" if self.name_alt else "")
            + ")"
        )

    @property
    def name(self) -> str:
        return self.attributeName

        self.name

    @property
    def name_backActual(self) -> str:
        "Actual backref name used"
        if self.option_doBack:
            name_backLocal = (
                self.name_back
                if self.name_back
                else get_name_related(source=self._model_target, relative=self.model)
            )
            if self.name_back_append and len(self.name_back_append) > 0:
                name_backLocal += (
                    self.name_back_append[0].upper() + self.name_back_append[1:]
                )
        return name_backLocal

    @property
    def _column_target(self):
        if isinstance(self.target, InstrumentedAttribute):
            return self.target
        else:
            return self._model_target.id

    @property
    def _model_target(self):
        if inspect.isclass(self.target):
            return self.target
        else:
            # Target is
            return self.target.parent.class_

    def relate(self, primary_key=False) -> sa.Column:
        if self.is_self:
            self.target = self.model

        if isinstance(self.target, sa.Column):
            # Treat as column
            return self.relate_column(column=self.target)

        elif isinstance(self.target, type) and issubclass(self.target, enum.Enum):
            "Enum, provide the sqlalchemy type 'ENUM'"
            return self.relate_enum(self.target)

        else:
            # Assume model to relate to (treat as model-to-model relation)
            output(
                source=self,
                message=f"[{self.model.__name__}] {self.idAttributeName} REFERENCES {self.target}",
            )
            # col = Column(self.idAttributeName, ForeignKey(
            #     self.target.id), primary_key=primary_key)

            setattr(
                self.model,
                self.idAttributeName,
                Column(
                    self.idAttributeName,
                    self._column_target.type.__class__,
                    ForeignKey(self._column_target, ondelete="CASCADE"),
                    nullable=self.nullable,
                    index=True,  # Add an index on all primary keys for speed
                ),
            )

            setattr(
                self.model,
                self.attributeName,
                relationship(
                    self._model_target,
                    backref=(
                        backref(self.name_backActual, cascade="all")
                        if self.option_doBack
                        else None
                    ),
                    foreign_keys=getattr(self.model, self.idAttributeName),
                    primaryjoin=(
                        getattr(self.model, self.idAttributeName) == self._column_target
                    ),
                    remote_side=self._column_target,
                    cascade="save-update",
                    # lazy="dynamic",
                    # ^ WARN Can't use this ('dynamic' loaders cannot be used with many-to-one/one-to-one relationships and/or uselist=Fals)
                    # collection_class=SetLike,
                    # onupdate="CASCADE",
                ),
            )

        self._is_executed = True
        return getattr(self.model, self.idAttributeName)

    def relate_enum(self, enum):
        "Enum, provide the sqlalchemy type 'ENUM'"
        return self.relate_column(
            Column(get_name_related(source=self.model, relative=enum), Enum(enum))
        )

    def relate_column(self, column) -> sa.Column:
        output(source=self, message=f"[{self.model.__name__}] COLUMN {column}")
        setattr(self.model, column.name, column)
        return column

    @property
    def attributeName(self) -> str:
        return "_".join(
            [
                x
                for x in [
                    (
                        get_name_related(source=self.model, relative=self._model_target)
                        if not self.name_alt
                        else self.name_alt
                    ),
                    self.name_append,
                ]
                if (x and len(x))
            ]
        )

    @property
    def idAttributeName(self):
        return f"{self.attributeName}{self._column_target.name[0].upper()}{self._column_target.name[1:]}"


@dataclass
class Binder:
    modelBase: type
    children: List = field(default_factory=list)

    def bind(self, option_verbose: bool = False):
        """
        Primary executable method is at classmethod level

        Finds all models defined on Base, and constructs foreign key
        columns and relationships on each as per their defined parent classes.

        In order to have any sort of Unique Constraints,
        put the classes inside of the related list as a list.
        To make a primary key, put the classes inside the defined list.

        EX: To make a compound UniqueConstraint
        _related = [
            [model_A, model_B]
        ]

        EX: To make a single UniqueConstraint
        _related = [
            [model_A,]
        ]

        EX: To make un-unique relationships
        _related = [
            model_A,
            model_B
        ]

        EX: To make a primary key
        _defined = [
            model_A,
            model_B
        ]
        """

        if option_verbose:
            output(source=self, message="Executing bind...")

        for model in [
            _model
            for _model in self.modelBase._decl_class_registry.values()
            if hasattr(_model, "__table__")
        ]:
            model.__class_init_pre__()
            model.__class_init__()
            BinderSlave(modelBase=self.modelBase, model=model).main(
                option_verbose=option_verbose
            )
            # self.children.append(
            #
            # )
            #
            # cls(model=model)._main()

            # cls.addToRelated(model, model.__bases__)
            #
            # model.defined = cls.relator(model, model.defined, primary=True)
            # model.related = cls.relator(model, model.related)


@dataclass
class BinderSlave:
    """
    Creates generic methods tailored to each class, and also creates
    relationships, foreign keys, canidate primary keys, and indexes
    """

    _debug = False

    modelBase: type
    model: type
    accessibles: List[str] = field(default_factory=list)
    defined: List = field(default_factory=list)
    related: List = field(default_factory=list)
    indexes: List = field(default_factory=list)

    relators: List[Relator] = field(default_factory=list)

    @classmethod
    def primaryKeyColumnNames(cls, model) -> list:
        "This won't work because the columns have to be created beforehand"
        raise NotImplementedError(
            "This method won't work because the primary columns have to be created before creation of the model."
        )

        cls.addToRelated(model, model.__bases__)
        final = []
        for x in model._defined:
            for y in cls.decipher(model=model, x=x):
                if isinstance(y, Column):
                    final.append(y.name)
                elif isinstance(y, Relator):
                    final.append(y.attributeName)
            # output(model.__)
        return final

    def __post_init__(self):
        # Add Binder object to base
        self.model._binder = self
        if not issubclass(self.model, self.modelBase):
            raise Exception(
                f"Model {self.model !r} is not a subclass of base {self.modelBase !r}"
            )

    def addToRelated(self, model, bases):
        "Recursive function that adds superclass's defined attributes to the subclass"
        for base in bases:
            if base is self.modelBase:
                continue
            elif issubclass(base, self.modelBase):
                # output(source = self, message =
                #     "You're trying to subclass an existing table. Try again douche.",
                #     option_status=Status.CRITICAL,
                # )
                raise Exception(
                    f"You're trying to subclass an existing table ({base !r}), try again douche"
                )
            else:
                if hasattr(base, "defined"):
                    [
                        model._defined.append(x)
                        for x in base._defined
                        if x not in model._defined
                    ]
                if hasattr(base, "related"):
                    [
                        model._related.append(x)
                        for x in base._related
                        if x not in model._related
                    ]
                self.addToRelated(model, base.__bases__)

    def setAccessibles(self):
        "Add accessible flagged functions to a list, for API calls"
        for name, attr in (
            (name, getattr(self.model, name, None)) for name in dir(self.model)
        ):
            if callable(attr) and getattr(attr, "_accessible", False):
                self.accessibles.append(name)

    def allRelators(self) -> list:
        "WARNING: Use the relators attribute instead"
        # print([self.defined, self.related])
        raise NotImplementedError
        final = []

        def iterate(obj):
            if isinstance(obj, Relator):
                return [obj]
            elif isinstance(obj, list):
                finali = []
                [finali.extend(iterate(x)) for x in obj]
                return finali
            else:
                return []

        [final.extend(iterate(x)) for x in [self.model._defined, self.model._related]]

        return final

    def main(self, option_verbose: bool = False):
        if option_verbose:
            output(source=self, message=f"Binding {self.model} to {self.modelBase}")

        self.setAccessibles()

        if self.modelBase is None:
            raise Exception(f"No base model passed to {self}")

        self.addToRelated(self.model, self.model.__bases__)

        if option_verbose:
            output(
                source=self, message=f"{self.model} defined is {self.model._defined}"
            )
            output(
                source=self, message=f"{self.model} related is {self.model._related}"
            )

        # Execute relations
        self.defined = self.relator(self.model._defined, option_primary=True)
        self.related = self.relator(self.model._related)

        # WARNING: Don't delete the following -> This was working code
        # self.indexes = self.addConstraints(self.model._indexed)
        # [x.addToModel() for x in self.indexes]

        # Add indexes to dates:
        for col in self.model.__table__.columns:
            if isinstance(col.type, (DateTime, Date)):

                output(
                    source=self,
                    message=f"Setting index for {self.model.__table__} > {col} ({col.type})",
                    # option_status=Status.IMPORTANT,
                )
                # col.index = True
                # NOTE: sqlalchemy makes indexes like "ix_InvoiceItem_childId"
                self.model.__table__.append_constraint(
                    Index(f"ix_{self.model.__tablename__}_{col.name}", col)
                )

    def getRelationshipTo(self, other) -> List[str]:
        "Get list of column names that configures how the model is related to the other"
        assert issubclass(other, self.Base)

        final = []
        for item in self.relators:
            # print(item)
            if isinstance(item, Relator) and item.target is other:
                final.append(item.idAttributeName)

        return final

    def addConstraints(self, classList: list) -> list:
        final = []
        for x in classList:
            final.append(Constrainer(binder=self, targetList=x))

        return final

    def decipher(self, x) -> list:
        "Deciper the argument to determine how to relate to the model"

        model = self.model

        output(source=self, message=f"Deciphering {x !r} for model {self.model !r}")

        if isinstance(x, list):
            "Has multiple classes that are unique together"

            final = []
            [final.extend(z) for z in [self.decipher(y) for y in x]]
            return final

        elif isinstance(x, types.FunctionType):
            col = x(model)
            col.name = x.__name__
            assert isinstance(col, Column)
            "Run through decipher again as column"
            return self.decipher(col)

        elif isinstance(x, type) and issubclass(x, enum.Enum):
            "Enum, provide the sqlalchemy type 'ENUM'"
            return self.decipher(
                Column(get_name_related(source=model, relative=x), Enum(x))
            )

        elif isinstance(x, Column):
            "EXIT"
            output(source=self, message="[{}] COLUMN {}".format(model.__name__, x))
            # setattr(model, x.name, x)
            return [Relator(target=x, model=self.model)]

        elif isinstance(x, Relator):
            "EXIT"
            x.model = model
            return [x]

        elif isinstance(x, str):
            # Treat as basic lookup
            return self.decipher(getattr(self.model, x))
            # assert isinstance(att, InstrumentedAttribute)
            # return [att]

        elif (isinstance(x, type) and issubclass(x, self.modelBase)) or isinstance(
            x, InstrumentedAttribute
        ):
            "Run through decipher again as Relator"
            return self.decipher(Relator(model=model, target=x))

        else:
            # output(source = self, message = x)
            # output(source = self, message = x, type(x), x())
            raise Exception(f"Argument {x !r} ({type(x) !r}) Not supported type")

    def relator(self, classList: list, option_primary: bool = False) -> list:
        rtn = []
        for item in classList:
            list_deciphered = []
            for item_deciphered in self.decipher(item):
                if isinstance(item_deciphered, Relator):
                    self.relators.append(item_deciphered)
                    list_deciphered.append(item_deciphered.relate())
                elif isinstance(item_deciphered, (Column, InstrumentedAttribute)):
                    raise Exception(
                        f"Tried to pass in {item_deciphered !r} of type {type(item_deciphered) !r}"
                    )
                    list_deciphered.append(item_deciphered)
                else:
                    raise Exception(f"Unable to relate {item !r}")
                # elif isinstance(d, str):
                #     att = getattr(self.model, d)
                #     assert(isinstance(att, Column))
                #     list_deciphered.append(att)

                rtn.extend(list_deciphered)

            if len(list_deciphered) > 1:
                self.column_make_unique(*list_deciphered)

        # WARNING: keep 'and' here, otherwise it blows up
        # NOTE: To automatically add a unique constraint on single item lists or
        # singluar defined columns, make 'len(rtn) >= 1'
        if option_primary and (len(rtn) >= 1):
            self.column_make_unique(*rtn)

        return rtn

    def column_make_unique(self, *columns: List[Column], whereClause=None):
        return self.model.__table__.append_constraint(UniqueConstraint(*columns))
