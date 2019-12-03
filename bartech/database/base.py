import collections
import csv
import datetime
import enum
import itertools
import json
import math
import re
import string
import time
import typing
from collections import ChainMap
from dataclasses import dataclass, field
from pathlib import Path
from pprint import pprint
from threading import Lock
from typing import Dict, List

import pandas as pd
import sqlalchemy as sa
import sqlalchemy.exc
import sqlalchemy.orm.exc
import sqlalchemy_views
from fuzzywuzzy import fuzz, process
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    Interval,
    PrimaryKeyConstraint,
    Sequence,
    String,
    Table,
    Text,
    UniqueConstraint,
    and_,
    or_,
)
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import aliased, load_only, object_session, relationship, validates
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.sql import func, select, text
from sqlalchemy.sql.expression import case, cast

from creature.core.console import output
from creature.core.locks import SLock

from ..core.helpers import get_name_related
from .binder import Binder, Relator, accessible
from .sources import Source


class Model:
    """
    Philosphy: though some things do not need an id for identification,
    for generisism, all things must have an id
    """

    _data = []
    _defined = []
    _related = []
    _indexed = []
    # _caches = {}

    @classmethod
    def __class_init_pre__(cls):
        """Initialize the class variables that are objects or instances and are generic
        WARNING: Variables found here should not override objects that are unique to the class
        USE THIS METHOD VERY CAREFULLY"""
        # Method called in binder
        cls._lock_get_or_create = Lock()
        cls._sLock_get_or_create = SLock()
        cls._lock_bounty = Lock()
        # ^ This lock is not necessary because get or create is always called
        # in the bounty, so therefore, worst case, the object would be updated
        # with the same values
        cls._caches = dict()

    @classmethod
    def __class_init__(cls):
        "Method for abstraction, is called after pre and can be defined for specific classes"

    @declared_attr
    def __tablename__(cls):
        return cls.__name__

    @declared_attr
    def seq(cls):
        return Sequence("SEQUENCE_ID", metadata=cls.metadata)

    @declared_attr
    def id(cls):
        return Column(
            Integer,
            # cls.seq,
            # unique=True,
            primary_key=True,
            autoincrement=True,
            # server_default=cls.seq.next_value(),
            comment="Generic id that provides a blind join with any table. Relationships are always defined by relating this key.",
        )

    @classmethod
    def getFilterTerms(cls) -> List[str]:
        # NOTE: This is a very fast function, and probably does not need ot be cached (7.07 µs ± 50 ns per loop)
        # try:
        #     return cls._getFilterTerms
        # except AttributeError:
        #     cls._getFilterTerms = [
        #         y.name
        #         for y in list(
        #             itertools.chain(  # Merge items into 1 list
        #                 *[
        #                     # Filter Constraints
        #                     list(x.columns)
        #                     for x in cls.__table__.constraints
        #                     if isinstance(x, (PrimaryKeyConstraint, UniqueConstraint))
        #                 ]
        #             )
        #         )
        #     ]
        #     return cls._getFilterTerms
        return [
            y.name
            for y in list(
                itertools.chain(  # Merge items into 1 list
                    *[
                        # Filter Constraints
                        list(x.columns)
                        for x in cls.__table__.constraints
                        if isinstance(x, (PrimaryKeyConstraint, UniqueConstraint))
                    ]
                )
            )
        ]

    @classmethod
    def get_or_create(
        cls,
        requestor: Source,
        filters: dict,
        updates: dict = {},
        # manipulate: bool = False
        option_troubleShooting: bool = False,
    ):
        "Thread safe, more intutive get or create, to replace the get_or_create function"
        # As much stuff needs to be out of the lock to save time for multithreading
        query = requestor.query(cls).filter_by(**filters)
        # hsh = hash(tuple(sorted(filters.items())))
        hsh = tuple(sorted(filters.items()))
        with cls._sLock_get_or_create[hsh]:
            try:
                instance = query.one()
                instance = requestor.merge(instance)
            except NoResultFound:
                # Make new item
                try:
                    instance = cls(**filters)
                    # .options(FromCache("default"))
                    # TODO ^ I need to need to allow caching

                    # WARNING, attaching to multiple sessions causes errors (line below should work)
                    requestor.add(instance)
                    requestor.commit()
                except sqlalchemy.exc.IntegrityError:
                    requestor.rollback()
                    if option_troubleShooting:
                        raise
                    else:
                        cls.output(
                            f"Assuming that separate process is trying to commit object with filters {filters}",
                            option_status="IMPORTANT",
                        )
                        # This means that a separate process is trying to do the same thing
                        # SOLUTION -> retry the method
                        return cls.get_or_create_magic(
                            requestor=requestor,
                            filters=filters,
                            updates=updates,
                            option_troubleShooting=True,
                        )

            except MultipleResultsFound:
                cls.output(
                    f"Multiple found for filters:{filters}", option_status="CRITICAL"
                )
                raise

            # MAYBE: Dont need to be inside the "with" clause, as that is just for making new objects
            # Updates:

        for key, val in updates.items():
            if callable(val):
                # If a function is passed, assume that it is to change the
                # instance's internal variables based on its state
                setattr(instance, key, val(instance))
            else:
                setattr(instance, key, val)

        requestor.flush()
        # Autoflush should already be on
        # session.refresh(instance)
        # WARNING: Dont commit because this causes loss of control and errors on other proccesses
        # ^ I dont think that is true, and probably should be here.
        requestor.commit()
        requestor.merge(instance)
        # ^ Always merge the instance to the requestor, even though it may be redundant
        return instance

    @classmethod
    def createAll(cls, session) -> list:
        final = []
        output(message="CREATING", source=cls)
        for row in cls._data:
            rowDict = {
                cls._data_header[i]: row[i]
                for i in range(min(len(row), len(cls._data_header)))
                if " " not in cls._data_header[i]
            }

            c = cls(**rowDict)

            def getDataHeaderAttribute(header, val) -> (str, Base):
                if " " in header:
                    terms = header.split(" ")
                    try:
                        assert len(terms) == 2
                    except AssertionError:
                        print(terms)
                        raise

                    try:
                        """Try to get the class by the term
                        (has to have uppercase first letter)"""
                        model = cls._decl_class_registry[terms[0]]
                        name = get_name_related(source=cls, relative=model)
                    except KeyError:
                        """Assume that the term refers to a Relator object"""
                        model = None
                        all = []
                        for r in cls._metallurgy.allRelators():
                            all.append(r)
                            if r.nameAlt == terms[0]:
                                model = r.x
                        if not model:
                            print(all)
                            # raise Exception("Could not find model for {} on row {}".format(
                            #     terms[0]), row)
                            raise
                        name = terms[0]

                    query = session.query(model).filter(
                        getattr(model, terms[1]) == row[i]
                    )
                    try:
                        return (name, query.one())
                    except sqlalchemy.orm.exc.NoResultFound:
                        print(query.statement)
                        # print(row)
                        print(terms[1])
                        raise

            for i in range(min(len(row), len(cls._data_header))):
                if isinstance(cls._data_header[i], list):
                    "Composite (multi-component) attribute"
                    # TODO
                    raise

                    # setattr(c, name, query.one())
                if " " in cls._data_header[i]:
                    setattr(c, *getDataHeaderAttribute(cls._data_header[i], row[i]))

                # if " " in cls._data_header[i] and False:
                #     terms = cls._data_header[i].split(" ")
                #     try:
                #         assert(len(terms) == 2)
                #     except AssertionError:
                #         print(terms)
                #         raise
                #
                #     try:
                #         """Try to get the class by the term
                #         (has to have uppercase first letter)"""
                #         model = cls._decl_class_registry[terms[0]]
                #         name = cls.get_name_related(model)
                #     except KeyError:
                #         """Assume that the term refers to a Relator object"""
                #         model = None
                #         all = []
                #         for r in cls._metallurgy.allRelators():
                #             all.append(r)
                #             if r.nameAlt == terms[0]:
                #                 model = r.x
                #         if not model:
                #             print(all)
                #             raise Exception("Could not find model for {} on row {}".format(
                #                 terms[0]), row)
                #         name = terms[0]
                #
                #     query = session.query(model).filter(
                #         getattr(model, terms[1]) == row[i]
                #     )
                #     try:
                #         setattr(c, name, query.one())
                #     except sqlalchemy.orm.exc.NoResultFound:
                #         print(query.statement)
                #         print(row)
                #         print(terms[1])
                #         raise
                #

            try:
                session.add(c)
            except sqlalchemy.exc.IntegrityError:
                # The row has already been added
                output(message=f"ERROR with row '{row}', continuing...", source=cls)
            else:
                final.append(c)

        session.flush()
        session.commit()

        return final

    @classmethod
    def fetch(cls, session):
        "Refreshes this table from sources or processes defined in the class"
        raise NotImplementedError()

    @classmethod
    def is_parent_to(cls, other) -> bool:
        raise
        clsTerms = list(re.findall("[A-Z]+[a-z]+", cls.__tablename__))
        otherTerms = list(re.findall("[A-Z]+[a-z]+", other.__tablename__))

        # print("{}.get_name_related({})".format(
        #     cls.__tablename__, other.__tablename__))

        if len(clsTerms) > len(otherTerms):
            "If the class name of the "
            return False

        def iterate():
            i = -1
            while True:
                i += 1
                if i > min([len(clsTerms), len(otherTerms)]) - 1:
                    "i is bigger than one list"

                    if i > (len(clsTerms) - 1):
                        return "".join(otherTerms[i:])
                    elif i > (len(otherTerms) - 1):
                        return "".join(clsTerms[:i])  # This may be wrong

                else:
                    if clsTerms[i] == otherTerms[i]:
                        continue
                    else:
                        return "".join(otherTerms[i:])
                        # if clsTerms[i] == otherTerms[i]:
                        #     continue

        return iterate()

    @classmethod
    def getCacheNames(cls, caveat=None, requestor: Source = None) -> dict:
        if not requestor:
            requestor = Source.MAIN.r()

        "Return dictionary cache of names for all records"
        final = {}
        q = requestor.query(cls)
        if caveat:
            q = q.filter(caveat)
        for item in q.all():
            # print(item)
            for name in item.names:
                final[name] = item.id
        return final

    @classmethod
    def matchFuzzy(cls, key: str, caveat=None, requestor: Source = None) -> int:
        "Optimized fuzzy site finder, auto-caches data in a class variable, returns int Id for speed"
        # TODO: Implement caveat
        if not isinstance(key, str) or key == "":
            return None

        if not requestor:
            requestor = Source.MAIN.r()

        cacheName = "names" if caveat is None else str(caveat)

        try:
            if cls._caches[cacheName].hasExpired:
                raise KeyError
                # NOTE: Raising a KeyError is not the best thing to do,
                # but techically the key shouldn't exist, so it is okay
        except AttributeError:
            # Class does not have _caches attribute, declare variable and retry
            cls._caches = {}
            # RECURSIVE -->
            return cls.matchFuzzy(key, caveat=caveat, requestor=requestor)
        except KeyError:
            cls._caches[cacheName] = Cache(
                data=cls.getCacheNames(caveat=caveat, requestor=requestor)
            )

        key = key.upper()

        try:
            # Try to just get by key name
            return cls._caches[cacheName][key]
        except KeyError:
            pass

        # Continue to fuzzy matching
        # print(cls._caches[cacheName], cacheName)
        #
        # print()
        # print(cls.getCacheNames(, caveat=caveat))
        r = process.extractOne(key, list(cls._caches[cacheName].keys()))
        if r[1] < cls._matchFuzzyThreshold:
            cls.output(
                f"[FUZZY] Bad matching: {key}, best: {r[0]}, score: {r[1]}",
                option_line_clear=True,
                option_status="CAUTION",
            )
            # raise Exception_BadFuzzyMatch()
            return cls._caches[cacheName].learnAndReturn(key, None)
        else:
            return cls._caches[cacheName].learnAndReturn(
                key, cls._caches[cacheName].get(r[0])
            )

    def __repr__(self):
        "Returns string representation with all primary key values"
        try:
            _internal = self.__repr_internal__()
        except AttributeError:
            additionalAttrs = ["id", "index"]
            attrs = {
                **{y: getattr(self, y) for y in additionalAttrs if hasattr(self, y)},
                **{
                    x.name: getattr(self, x.name)
                    for x in self.__class__._binder.defined
                },
            }
            # for x in self.__class__._defined:
            #     print(x)
            # if not self.__class__._metallurgy:
            # return "<{}({})>".format(
            #     self.__class__.__name__,
            #     ", ".join(
            #         [
            #             "{}='{}'".format(col, self.__getattribute__(col))
            #             for col in [
            #                 key.name for key in self.__class__.__table__.primary_key
            #             ]
            #         ]
            #     ),
            # )
            # # else:
            _internal = ", ".join(
                [(f"{k}={attrs[k] !r}" if k != "id" else f"#{attrs[k]}") for k in attrs]
            )
        return f"<{self.__class__.__name__}({_internal})>"

    def __gt__(self, other):
        try:
            return self.index > other.index
        except AttributeError:
            pass

        try:
            return self.abbreviation > other.abbreviation
        except AttributeError:
            pass

        try:
            return self.name > other.name
        except AttributeError:
            pass

        return self.id > other.id

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.id == other.id

    @property
    def names(self) -> list:
        final = []
        for propname in ["name", "abbreviation", "altNames"]:
            if hasattr(self, propname):
                if isinstance(getattr(self, propname), list):
                    final += [x.name for x in getattr(self, propname)]
                else:
                    final.append(getattr(self, propname))
        return final

    @property
    def sess(self):
        return object_session(self)

    def autoAssign(self, paramDict):
        # argList = inspect.getfullargspec(self.__init__).args
        # argList.remove('self')
        for col in self.__table__.columns.keys():
            try:
                self.__setattr__(col, paramDict[col])
            except KeyError as ke:
                if (
                    self.__table__.columns[col].nullable
                    or self.__table__.columns[col].default
                    or self.__table__.columns[col].server_default
                    or self.__table__.columns[col].autoincrement
                ):
                    "This is okay, just means there isn't data for this one or there is a default"
                    pass
                else:
                    raise ke

    def toDict(self):
        return Datafy(self).main()

    def datafy(self):
        return Datafy(self).main()

    @property
    def base(self) -> type:
        return object_session(self).Base


class Indexed:
    @declared_attr
    def index(cls):
        return Column(
            Integer,
            unique=True,
            comment="Unique integer index, usually to define order",
        )

    @classmethod
    def getByIndex(cls, session, index: int):
        return session.query(cls).filter_by(index=index).one()


class TimeStamped:
    @declared_attr
    def timeCreated(cls):
        return Column(DateTime, server_default=func.now())

    # @declared_attr
    # def timeUpdated(cls):
    #     return Column(DateTime, server_default=func.now(), onupdate=func.now())


Base = declarative_base(cls=Model)
