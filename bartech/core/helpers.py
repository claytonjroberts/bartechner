"Static methods that may be used throughout the codebase"
import re
import typing
from .constants import REGEX_NAME_TERMS


def convertToBoolean(x):
    if not isinstance(x, str):
        return None

    xu = x.upper()
    if xu in ["Y", "YES", "T", "TRUE"]:
        return True

    if xu in ["N", "NO", "F", "FALSE"]:
        return False

    return None


def convertToNotNone(x):
    return x if x is not None else 0


def reduceSmallToZero(num: float):
    if num is None:
        return None
    else:
        return 0 if abs(num) <= 1e-5 else num


def rowStandardize(row):
    "Standardizes row to be able to be used via dict methods"
    if isinstance(row, dict):
        return row
    else:
        return dict(zip(row.keys(), row))


def get_name_terms(obj) -> typing.List[str]:
    if isinstance(obj, str):
        _name = obj
    elif isinstance(obj, type):
        _name = obj.__name__
    else:
        raise AssertionError(f"{obj !r} is not a string or class")

    return list(re.findall(REGEX_NAME_TERMS, str(_name)))


def get_name_related(source, relative, option_snake_case=True) -> str:
    """
    How is source related to relative?
    XxYy.func(Xx) -> Xx
    Xx.func(XxYy) -> Yy
    XxZz.func(XxYy) -> XxYy
    XxYyZz.func(XxAa) -> XxAa

    User.get_name_related(UserName) -> name
    UserName.get_name_related(User) -> user
    UserName.get_name_related(UserAuthorization) -> UserAuthorization
    UserName.get_name_related(Ability) -> ability
    Ability.get_name_related(UserName) -> userName
    Race.get_name_related(RaceSpecification) -> specification
    Race.get_name_related(RaceBase) -> base

    Item.get_name_related(ItemDifficulty) -> difficulty
    ItemDifficulty.get_name_related(Item) -> item
    ItemSomething.get_name_related(ItemDifficulty) -> itemDifficulty
    ItemDifficulty.get_name_related(AbilityBranch) -> abilityBranch
    """

    terms_source, terms_relative = (
        get_name_terms(x.__tablename__ if hasattr(x, "__tablename__") else x.__name__)
        for x in [source, relative]
    )

    if (
        len(terms_source) < len(terms_relative)
        and terms_source == terms_relative[: len(terms_source)]
    ):
        "Item : ItemSomething -> Something"
        final = terms_relative[len(terms_source) :]
    elif (
        len(terms_source) > len(terms_relative)
        and terms_relative == terms_source[: len(terms_relative)]
    ):
        # I feel like I need this case, but maybe not.
        "ItemSomething : Item -> Item"
        final = terms_relative
    else:
        "ItemSomething : ItemDifferent -> ItemDifferent"
        final = terms_relative

    try:
        return f"{final[0].lower()}{''.join(final[1:]) if len(final) > 1 else ''}"
    except:
        raise Exception(f"Error with ({terms_source})->({terms_relative}) = {final}")
