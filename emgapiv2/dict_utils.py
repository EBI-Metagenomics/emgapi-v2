from typing import Any

from pydantic.fields import PydanticUndefined as Unset


def some(dictionary: dict, keys: set, default: Any = Unset):
    """
    Return a partial dict, including only some of the keys in the original.
    Optionally absent keys are replaced with the default value.
    :param dictionary: Dictionary, usually a superset of the desired ones.
    :param keys: Set of keys to include.
    :param default: Value to use for absent keys.
    :return: Partial dictionary containing exactly the desired keys.
    """
    return {
        k: dictionary.get(k, default)
        for k in keys
        if k in dictionary or default is not Unset
    }


def add(dictionary1: dict, dictionary2: dict):
    """
    Like dict .update(), except returning a new dict of the joined dicts.
    :param dictionary1: First dictionary to be added to
    :param dictionary2: Second dictionary, overrides keys in dictionary1 if clashing.
    :return: Added dictionary.
    """
    return {**dictionary1, **dictionary2}
