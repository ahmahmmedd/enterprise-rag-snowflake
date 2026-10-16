"""Manages Snowflake tag."""

from ._generated.models import (
    Tag,
)
from ._tag import TagCollection, TagResource, TagValue


__all__ = [
    "TagResource",
    "TagCollection",
    "TagValue",
    "Tag",
]
