from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from ._generated.api.tag_api_base import TagCollectionBase, TagResourceBase


if TYPE_CHECKING:
    from snowflake.core.schema import SchemaResource


class TagCollection(TagCollectionBase):
    """Represents the collection operations on the Snowflake tag resource.

    With this collection, you can create, iterate through, and search for tags that you have access to
    in the current context.
    """

    def __init__(self, schema: "SchemaResource"):
        super().__init__(schema, TagResource)


class TagResource(TagResourceBase):
    """Represents a reference to a Snowflake tag.

    With this tag reference, you can fetch information about a tag, as well as
    perform certain actions on it.
    """

    _plural_name = "tags"


@dataclass
class TagValue:
    """Represents a value of a Snowflake tag.

    With this object, you can set tags on resources. It is also used when fetching tags.

    Parameters
    __________
    value: str
    level: bool, optional. Read-only; used only when fetching tags and ignored when provided in set-tags call.
    """

    value: str
    level: Optional[str] = None
