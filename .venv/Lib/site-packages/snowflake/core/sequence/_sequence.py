import typing

from ._generated.api.sequence_api_base import SequenceCollectionBase, SequenceResourceBase


if typing.TYPE_CHECKING:
    from snowflake.core.schema import SchemaResource


class SequenceCollection(SequenceCollectionBase):
    """Represents the collection operations on the Snowflake sequence resource.

    With this collection, you can create, iterate through, and search for sequences that you have access to
    in the current context.
    """

    def __init__(self, schema: "SchemaResource"):
        super().__init__(schema, SequenceResource)


class SequenceResource(SequenceResourceBase):
    """Represents a reference to a Snowflake sequence.

    With this sequence reference, you can fetch information about a sequence, as well as
    perform certain actions on it.
    """

    _plural_name = "sequences"
