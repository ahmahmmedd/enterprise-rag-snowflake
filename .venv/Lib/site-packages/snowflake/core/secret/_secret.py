from typing import TYPE_CHECKING

from ._generated.api.secret_api_base import SecretCollectionBase, SecretResourceBase


if TYPE_CHECKING:
    from snowflake.core.schema import SchemaResource


class SecretCollection(SecretCollectionBase):
    """Represents the collection operations on the Snowflake secret resource.

    With this collection, you can create, iterate through, and search for secrets that you have access to
    in the current context.
    """

    def __init__(self, schema: "SchemaResource"):
        super().__init__(schema, SecretResource)


class SecretResource(SecretResourceBase):
    """Represents a reference to a Snowflake secret.

    With this secret reference, you can fetch information about a secret, as well as perform
    certain actions on it.
    """

    _plural_name = "secrets"
