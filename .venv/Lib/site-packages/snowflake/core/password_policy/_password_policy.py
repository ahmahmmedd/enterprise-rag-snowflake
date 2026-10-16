from typing import TYPE_CHECKING

from ._generated.api.password_policy_api_base import PasswordPolicyCollectionBase, PasswordPolicyResourceBase


if TYPE_CHECKING:
    from snowflake.core.schema import SchemaResource


class PasswordPolicyCollection(PasswordPolicyCollectionBase):
    """Represents the collection operations on the Snowflake password policy resource.

    With this collection, you can create, iterate through, and search for password policies that you have access to
    in the current context.
    """

    def __init__(self, schema: "SchemaResource"):
        super().__init__(schema, PasswordPolicyResource)


class PasswordPolicyResource(PasswordPolicyResourceBase):
    """Represents a reference to a Snowflake password policy.

    With this password policy reference, you can fetch information about a password policy, as well as
    perform certain actions on it.
    """

    _plural_name = "password_policies"
