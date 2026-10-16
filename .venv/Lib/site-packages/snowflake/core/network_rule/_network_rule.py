from typing import TYPE_CHECKING

from ._generated.api.network_rule_api_base import NetworkRuleCollectionBase, NetworkRuleResourceBase


if TYPE_CHECKING:
    from snowflake.core.schema import SchemaResource


class NetworkRuleCollection(NetworkRuleCollectionBase):
    """Represents the collection operations on the Snowflake network rule resource.

    With this collection, you can create, iterate through, and search for network rules that you have access to
    in the current context.
    """

    def __init__(self, schema: "SchemaResource"):
        super().__init__(schema, NetworkRuleResource)


class NetworkRuleResource(NetworkRuleResourceBase):
    """Represents a reference to a Snowflake network rule.

    With this network rule reference, you can fetch information about a network rule, as well as
    perform certain actions on it.
    """

    _plural_name = "network_rules"
