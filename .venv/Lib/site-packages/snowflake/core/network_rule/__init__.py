"""Manages Snowflake network rule."""

from ._generated.models import (
    NetworkRule,
)
from ._network_rule import NetworkRuleCollection, NetworkRuleResource


__all__ = [
    "NetworkRuleResource",
    "NetworkRuleCollection",
    "NetworkRule",
]
