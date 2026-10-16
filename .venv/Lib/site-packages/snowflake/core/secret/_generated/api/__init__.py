from __future__ import absolute_import

# flake8: noqa

# import apis into api package
from snowflake.core.secret._generated.api.secret_api import SecretApi

__all__ = [
    "SecretApi",
]
