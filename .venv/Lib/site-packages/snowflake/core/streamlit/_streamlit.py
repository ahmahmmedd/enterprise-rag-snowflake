from typing import TYPE_CHECKING

from ._generated.api.streamlit_api_base import StreamlitCollectionBase, StreamlitResourceBase


if TYPE_CHECKING:
    from snowflake.core.schema import SchemaResource


class StreamlitCollection(StreamlitCollectionBase):
    """Represents the collection operations on the Snowflake Streamlit resource."""

    def __init__(self, schema: "SchemaResource"):
        super().__init__(schema, StreamlitResource)


class StreamlitResource(StreamlitResourceBase):
    """Represents a reference to a Snowflake Streamlit."""

    _plural_name = "streamlits"
