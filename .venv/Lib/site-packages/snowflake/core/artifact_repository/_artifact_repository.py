from typing import TYPE_CHECKING

from ._generated.api.artifact_repository_api_base import (
    ArtifactRepositoryCollectionBase,
    ArtifactRepositoryResourceBase,
)


if TYPE_CHECKING:
    from snowflake.core.schema import SchemaResource


class ArtifactRepositoryCollection(ArtifactRepositoryCollectionBase):
    """Represents the collection operations on the Snowflake artifact repository resource.

    With this collection, you can create, iterate through, and search for artifact repository that you have access to
    in the current context.
    """

    def __init__(self, schema: "SchemaResource") -> None:
        super().__init__(schema, ArtifactRepositoryResource)


class ArtifactRepositoryResource(ArtifactRepositoryResourceBase):
    """Represents a reference to a Snowflake artifact repository.

    With this artifact repository reference, you can fetch information about an artifact repository,
    as well as perform certain actions on it.
    """

    _plural_name = "artifact_repositories"
