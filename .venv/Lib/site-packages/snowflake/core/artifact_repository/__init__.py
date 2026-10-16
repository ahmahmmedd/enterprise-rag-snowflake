"""Manages Snowflake artifact repository.

Example:
    >>> new_artifact_repository = ArtifactRepository(
    ...     name="my_artifact_repo", type="PIP", api_integration="my_api_integration"
    ... )
    >>> artifact_repositories = root.databases["MYDB"].schemas["MYSCHEMA"].artifact_repositories
    >>> my_artifact_repo = artifact_repositories.create(new_artifact_repository)
    >>> my_artifact_repo_snapshot = my_artifact_repo.fetch()
    >>> ar_data = artifact_repositories.iter(like="%my")
    >>> an_existing_repo = artifact_repositories["an_existing_repo"]
    >>> an_existing_repo.drop()

Refer to :class:`snowflake.core.Root` to create the ``root``.
"""

from ._artifact_repository import ArtifactRepositoryCollection, ArtifactRepositoryResource
from ._generated.models.artifact_repository import ArtifactRepository


__all__ = ["ArtifactRepository", "ArtifactRepositoryCollection", "ArtifactRepositoryResource"]
