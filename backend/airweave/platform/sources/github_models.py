"""Native GitHub validation and bounded provider progress, not a second capture state owner."""

from typing import Annotated, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StrictBool,
    model_validator,
)

Sha = Annotated[str, Field(pattern=r"^[a-f0-9]{40}$")]
NativeId = Annotated[int, Field(gt=0, strict=True)]


class NativeModel(BaseModel):
    """Validate routing fields while retaining untouched responses separately."""

    model_config = ConfigDict(extra="ignore")


class Identity(NativeModel):
    """Native numeric object identity."""

    id: NativeId


class Repository(Identity):
    """Selected repository routing and authorization fields."""

    full_name: str = Field(pattern=r"^[a-zA-Z0-9_-]+/[a-zA-Z0-9_.-]+$")
    owner: Identity
    default_branch: str


class Issue(Identity):
    """Native issue routing; pull requests also appear in this endpoint."""

    number: int = Field(gt=0, strict=True)
    repository_url: str
    pull_request: dict[str, JsonValue] | None = None


class CommitSide(NativeModel):
    """Native immutable Git commit identity."""

    sha: Sha


class PullRequest(Identity):
    """Pinned diff comparison and completeness count."""

    number: int = Field(gt=0, strict=True)
    head: CommitSide
    base: CommitSide
    changed_files: int = Field(ge=0)


class TreeEntry(NativeModel):
    """Native nonrecursive Git tree entry."""

    path: str = Field(min_length=1, max_length=4096)
    mode: Literal["100644", "100755", "040000", "160000", "120000"]
    type: Literal["blob", "tree", "commit"]
    sha: Sha
    size: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def coherent_type(self) -> "TreeEntry":
        """Git mode determines whether this is a blob, subtree, or submodule reference."""
        expected = (
            "tree" if self.mode == "040000" else "commit" if self.mode == "160000" else "blob"
        )
        if self.type != expected:
            raise ValueError("GitHub tree entry type does not match its native mode")
        return self


class Tree(NativeModel):
    """Native Git tree response with explicit truncation signal."""

    sha: Sha
    tree: list[dict[str, JsonValue]]
    truncated: StrictBool


class GitCommit(NativeModel):
    """Native commit and its immutable root tree."""

    sha: Sha
    tree: CommitSide


class Branch(NativeModel):
    """Native selected branch and current commit."""

    name: str = Field(min_length=1, max_length=1024)
    commit: CommitSide


class RepositoryContext(BaseModel):
    """Source provenance needed to route child reads; never substitutes for native JSON."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    repository_id: NativeId
    owner_id: NativeId
    full_name: str = Field(pattern=r"^[a-zA-Z0-9_-]+/[a-zA-Z0-9_.-]+$")


class IssuePayload(BaseModel):
    """Unmodified issue and PR originals plus verified repository provenance."""

    model_config = ConfigDict(extra="forbid")
    issue: dict[str, JsonValue]
    repository: RepositoryContext
    pull_request_detail: dict[str, JsonValue] | None = None


class BranchPayload(BaseModel):
    """Unmodified branch original plus verified repository provenance."""

    model_config = ConfigDict(extra="forbid")
    branch: dict[str, JsonValue]
    repository: RepositoryContext


class RootProgress(BaseModel):
    """Position in the fixed authorized repository selection."""

    model_config = ConfigDict(extra="forbid")
    version: Literal[1] = 1
    repository_index: int = Field(default=0, ge=0)


class ListProgress(BaseModel):
    """Native page position and optional pinned PR diff identity."""

    model_config = ConfigDict(extra="forbid")
    version: Literal[1] = 1
    page: int = Field(default=1, ge=1)
    count: int = Field(default=0, ge=0)
    head_sha: Sha | None = None
    base_sha: Sha | None = None


class TreeFrame(BaseModel):
    """A DFS frame holds one path component, avoiding repeated full paths at every depth."""

    model_config = ConfigDict(extra="forbid")
    sha: Sha
    component: str = Field(max_length=4096)
    offset: int = Field(default=0, ge=0)


class FileProgress(BaseModel):
    """Pinned immutable tree plus bounded DFS position, validated again by ScanContinuation."""

    model_config = ConfigDict(extra="forbid")
    version: Literal[1] = 1
    ref: str | None = None
    commit_sha: Sha | None = None
    tree_sha: Sha | None = None
    stack: tuple[TreeFrame, ...] = ()
    initialized: StrictBool = False

    @model_validator(mode="after")
    def coherent_snapshot(self) -> "FileProgress":
        """Malformed progress must fail before provider I/O or absence reconciliation."""
        pinned = (self.ref, self.commit_sha, self.tree_sha)
        if self.initialized:
            if any(value is None for value in pinned):
                raise ValueError("Initialized GitHub code progress requires a complete snapshot")
            if self.stack and (self.stack[0].sha != self.tree_sha or self.stack[0].component):
                raise ValueError("GitHub DFS root does not match the pinned tree")
        elif any(value is not None for value in pinned) or self.stack:
            raise ValueError("Uninitialized GitHub code progress cannot contain a snapshot")
        return self


class Dates(NativeModel):
    """Optional upstream timestamps, never substituted with local observation time."""

    created_at: AwareDatetime | None = None
    updated_at: AwareDatetime | None = None
