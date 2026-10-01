"""Native Slack application error codes shared by message and file acquisition."""


class SlackApiError(ValueError):
    """A native application failure cannot imply a successful empty page."""

    def __init__(self, code: str):
        """Expose the bounded provider code to explicit access interpretation."""
        self.code = code
        super().__init__(f"Slack request failed: {code}")
