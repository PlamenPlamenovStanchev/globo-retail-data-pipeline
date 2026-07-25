"""Project-specific exceptions for pipeline quality gates."""


class OutputValidationError(Exception):
    """Raised when transformed output is not approved for processed-zone persistence."""
