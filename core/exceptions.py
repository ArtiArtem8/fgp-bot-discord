"""Custom exceptions for the application."""


class EnvVarError(Exception):
    """Exception raised when a required environment variable is missing or invalid."""

    def __init__(self, var_name: str) -> None:
        """Initialize with the name of the missing or invalid environment variable.

        :param var_name: The name of the environment variable that is missing or invalid
        :type var_name: str:
        """
        super().__init__(f"Missing required environment variable '{var_name}'.")
        self.var_name = var_name


class APIError(Exception):
    """Exception raised for errors in API."""


class BotNotInitializedError(RuntimeError):
    """Raised when bot resources are accessed before initialization."""

    def __init__(self, resource_name: str) -> None:
        """Initialize error with resource name.

        :param resource_name: Name of the resource that wasn't initialized.
        :type resource_name: str
        """
        super().__init__(
            f"{resource_name} accessed before initialization. "
            f"Ensure setup_hook has completed successfully.",
        )
