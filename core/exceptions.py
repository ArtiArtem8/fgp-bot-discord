"""Custom exceptions for the application."""


class EnvVarError(Exception):
    """Exception raised when a required environment variable is missing or invalid."""

    def __init__(self, var_name: str) -> None:
        """Initialize with the name of the missing or invalid environment variable.

        :param var_name: The name of the environment variable that is missing or invalid
        :type var_name: str:
        """
        super().__init__(f"Required environment variable '{var_name}' is not set.")
        self.var_name = var_name
