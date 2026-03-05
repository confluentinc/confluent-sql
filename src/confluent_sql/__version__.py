"""Version information for the confluent_sql package."""

from importlib.metadata import PackageNotFoundError, version

try:
    # Get version from installed package metadata (pyproject.toml)
    VERSION = version("confluent-sql")
except PackageNotFoundError:
    # Fallback for development/editable installs where package isn't found
    VERSION = "0.0.0+dev"
