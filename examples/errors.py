"""
This script is used to see actual error messages by the library.
We want error messages to be clear and to always point to the "correct" error cause.
The library should always raise one of our exceptions, bar any bug.
"""

from utils import connection_manager
import logging


logger = logging.getLogger(__name__)

if __name__ == "__main__":
    with connection_manager() as conn:
        print("==> Creating first cursor")
        cursor = conn.cursor()
        # This statement is invalid because we can't write 'as value' since value is a keyword
        statement = "SELECT 1 as value"
        print(f"==> Running a statement with a syntax error: '{statement}'")
        try:
            cursor.execute(statement)
        except Exception:
            logger.exception("==> Error: ", exc_info=True)
        print("==> Can you tell what the problem was?")
