from utils import connection_manager, setup_logging


if __name__ == "__main__":
    setup_logging()
    with connection_manager() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 as test_value_1, 2 as test_value_2, 3 as test_value_3")
        # We can use the cursor as an iterator:
        for row in cursor:
            print(row)
