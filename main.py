from db_manager import create_connection, execute_sql_script
from database.seed import seed_database


def main():
    db_file = "election.db"

    conn = create_connection(db_file)

    if conn:
        execute_sql_script(conn, "database/schema.sql")
        print("Successfully initialized database schema!")
        seed_database(conn)
        conn.close()


if __name__ == "__main__":
    main()
