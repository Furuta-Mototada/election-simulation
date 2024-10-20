import sqlite3


def create_connection(db_file):
    """Create a database connection to an SQLite database"""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connected to database: {db_file}")
    except sqlite3.Error as e:
        print(e)
    return conn


def execute_sql_script(conn, script_file):
    """Execute SQL script from a file"""
    try:
        with open(script_file, "r") as f:
            sql_script = f.read()
        conn.executescript(sql_script)
        print(f"Executed script: {script_file}")
    except Exception as e:
        print(f"Error executing script {script_file}: {e}")
