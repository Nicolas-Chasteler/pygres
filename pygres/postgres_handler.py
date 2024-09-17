#postgres_handler.py
import os
import psycopg2
import hashlib

class PostgresHandler:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(PostgresHandler, cls).__new__(cls)
            cls._initialize(cls._instance)
        return cls._instance

    @classmethod
    def _initialize(cls, instance):
        # Get connection info
        dbname = os.getenv("PG_NAME", "postgres")
        user = os.getenv("PG_USER", "postgres")
        password = os.getenv("PG_PASSWORD", "postgres")
        host = os.getenv("PG_HOST", "localhost")
        port = os.getenv("PG_PORT", 5432)

        # Connect to PG
        instance.dsn = f"dbname={dbname} user={user} password={password} host={host} port={port}"
        instance.conn = psycopg2.connect(instance.dsn)
        instance.cursor = instance.conn.cursor()

        # Create pg_scripts if not exists
        instance._check_pg_script()

        pg_script_directory = os.getenv("PG_SCRIPT_DIRECTORY")

        if pg_script_directory:
            run_scripts(pg_script_directory)

    # Execute arbitrary sql file and save record to pg_scripts
    def execute_sql_file(self, sql_file_path, skip_hash_check=False):
        # Open sql file
        with open(sql_file_path, 'r') as file:
            sql_content = file.read()

        hash_func = hashlib.new('md5')
        hash_func.update(sql_content)
        file_hash = hash_func.hexdigest()

        # Check the hash of the file against the pg_scripts table
        if not skip_hash_check:

            # Pull hash value of run file
            hash_check_query = """
            SELECT hash
            FROM pg_scripts
            WHERE file_name = %s
            );
            """
            self.cursor.execute(check_query, (file_name,))
            executed_hash = self.cursor.fetchone()

            # Throw error if previously run file hash isn't equal to new file hash
            if hash and hash[0] != file_hash_value:
                raise HashMismatchError(executed_hash, file_hash)


        # Execute the SQL file
        self.cursor.execute(sql_content)
        self.conn.commit()

        # Prepare to insert record into pg_scripts
        file_name = os.path.basename(sql_file_path)
        script_id = int(file_name.split("__")[0])  # Extract the numeric ID

        insert_query = """
            INSERT INTO pg_scripts (id, file_name, hash) VALUES (%s, %s, %s);
        """
        # Insert record of execution into pg_scripts
        self.cursor.execute(insert_query, (script_id, file_name, file_hash))
        self.conn.commit()  # Commit the insertion

    def run_scripts(self, folder_path):
        # Get all pg_scripts
        sql_files = sorted([f for f in os.listdir(folder_path) if f.endswith(".sql")])

        for file_name in sql_files:
            file_path = os.path.join(folder_path, file_name)
            self.execute_sql_file(file_path)

    # Check if pg_scripts table exists, if not create
    def _check_pg_script(self):
        # Checks if pg_scripts exists
        check_query = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = 'pg_scripts'
        );
        """
        self.cursor.execute(check_query)
        exists = self.cursor.fetchone()[0]

        if not exists:
            self.execute_sql_file(os.path.join(os.path.dirname(__file__), "pg_scripts", "000__Create_pg_scripts.sql"), skip_hash_check=True)

    def close(self):
        self.cursor.close()
        self.conn.close()
        super().close()

    def get_cursor(self):
        return self.cursor
