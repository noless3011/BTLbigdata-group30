"""
Initialize Cassandra Schema for University Analytics
Run this script to create tables in Cassandra
"""

import os
import sys
import time

from cassandra.cluster import Cluster

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = "university_analytics"


def wait_for_cassandra(cluster, max_retries=30):
    """Wait for Cassandra to be ready"""
    print(f"Waiting for Cassandra at {CASSANDRA_HOST}:{CASSANDRA_PORT}...")

    for i in range(max_retries):
        try:
            session = cluster.connect()
            session.execute("SELECT now() FROM system.local")
            print("✓ Cassandra is ready")
            session.shutdown()
            return True
        except Exception as e:
            if i < max_retries - 1:
                print(f"  Attempt {i + 1}/{max_retries}: Waiting...")
                time.sleep(10)
            else:
                print(f"✗ Failed to connect to Cassandra: {e}")
                return False
    return False


def create_schema(session):
    """Create all tables in Cassandra"""

    print("\nCreating Cassandra schema...")

    # Read schema from CQL file
    schema_file = os.path.join(os.path.dirname(__file__), "cassandra_schema.cql")

    if os.path.exists(schema_file):
        print(f"Reading schema from {schema_file}")
        with open(schema_file, "r") as f:
            schema_content = f.read()

        # Split by semicolon and execute each statement
        statements = [s.strip() for s in schema_content.split(";") if s.strip()]

        for i, statement in enumerate(statements, 1):
            # Skip comments
            if statement.startswith("--") or not statement:
                continue

            try:
                session.execute(statement)
                # Only print table creation statements
                if "CREATE TABLE" in statement.upper():
                    table_name = (
                        statement.split("TABLE")[1].split("(")[0].strip().split()[-1]
                    )
                    print(f"  ✓ Created table: {table_name}")
                elif "CREATE KEYSPACE" in statement.upper():
                    print(f"  ✓ Created keyspace: {CASSANDRA_KEYSPACE}")
                elif "CREATE INDEX" in statement.upper():
                    print(f"  ✓ Created index")
            except Exception as e:
                # Ignore "already exists" errors
                if "already exists" not in str(e).lower():
                    print(f"  ✗ Error executing statement: {e}")
                    print(f"    Statement: {statement[:100]}...")
    else:
        print(f"✗ Schema file not found: {schema_file}")
        print("Creating tables manually...")
        create_tables_manually(session)


def create_tables_manually(session):
    """Manually create essential tables if schema file not found"""

    essential_tables = [
        """
        CREATE TABLE IF NOT EXISTS active_users (
            window_start timestamp,
            window_end timestamp,
            active_users int,
            source text,
            PRIMARY KEY (window_start, window_end)
        ) WITH CLUSTERING ORDER BY (window_end DESC)
        """,
        """
        CREATE TABLE IF NOT EXISTS course_overview (
            course_id text PRIMARY KEY,
            total_enrolled_students int,
            total_videos_watched int,
            total_materials_downloaded int,
            total_assignments_submitted int,
            total_quizzes_completed int,
            avg_assignment_score double,
            avg_quiz_score double,
            total_interactions int,
            last_updated timestamp
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS student_overview (
            student_id text PRIMARY KEY,
            total_courses_enrolled int,
            total_videos_watched int,
            total_assignments_submitted int,
            total_quizzes_completed int,
            total_logins int,
            total_materials_downloaded int,
            avg_assignment_score double,
            avg_quiz_score double,
            total_watch_time_minutes double,
            last_login timestamp,
            last_updated timestamp
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS system_summary (
            summary_id text PRIMARY KEY,
            current_active_users int,
            total_active_courses int,
            total_students int,
            total_courses int,
            batch_last_run timestamp,
            speed_layer_status text,
            last_updated timestamp
        )
        """,
    ]

    for table_sql in essential_tables:
        try:
            session.execute(table_sql)
            table_name = table_sql.split("TABLE")[1].split("(")[0].strip().split()[-1]
            print(f"  ✓ Created table: {table_name}")
        except Exception as e:
            print(f"  ✗ Error creating table: {e}")


def main():
    """Main function"""
    print("=" * 60)
    print("Cassandra Schema Initialization")
    print("=" * 60)

    try:
        # Connect to Cassandra
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)

        # Wait for Cassandra to be ready
        if not wait_for_cassandra(cluster, max_retries=30):
            print("\n✗ Failed to connect to Cassandra")
            sys.exit(1)

        # Create session
        session = cluster.connect()

        # Create keyspace
        print(f"\nCreating keyspace: {CASSANDRA_KEYSPACE}")
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
        print(f"✓ Keyspace created/verified")

        # Use keyspace
        session.set_keyspace(CASSANDRA_KEYSPACE)

        # Create schema
        create_schema(session)

        print("\n" + "=" * 60)
        print("✓ Schema initialization complete!")
        print("=" * 60)

        # Cleanup
        session.shutdown()
        cluster.shutdown()

    except Exception as e:
        print(f"\n✗ Error during schema initialization: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
