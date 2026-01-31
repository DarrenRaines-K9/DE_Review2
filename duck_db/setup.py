import duckdb
from logger import logger


def setup_duckdb():
    """Create DuckDB database with schema, table, and sample data"""

    # Connect to DuckDB (creates database.duckdb file)
    conn = duckdb.connect('duck_db/database.duckdb')
    logger.info("Connected to DuckDB at duck_db/database.duckdb")

    # Create schema
    conn.execute("CREATE SCHEMA IF NOT EXISTS main")
    logger.info("Created schema 'main'")

    # Create table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS main.products (
            id INTEGER,
            name VARCHAR,
            price DECIMAL
        )
    """)
    logger.info("Created table 'main.products'")

    # Insert sample data
    conn.execute("""
        INSERT INTO main.products VALUES
        (1, 'Laptop', 999.99),
        (2, 'Mouse', 25.50),
        (3, 'Keyboard', 75.00)
    """)
    logger.info("Inserted 3 rows into 'main.products'")

    conn.close()
    logger.info("DuckDB setup complete")


if __name__ == '__main__':
    setup_duckdb()
