from sqlalchemy import create_engine, MetaData, Table, select
from loguru import logger


class SQLAlchemyHandler:
    """
    Generic class for SQLAlchemy interactions.
    """
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
        self.metadata = MetaData(bind=self.engine)

    def fetch_table(self, table_name: str):
        try:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            query = select(table)
            with self.engine.connect() as connection:
                result = connection.execute(query)
                return [dict(row) for row in result]
        except Exception as e:
            logger.exception(f"Failed to fetch table '{table_name}': {e}")
            raise


# Example Implementation
class ExampleSQLAlchemyHandler(SQLAlchemyHandler):
    """
    Example SQLAlchemy implementation.
    """
    def __init__(self):
        super().__init__("sqlite:///example.db")

    def fetch_example_table(self):
        return self.fetch_table("example_table")