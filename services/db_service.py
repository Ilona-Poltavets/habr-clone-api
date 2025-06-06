from datetime import datetime

from dotenv import load_dotenv
from fastapi import HTTPException
from sqlalchemy import Column, Integer, String, Table, DateTime, inspect
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError, CompileError
from sqlalchemy.testing.suite.test_reflection import metadata

from services.logger import logger

load_dotenv()


def create_or_get_table_name(engine, metadata, table_name, json_data=None):
    columns = [
        Column("id", Integer, primary_key=True),
        Column("created_at", DateTime, default=datetime.utcnow),
        Column("updated_at", DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
    ]

    if json_data:
        for key in json_data[0].keys():
            if key not in ['id', 'created_at', 'updated_at']:
                columns.append(Column(key, String(256), nullable=True))

    table = Table(
        table_name,
        metadata,
        *columns
    )
    metadata.create_all(engine)

    return table


def check_connection(engine):
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            print("✅ DB connection successful:", result.scalar())
            inspector = inspect(engine)
            if 'entities' in inspector.get_table_names():
                print("✅ Table 'entities' exists.")
            else:
                print("⚠️ Table 'entities' does not exist. Creating it...")
                metadata.create_all(engine)
                print("✅ Table 'entities' created.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ DB connection failed: {str(e)}")


async def save_to_db(engine, metadata, json_data: list, table_name: str):
    try:
        logger.info(f"Start saving data to table '{table_name}'...")
        table = create_or_get_table_name(engine, metadata, table_name)
        conn = engine.connect()
        trans = conn.begin()

        table_columns = set(table.columns.keys())

        for item in json_data:
            filtered_item = {k: v for k, v in item.items() if k in table_columns}
            extra_keys = set(item.keys()) - table_columns
            if extra_keys:
                logger.warning(f"Skipping unused columns for table '{table_name}': {extra_keys}")

            insert_stmt = insert(table).values(**filtered_item)
            conn.execute(insert_stmt)

        trans.commit()
        logger.info(f"Data saved successfully to table '{table_name}'")
    except CompileError as e:
        logger.error(f"CompileError: {str(e)}")
        if 'trans' in locals():
            trans.rollback()
        raise
    except SQLAlchemyError as e:
        logger.error(f"Database error: {str(e)}")
        if 'trans' in locals():
            trans.rollback()
        raise
    except Exception as e:
        logger.error(f"Unexpected error while saving data: {str(e)}")
        if 'trans' in locals():
            trans.rollback()
        raise
    finally:
        if 'conn' in locals():
            conn.close()
