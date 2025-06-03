import os

import sqlalchemy
from fastapi import HTTPException
from sqlalchemy import Column, Integer, String, Text, Table, Boolean, Float, DateTime
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import ProgrammingError, SQLAlchemyError, CompileError
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

from dotenv import load_dotenv

from services.logger import logger

load_dotenv()

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


async def save_to_db(engine, metadata, data_list: list, table_name: str):
    try:
        logger.info("Start saving data to the database...")
        table = create_or_get_table_name(engine, metadata, table_name)
        conn = engine.connect()
        trans = conn.begin()

        table_columns = set(table.columns.keys())

        for row in data_list:
            filtered_row = {k: v for k, v in row.items() if k in table_columns}
            extra_keys = set(row.keys()) - table_columns
            if extra_keys:
                logger.warning(f"Skipping unused columns for table '{table_name}': {extra_keys}")

            insert_stmt = insert(table).values(**filtered_row)
            conn.execute(insert_stmt)

        trans.commit()
        logger.info("Data saved successfully")
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