import os
import duckdb
import traceback
import sqlalchemy as sqla
from sqlalchemy.exc import OperationalError
import pandas as pd
from datetime import datetime

from src.utils import constants
from src.utils.logger import logger

class BaseDBConnector:
    def __init__(self):
        self.conn_string = None
        self.db_folder = constants.DATA_PATH

        self.duck_connection = duckdb.connect(os.path.join(self.db_folder, "duck.db"))

    def fetchone_read_query(self, query: str):
        if self.conn_string is None:
            logger.error("Não foi possível fazer a conexão com o banco, pois a string de conexão não foi definida!")
            return None
        
        engine = sqla.create_engine(self.conn_string)

        attempt = 1
        max_tries = 3
        delay = 60

        while attempt <= max_tries:
            try:
                logger.debug("Executando query...")
                logger.debug(query)

                with engine.connect() as conn:
                    result = conn.execute(sqla.text(query))
                    data = result.fetchall()

                    columns = result.keys()
                    df = pd.DataFrame(data, columns=columns) if data else pd.DataFrame(columns=columns)

                    conn.close()

                return df.iloc[0]
            except OperationalError as e:
                logger.error(f"Erro ao executar a query: {e}. Aguardando {delay} segundos...")
                tb_str = traceback.format_exc()
                logger.error(f"\n{tb_str}")
                attempt += 1
                time.sleep(delay)
        
        logger.error("Não foi possível realizar a consulta...")
        return None

    def fetchmany_read_query(self, query: str):
        if self.conn_string is None:
            logger.error("Não foi possível fazer a conexão com o banco, pois a string de conexão não foi definida!")
            return None
        
        engine = sqla.create_engine(self.conn_string)

        attempt = 1
        max_tries = 3
        delay = 60

        while attempt <= max_tries:
            try:
                logger.debug("Executando query...")
                logger.debug(query)

                with engine.connect() as conn:
                    result = conn.execute(sqla.text(query))
                    data = result.fetchall()

                    columns = result.keys()
                    df = pd.DataFrame(data, columns=columns) if data else pd.DataFrame(columns=columns)

                    conn.close()

                return df
            except OperationalError as e:
                logger.error(f"Erro ao executar a query: {e}. Aguardando {delay} segundos...")
                tb_str = traceback.format_exc()
                logger.error(f"\n{tb_str}")
                attempt += 1
                time.sleep(delay)

        logger.error("Não foi possível realizar a consulta...")
        return None

    def upsert_data(self, df, table_name, *, include_columns=[], exclude_columns=[], update_schema=False):
        # Perform an upsert operation (update or insert) on the specified table.
        #
        # Args:
        #     df (pandas.DataFrame): The source DataFrame containing the data to upsert.
        #     table_name (str): The name of the target table in the database.
        #     include_columns (list, optional): Specific columns to use for matching rows. If provided, only these columns are used for matching.
        #     exclude_columns (list, optional): Columns to exclude from the matching logic. This argument cannot be used with `include_columns`.
        #     update_schema (bool, optional): If True, allows schema evolution by adding missing columns. Defaults to False.
        #
        # Returns:
        #     int: The total number of rows affected (updated + inserted).
        #
        # Raises:
        #     ValueError: If both 'include_columns' and 'exclude_columns' are provided.

        self.duck_connection.execute(f"DROP TABLE IF EXISTS {table_name}")

        if include_columns and exclude_columns:
            raise ValueError("Arguments 'include_columns' and 'exclude_columns' cannot be used together.")

        # Ensure 'data_processamento' exists in the DataFrame
        if 'data_processamento' not in df.columns:
            df['data_processamento'] = datetime.now()

        self.duck_connection.register("temp_df", df)

        # Check if the table exists
        table_exists = self.duck_connection.execute(
            f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        ).fetchone()[0]

        if table_exists == 0:
            # If the table doesn't exist, create it and insert data
            logger.info(f"Criando tabela {table_name}")
            return (
                self.duck_connection.sql(
                    f"CREATE TABLE {table_name} AS SELECT * FROM temp_df"
                )
            )
        else:
            # Get existing columns in the destination table
            existing_columns = [
                row[0]
                for row in self.duck_connection.execute(
                    f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                    """
                ).fetchall()
            ]

            if update_schema:
                # Identify missing columns
                missing_columns = [
                    col for col in df.columns if col not in existing_columns
                ]

                # Add missing columns to the destination table
                for col in missing_columns:
                    if col == "data_processamento":
                        logger.info(f"Adicionando coluna '{col}' como TIMESTAMP na tabela {table_name}")
                        self.duck_connection.execute(
                            f"ALTER TABLE {table_name} ADD COLUMN {col} TIMESTAMP DEFAULT (now())"
                        )
                    else:
                        col_type = str(df[col].dtype)
                        if col_type == "int64":
                            sql_type = "BIGINT"
                        elif col_type == "float64":
                            sql_type = "DOUBLE"
                        elif col_type == "object":
                            sql_type = "VARCHAR"
                        elif col_type.startswith("datetime"):
                            sql_type = "TIMESTAMP"
                        else:
                            sql_type = "VARCHAR"

                        logger.info(f"Adicionando coluna '{col}' com tipo {sql_type} na tabela {table_name}")
                        self.duck_connection.execute(
                            f"ALTER TABLE {table_name} ADD COLUMN {col} {sql_type}"
                        )

            # Determine columns for matching
            if include_columns:
                match_columns = include_columns
                
            else:
                match_columns = [
                    col for col in df.columns if col not in exclude_columns and col != 'data_processamento'
                ]
            common_columns = set(df.columns) & set(existing_columns)
            def quote_wrap(iterable):
                return [f'"{x}"' for x in iterable]
            match_columns = quote_wrap(match_columns)
            common_columns = quote_wrap(common_columns)
            
            # Update matching rows
            update_query = f"""
                UPDATE {table_name}
                SET {',\n    '.join(f'{col} = temp_df.{col}' for col in common_columns if col != 'data_processamento')}
                FROM temp_df
                WHERE 1=1
                AND {'\n    AND '.join(f'{table_name}.{col} = temp_df.{col}' for col in match_columns)}
                AND {table_name}.data_processamento < temp_df.data_processamento
                RETURNING *;
            """
            logger.trace(update_query)
            updated_rows = (
                self.duck_connection.sql(update_query).aggregate("count(*)").fetchone()[0]
            )

            if updated_rows > 1:
                logger.info(
                    f"{updated_rows} registros atualizados na tabela {table_name}"
                )
            else:
                logger.debug(
                    f"{updated_rows} registros atualizados na tabela {table_name}"
                )

            # Insert missing rows
            insert_query = f'''
                INSERT INTO {table_name} ({', '.join(quote_wrap(existing_columns))}) 
                SELECT {', '.join(
                    f'source."{col}"' if col in df.columns else "NULL" for col in existing_columns
                )}
                FROM temp_df AS source
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {table_name} AS target
                    WHERE 1=1
                    AND {'\n    AND '.join(f"source.{col} = target.{col}" for col in match_columns)}
                )
                RETURNING *;
            '''
            logger.trace(insert_query)
            inserted_rows = (
                self.duck_connection.sql(insert_query).aggregate("count(*)").fetchone()[0]
            )

            if inserted_rows > 1:
                logger.info(
                    f"{inserted_rows} registros inseridos na tabela {table_name}"
                )
            else:
                logger.debug(
                    f"{inserted_rows} registros inseridos na tabela {table_name}"
                )

            affected_rows = updated_rows + inserted_rows

            # Clean up temporary table
            self.duck_connection.unregister("temp_df")

            return affected_rows
