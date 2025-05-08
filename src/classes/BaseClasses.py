import os
import traceback
import sqlalchemy as sqla
from sqlalchemy.exc import OperationalError
import pandas as pd

from src.utils import constants
from src.utils.logger import logger

class BaseDBConnector:
    def __init__(self):
        self.conn_string = None

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
