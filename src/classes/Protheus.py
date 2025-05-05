import os
import traceback
import sqlalchemy as sqla
from dotenv import load_dotenv
from datetime import datetime

from .BaseClasses import BaseDBConnector
from src.utils.logger import logger
from src.utils.constants import YESTERDAY, SQL_PATH

load_dotenv()

class Protheus(BaseDBConnector):
    def __init__(self):
        super().__init__()

        self.server = os.getenv("PROTHEUS_SERVER")
        self.database = os.getenv("PROTHEUS_DB")
        self.username = os.getenv("PROTHEUS_USERNAME")
        self.password = os.getenv("PROTHEUS_PASSWORD")
        self.port = os.getenv("PROTHEUS_PORT")

        self.conn_string = f"mssql+pyodbc://{self.username}:{self.password}@{self.server}:{self.port}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server"

    def print_connection(self):
        print(self.conn_string)

    def get_shipping_ticket_summary(self, date: datetime = None):
        # Esta função tem como objetivo buscar no Protheus todas as fichas de remessa emitidas em uma determinada data.
        #
        # Se a data não for especificada, a função irá utilizar o dia anterior ao de sua execução como padrão.
        #
        # Ao ser executada, a função irá retornar um DataFrame com as seguintes colunas:
        # - Nome da agência
        # - Código da agência - Código da agência no Protheus (NÃO É O MESMO DO CÓDIGO DA AGÊNCIA NO TOTALBUS!)
        # - Número da ficha - Segue o formato padrão de YYYYMMDD que representa a data do fechamento dessa ficha
        # - Receita
        # - Despesa
        # - Valor líquido - (Receita - Despesa)

        if date is None:
            logger.warning("Data não especificada, usando D-1...")
            date = YESTERDAY

        emission_date = date.strftime("%Y%m%d")

        try:
            path_file = os.path.join(SQL_PATH, "protheus_shipping_tickets.sql")
            with open(path_file) as file:
                logger.debug(f"Buscando todas as fichas de remessa do dia {date.strftime("%d/%m/%Y")}...")
                query = file.read().format(**locals())

                shipping_tickets = self.fetchmany_read_query(query=query)

            return shipping_tickets
        except Exception as e:
            logger.error(f"Não foi possível buscar as fichas de remessa. Cheque os logs de debug!")
            logger.error(f"Motivo: {e}")
            tb_str = traceback.format_exc()
            logger.error(f"\n{tb_str}")
            return None

    def get_shipping_details(self, date: datetime = None, agency_code: str = None, associated_company: str = None):
        # Esta função tem como objetivo buscar no Protheus detalhes de despesas e algumas receitas extras,
        # tais como multas, excesso de bagagem, etc. Tudo isso a partir do código da agência no Protheus.
        #
        # A consulta não lista os bilhetes transacionados da agência!
        #
        # Se não for passado o código da agência e a empresa associada, a função não executará, assim retornando None.
        #
        # Se a data não for especificada, a função irá utilizar o dia anterior de sua execução como padrão.
        #
        # Ao ser executada, o retorno esperado é um DataFrame com as seguintes colunas:
        # - Nome da agência
        # - Código da agência - Código da agência no Protheus (NÃO É O MESMO DO CÓDIGO DA AGÊNCIA NO TOTALBUS!)
        # - Número da ficha - Segue o formato padrão de YYYYMMDD que representa a data do fechamento dessa ficha
        # - Tipo de transação - Receita ou despesa
        # - Descrição da transação
        # - Valor - O valor referente àquele tipo de transação

        if agency_code is None or associated_company is None:
            logger.error("Código da agência não especificado, abortando a execução da função...")
            return None

        if date is None:
            logger.warning("Data não especificada, usando D-1...")
            date = YESTERDAY

        emission_date = date.strftime("%Y%m%d")

        try:
            path_file = os.path.join(SQL_PATH, "protheus_agency_details.sql")
            with open(path_file) as file:
                logger.debug(f"Buscando as transações adicionais da agência. Código: {agency_code}. Data: {date.strftime("%d/%m/%Y")}...")
                query = file.read().format(**locals())

                extra_events = self.fetchmany_read_query(query=query)

            return extra_events
        except Exception as e:
            logger.error(f"Não foi possível buscar as transações adicionais de {agency_code}. Cheque os logs de debug!")
            logger.error(f"Motivo: {e}")
            tb_str = traceback.format_exc()
            logger.error(f"\n{tb_str}")
            return None
