import os
import traceback
import sqlalchemy as sqla
from dotenv import load_dotenv
from datetime import datetime, timedelta

from .BaseClasses import BaseDBConnector
from src.utils.logger import logger
from src.utils.constants import YESTERDAY, SQL_PATH

load_dotenv()

class TotalBus(BaseDBConnector):
    def __init__(self):
        super().__init__()
        
        self.server = os.getenv("ORACLE_SERVER")
        self.service = os.getenv("ORACLE_SID")
        self.username = os.getenv("ORACLE_USERNAME")
        self.password = os.getenv("ORACLE_PASSWORD")
        self.port = os.getenv("ORACLE_PORT")

        self.conn_string = f"oracle+oracledb://{self.username}:{self.password}@{self.server}:{self.port}/?service_name={self.service}"

    def print_connection(self):
        print(self.conn_string)

    def get_agency_shipping_report(self, date: datetime = None, agency_name: str = None):
        # Essa função tem como objetivo buscar a receita de uma ficha de remessa de uma agência no TotalBus (RJ)
        # a partir do seu nome e de uma data específica.
        #
        # Se a data não for especificada, a função irá utilizar o dia anterior ao de sua execução como padrão.
        #
        # A função não irá executar se o nome da agência ou a empresa associada não forem passados, assim retornando None.
        #
        # Ao passar os parâmetros, a função deve retornar em um dicionário os valores totais de:
        # - Id da empresa (Cantelle ou Princesa)
        # - Passagens
        # - Taxas de embarque
        # - Taxas de pedágio
        # - Outras taxas
        # - Taxas de seguro
        # - Receita total
        #
        # Se houver algum erro de execução durante a consulta, esta função irá retornar None

        if agency_name is None:
            logger.error("Agência não especificada! Interrompendo execução da função...")
            return None

        if date is None:
            logger.warning("Data não especificada, usando D-1...")
            date = YESTERDAY

        start_date = (date - timedelta(days=1)).strftime("%d-%b-%y")
        end_date = date.strftime("%d-%b-%y")

        try:
            file_path = os.path.join(SQL_PATH, "totalbus_agency_shipping_report.sql")
            with open(file_path) as file:
                logger.debug(f"Buscando a ficha de remessa da agência {agency_name} - Data: {date.strftime("%d/%m/%Y")}...")
                query = file.read().format(**locals())

                result = self.fetchmany_read_query(query=query)
                
                result_list = [{
                    "associated_company": ticket[1]["associated_company"],
                    "ticket_total": ticket[1]["ticket_total"],
                    "boarding_tax_total": ticket[1]["boarding_tax_total"],
                    "toll_tax_total": ticket[1]["toll_tax_total"],
                    "others_total": ticket[1]["others_total"],
                    "insurance_total": ticket[1]["insurance_total"],
                    "receipt_total": sum(map(lambda x: float(x) if x is not None else 0.0, ticket[1][1::]))
                } for ticket in result.iterrows()]

                return result_list
        except Exception as e:
            logger.error(f"Não foi possível buscar a ficha da agência {agency_name}. Cheque os logs de debug!")
            logger.error(f"Motivo: {e}")
            tb_str = traceback.format_exc()
            logger.error(f"\n{tb_str}")
            return None

    def get_agency_cancelled_total(self, date: datetime = None, agency_name: str = None):
        # Essa função tem como objetivo buscar apenas o total de transações canceladas de uma ficha de remessa de uma
        # agência no TotalBus (RJ) a partir do seu nome e de uma data específica.
        #
        # Se a data não for especificada, a função irá utilizar o dia anterior ao de sua execução como padrão.
        #
        # Note que a função não busca os bilhetes vendidos, entregues e transferidos, pois esses categorizam como receita,
        # que já foi somada na função get_agency_shipping_report!
        #
        # A função não irá executar se o nome da agência não for passado, assim retornando None.
        #
        # Ao passar os parâmetros, a função deve retornar as transações canceladas em um dicionário, contendo as seguintes chaves:
        # - Id da empresa (Cantelle ou Princesa)
        # - Passagens
        # - Taxas de embarque
        # - Taxas de pedágio
        # - Outras taxas
        # - Taxas de seguro
        # - Receita total
        #
        # Se houver algum erro de execução durante a consulta, esta função irá retornar None

        if agency_name is None:
            logger.error("Agência não especificada! Interrompendo execução da função...")
            return None

        if date is None:
            logger.warning("Data não especificada, usando D-1...")
            date = YESTERDAY
        
        start_date = (date - timedelta(days=1)).strftime("%d-%b-%y")
        end_date = date.strftime("%d-%b-%y")

        try:
            file_path = os.path.join(SQL_PATH, "totalbus_agency_cancelled_transactions.sql")
            with open(file_path) as file:
                logger.debug(f"Buscando transações canceladas da ficha de remessa da agência {agency_name} - Data: {date.strftime("%d/%m/%Y")}...")
                query = file.read().format(**locals())

                cancelled_transactions = self.fetchmany_read_query(query=query)

                cancelled_transactions = [{
                    "associated_company": ticket[1]["associated_company"],
                    "ticket_total": ticket[1]["ticket_total"],
                    "boarding_tax_total": ticket[1]["boarding_tax_total"],
                    "toll_tax_total": ticket[1]["toll_tax_total"],
                    "others_total": ticket[1]["others_total"],
                    "insurance_total": ticket[1]["insurance_total"],
                    "cancelled_total": sum(map(lambda x: float(x) if x is not None else 0.0, ticket[1][1::]))
                } for ticket in cancelled_transactions.iterrows()]

                return cancelled_transactions
        except Exception as e:
            logger.error(f"Não foi possível buscar as transações da agência {agency_name}. Cheque os logs de debug!")
            logger.error(f"Motivo: {e}")
            tb_str = traceback.format_exc()
            logger.error(f"\n{tb_str}")
            return None

    def get_agency_extra_events(self, date: datetime = None, agency_name: str = None):
        # Esta função tem como objetivo retornar todas as transações extras de uma agência no TotalBus (RJ) em determinado dia.
        #
        # Se a data não for especificada, a função irá utilizar o dia anterior ao de sua execução como padrão.
        #
        # A função não irá executar se o nome da agência não for passado, assim retornando None.
        #
        # Ao passar os parâmetros, a função deve retornar as transações extras em um dicionário, contendo as seguintes chaves:
        # - Descrição - O que é a transação (ex: multa, excesso de bagagem, etc.)
        # - Natureza - Se é receita ou despesa (obs: pode vir como [NULL])
        # - Valor total - Valor total daquele tipo de transação, já somado se houver mais de uma do mesmo tipo
        #
        # Se houver algum erro de execução durante a consulta, esta função irá retornar None

        if agency_name is None:
            logger.error("Agência não especificada! Interrompendo execução da função...")
            return None

        if date is None:
            logger.warning("Data não especificada, usando D-1...")
            date = YESTERDAY

        start_date = (date - timedelta(days=1)).strftime("%d-%b-%y")
        end_date = date.strftime("%d-%b-%y")

        try:
            file_path = os.path.join(SQL_PATH, "totalbus_agency_extra_events.sql")
            with open(file_path) as file:
                logger.debug(f"Buscando todas as transações extras da agência {agency_name} - Data: {date.strftime("%d/%m/%Y")}...")
                query = file.read().format(**locals())

                extra_events = self.fetchmany_read_query(query=query)

                extra_events = [{
                    "associated_company": event.associated_company,
                    "description": event.bill_description.strip(),
                    "nature": event.nature,
                    "total": float(event.bill_value) or 0.0
                } for event in extra_events.itertuples()]
                return extra_events
        except Exception as e:
            logger.error(f"Não foi possível buscar as transações extras da ficha de remessa da agência {agency_name}. Cheque os logs de debug!")
            logger.error(f"Motivo: {e}")
            tb_str = traceback.format_exc()
            logger.error(f"\n{tb_str}")
            return None
