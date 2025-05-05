from datetime import datetime
import pandas as pd

from .classes.Protheus import Protheus
from .classes.TotalBus import TotalBus
from .utils.logger import logger
from .utils.constants import YESTERDAY

class App():
    def check_shipping_tickets(self):
        # Esta função é a função principal da automação. É ela quem vai fazer a checagem das fichas de remessa
        #
        # A função comeca puxando as fichas de remessa existentes no Protheus, e daí faz o comparativo com as
        # informações do TotalBus.
        #
        # A checagem é feita da seguinte forma:
        # 1 - Verifica se o valor de receita do Protheus é o mesmo do TotalBus
        # 2 - Verifica bilhetes cancelados e devolvidos em ambas as plataformas
        # 3 - Verifica se há despesas extras ou receitas extras registradas no Protheus
        #
        # Após a checagem, as informações de quais fichas e agências estão corretas ou discrepantes é armazenada no
        # banco de dados, com a data de verificação. Fichas discrepantes devem conter uma mensagem do quê está
        # discrepante na conferência dos bilhetes.

        logger.info("Iniciando processo de comparação de fichas de remessa...")

        protheus_connector = Protheus()
        totalbus_connector = TotalBus()

        date = datetime(2025, 4, 16)

        shipping_tickets = protheus_connector.get_shipping_ticket_summary(date=date)
        
        if shipping_tickets is None:
            return -1
        else:
            logger.success("Fichas de remessa obtidas com sucesso!")

        if len(shipping_tickets) == 0:
            logger.info("Não há nenhuma ficha de remessa para avaliar hoje!")
            return 0

        valid_tickets = []
        incongruent_tickets = []

        for i, ticket in enumerate(shipping_tickets.itertuples()):
            agency_name = ticket.agency_name.strip()
            agency_code = ticket.agency_code.strip()
            associated_company = ticket.associated_company.strip()

            logger.info("-" * 100)
            logger.info(f"Verificando ficha de remessa {i+1}/{len(shipping_tickets)}...")
            logger.info(f"Nome da agência: {agency_name}.")
            logger.info(f"Empresa: {associated_company}")
            logger.info(f"Código da agência: {agency_code}.")

            # Ignora fichas de remessa vazias
            if float(ticket.receipt) == 0.0:
                logger.info("Ficha vazia! Pulando...")
                valid_tickets.append({
                    "nome_agencia": agency_name,
                    "cod_agencia_protheus": agency_code,
                    "num_ficha_protheus": ticket.ticket_number,
                })
                continue

            # Procura ticket no TotalBus
            totalbus_ticket = totalbus_connector.get_agency_shipping_report(date=date, agency_name=agency_name, associated_company=associated_company)
            incongruence_message = ""


            if totalbus_ticket is None:
                logger.error(f"Houve um erro ao procurar pela ficha da agência {agency_name} no TotalBus. Pulando...")
                incongruent_tickets.append({
                    "nome_agencia": agency_name,
                    "cod_agencia_protheus": agency_code,
                    "num_ficha_protheus": ticket.ticket_number,
                    "motivo_erro": "Ocorreu um erro ao buscar a ficha da agência no TotalBus.",
                })
                continue

            # 1 - Verifica se o valor de receita do Protheus é o mesmo do TotalBus
            if totalbus_ticket["receipt_total"] != float(ticket.receipt):
                incongruence_message += ";Valor da receita está discrepante."

            # Procura detalhes de receitas e despesas no Protheus e TotalBus
            protheus_details = protheus_connector.get_shipping_details(date=date, agency_code=agency_code, associated_company=associated_company)
            totalbus_extra_events = totalbus_connector.get_agency_extra_events(date=date, agency_name=agency_name, associated_company=associated_company)
            totalbus_cancelled_transactions = totalbus_connector.get_agency_cancelled_transactions(date=date, agency_name=agency_name, associated_company=associated_company)

            # Cálculo do valor total cancelado ou devolvido, do Protheus e TotalBus
            protheus_cancelled_total = 0
            protheus_find_cancelled_transactions = protheus_details.loc[protheus_details["transaction_description"].str.contains("BILHETE CANCELADO", na=False)]

            if len(protheus_find_cancelled_transactions) > 0:
                protheus_cancelled_total += float(protheus_find_cancelled_transactions.iloc[0]["transaction_value"])
            
            protheus_find_returned_transactions = protheus_details.loc[protheus_details["transaction_description"].str.contains("BILHETE DEVOLVIDO", na=False)]

            if len(protheus_find_returned_transactions) > 0:
                protheus_cancelled_total += float(protheus_find_returned_transactions.iloc[0]["transaction_value"])

            totalbus_cancelled_total = sum([transaction["total"] for transaction in totalbus_cancelled_transactions])

            # 2 - Verifica bilhetes cancelados e devolvidos em ambas as plataformas 
            if protheus_cancelled_total != totalbus_cancelled_total:
                incongruence_message += ";Valores de bilhetes cancelados e devolvidos não batem."

            # 3 - Verifica se há despesas extras ou receitas extras registradas no Protheus
            for extra_event in totalbus_extra_events:
                protheus_find_extra_event = protheus_details.loc[protheus_details["transaction_description"].str.contains(extra_event["description"], na=False)]
                
                if len(protheus_find_extra_event) == 0:
                    incongruence_message += f";Transação de {extra_event["description"]} não encontrada no Protheus."
                elif float(protheus_find_extra_event.iloc[0]["transaction_value"]) != extra_event["total"]:
                    incongruence_message += f";Valor de {extra_event["description"]} não bate com o valor encontrado no Protheus."
            
            if incongruence_message == "":
                valid_tickets.append({
                    "nome_agencia": agency_name,
                    "cod_agencia_protheus": agency_code,
                    "num_ficha_protheus": ticket.ticket_number,
                })
            else:
                incongruent_tickets.append({
                    "nome_agencia": agency_name,
                    "cod_agencia_protheus": agency_code,
                    "num_ficha_protheus": ticket.ticket_number,
                    "motivo_erro": incongruence_message.replace(";", "", 1),
                })
        
        logger.info("-"*100)
        
        protheus_connector.upsert_data(df=pd.DataFrame(valid_tickets), table_name="valid_tickets")
        protheus_connector.upsert_data(df=pd.DataFrame(incongruent_tickets), table_name="incongruent_tickets")
