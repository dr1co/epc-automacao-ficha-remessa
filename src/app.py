import os
import pandas as pd
import smtplib
import ssl
import traceback
from datetime import datetime, timedelta
from dotenv import load_dotenv
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase

from .classes.Protheus import Protheus
from .classes.TotalBus import TotalBus
from .classes.DuckConnector import DuckConnector
from .utils import constants
from .utils.logger import logger
from .utils.constants import YESTERDAY

load_dotenv()

class App():
    def check_shipping_tickets(self, date: datetime = None):
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
        # 
        # Se a data não for especificada, a função irá utilizar o dia anterior ao de sua execução como padrão.
        # 
        # A função retorna 0 se não houver nenhum erro durante a sua execução, e 1 caso contrário.

        if date is None:
            logger.warning("Nenhuma data foi passada! Utilizando D-1...")
            date = constants.YESTERDAY

        logger.info("Iniciando processo de comparação de fichas de remessa...")

        duck_connector = DuckConnector()
        protheus_connector = Protheus()
        totalbus_connector = TotalBus()

        shipping_tickets = protheus_connector.get_shipping_ticket_summary(date=date)
        
        if shipping_tickets is None:
            logger.error("Não foi possível receber as fichas de remessa do Protheus...")
            return 1
        else:
            logger.success("Fichas de remessa obtidas com sucesso!")

        if len(shipping_tickets) == 0:
            logger.info("Não há nenhuma ficha de remessa para avaliar hoje!")
            return 0

        valid_tickets = []
        incongruent_tickets = []
        
        for i, ticket in enumerate(shipping_tickets.itertuples()):
            try:
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
                        "observacao": "Ficha de remessa zerada."
                    })
                    continue

                # Procura ticket no TotalBus
                totalbus_tickets = totalbus_connector.get_agency_shipping_report(date=date, agency_name=agency_name)
                observation_message = ""
                incongruence_message = ""

                if totalbus_tickets is None:
                    logger.error(f"Houve um erro ao procurar pela ficha da agência {agency_name} no TotalBus. Pulando...")
                    incongruent_tickets.append({
                        "nome_agencia": agency_name,
                        "cod_agencia_protheus": agency_code,
                        "num_ficha_protheus": ticket.ticket_number,
                        "motivo_erro": "Ocorreu um erro ao buscar a ficha da agência no TotalBus.",
                    })
                    continue

                # 1 - Verifica se o valor de receita do Protheus é o mesmo do TotalBus
                receipt_matching = False
                for totalbus_ticket in totalbus_tickets:
                    if round(totalbus_ticket["receipt_total"], 2) == round(float(ticket.receipt), 2):
                        receipt_matching = True
                
                if not receipt_matching:
                    incongruence_message += ";Valor da receita não está batendo."

                # Procura detalhes de receitas e despesas no Protheus e TotalBus
                protheus_details = protheus_connector.get_shipping_details(date=date, agency_code=agency_code, associated_company=associated_company)
                totalbus_extra_events = totalbus_connector.get_agency_extra_events(date=date, agency_name=agency_name)
                totalbus_cancelled_total = totalbus_connector.get_agency_cancelled_total(date=date, agency_name=agency_name)

                # Cálculo do valor total cancelado ou devolvido, do Protheus e TotalBus
                protheus_cancelled_total = 0
                protheus_find_cancelled_transactions = protheus_details.loc[protheus_details["transaction_description"].str.contains("BILHETE CANCELADO", na=False)]

                if len(protheus_find_cancelled_transactions) > 0:
                    protheus_cancelled_total += float(protheus_find_cancelled_transactions.iloc[0]["transaction_value"])
                
                protheus_find_returned_transactions = protheus_details.loc[protheus_details["transaction_description"].str.contains("BILHETE DEVOLVIDO", na=False)]

                if len(protheus_find_returned_transactions) > 0:
                    protheus_cancelled_total += float(protheus_find_returned_transactions.iloc[0]["transaction_value"])

                # 2 - Verifica bilhetes cancelados e devolvidos em ambas as plataformas
                cancelled_matching = False

                if protheus_cancelled_total == 0 and len(totalbus_cancelled_total) == 0:
                    observation_message += ";Não há bilhetes cancelados e devolvidos."
                    cancelled_matching = True

                for totalbus_ticket in totalbus_cancelled_total:
                    if round(totalbus_ticket["cancelled_total"], 2) == round(protheus_cancelled_total, 2):
                        cancelled_matching = True

                if not cancelled_matching:
                    incongruence_message += ";Valores de bilhetes cancelados e devolvidos não batem."

                # 3 - Verifica se há despesas extras ou receitas extras registradas no Protheus
                for extra_event in totalbus_extra_events:
                    protheus_find_extra_event = protheus_details.loc[protheus_details["transaction_description"].str.contains(extra_event["description"], na=False)]
                    
                    if len(protheus_find_extra_event) == 0:
                        incongruence_message += f";Transação de {extra_event["description"]} não encontrada no Protheus."
                    elif float(protheus_find_extra_event.iloc[0]["transaction_value"]) != extra_event["total"]:
                        incongruence_message += f";Valor de {extra_event["description"]} não bate com o valor encontrado no Protheus."

                # 4 - Verifica se há Vendas POS e Requisições no protheus
                protheus_pos = protheus_details.loc[protheus_details["transaction_description"].str.contains("POS", na=False)]

                if not protheus_pos.empty:
                    message = ";A ficha contém Vendas POS."

                    if incongruence_message != "":
                        incongruence_message += message
                    else:
                        observation_message += message
                
                protheus_requisitions = protheus_details.loc[protheus_details["transaction_description"].str.contains("REQUISIÇÕES", na=False)]

                if not protheus_requisitions.empty:
                    message = ";A ficha contém requisições."

                    if incongruence_message != "":
                        incongruence_message += message
                    else:
                        observation_message += message
                
                if incongruence_message == "":
                    valid_tickets.append({
                        "nome_agencia": agency_name,
                        "cod_agencia_protheus": agency_code,
                        "num_ficha_protheus": ticket.ticket_number,
                        "observacao": observation_message.replace(";", "", 1) if observation_message != "" else "Nada a declarar.",
                    })
                else:
                    incongruent_tickets.append({
                        "nome_agencia": agency_name,
                        "cod_agencia_protheus": agency_code,
                        "num_ficha_protheus": ticket.ticket_number,
                        "motivo_erro": incongruence_message.replace(";", "", 1),
                    })
            except Exception as e:
                logger.error(f"Ocorreu um erro ao fazer o comparativo da ficha de remessa {i + 1}")
                logger.error(f"Motivo: {e}")
                tb_str = traceback.format_exc()
                logger.error(f"\n{tb_str}")
        
        logger.info("-"*100)
        
        duck_connector.upsert_data(df=pd.DataFrame(valid_tickets), table_name="valid_tickets")
        duck_connector.upsert_data(df=pd.DataFrame(incongruent_tickets), table_name="incongruent_tickets")

        logger.success("Comparativo concluído com sucesso!")
        return 0

    def generate_csv_files(self, date: datetime = None):
        # Esta função tem como objetivo apenas gerar os arquivos .xlsx com as informações das fichas de remessa.
        # Os arquivos são gerados a partir do banco duck.db e inseridos no diretório 'database/csv/'
        #
        # Se a data não for especificada, a função irá utilizar o dia anterior ao de sua execução como padrão.
        #
        # O retorno dessa função é 0 se tudo correr bem, e 1 se caso houver algum erro.

        if date is None:
            logger.warning("Nenhuma data foi passada! Utilizando D-1...")
            date = constants.YESTERDAY

        date = date - timedelta(days=1)

        duck_connector = DuckConnector()

        logger.info("Iniciando exportação dos arquivos de fichas de remessa...")

        try:
            valid_tickets_query = f"SELECT * FROM valid_tickets WHERE num_ficha_protheus = {date.strftime("%Y%m%d")}"
            duck_connector.export_data_to_csv(query=valid_tickets_query, filename="fichas_validadas")

            incongruent_tickets_query = f"SELECT * FROM incongruent_tickets WHERE num_ficha_protheus = {date.strftime("%Y%m%d")}"
            duck_connector.export_data_to_csv(query=incongruent_tickets_query, filename="fichas_discrepantes")

            logger.success("Arquivos .csv gerados com sucesso!")
            return 0
        except Exception as e:
            logger.error("Não foi possível concluir a geração dos arquivos .xlsx.")
            logger.error(f"Motivo: {e}")
            tb_str = traceback.format_exc()
            logger.error(f"\n{tb_str}")
            return 1
    
    def attach_file_to_mail(self, date: datetime = None, mail = None, file_path: str = None):
        # Esta função serve para colocar arquivos de anexo no e-mail passado. Assim retornando o objeto de e-mail com o anexo.
        #
        # Se a função não receber o objeto de e-mail ou o caminho do arquivo, ela não executará, assim retornando None.

        if mail is None or file_path is None:
            logger.error("Objeto de e-mail ou caminho do arquivo não foi passado. Interrompendo a execução do programa...")
            return None

        if date is None:
            logger.warning("Nenhuma data foi passada. Utilizando D-1")
            date = constants.YESTERDAY

        date = date - timedelta(days=1)
        
        with open(file_path, "rb") as file:
            payload = MIMEBase("application", "octet-stream")
            payload.set_payload(file.read())
            
        encoders.encode_base64(payload)
        payload.add_header(
            "Content-Disposition",
            f"attachment; filename={date.strftime("%d-%m-%Y")}_{os.path.basename(file_path)}"
        )
        
        mail.attach(payload)

        return mail

    def send_email(self, date: datetime = None, recipients: list = None):
        # Esta função tem como objetivo disparar e-mails informando as fichas válidas e inválidas para os destinatários especificados.
        #
        # A função utiliza o serviço SMTP da princesa e o remetente é o usuário 'naoresponda-automate@princesadoscampos.com.br'.
        #
        # Para o envio dos e-mails, é necessário especificar os destinatários no parâmetro 'recipients'. Sem esse parâmetro especificado,
        # a função não executará.
        #
        # Se a data não for especificada, a função irá utilizar o dia anterior ao de sua execução como padrão.
        #
        # A função sempre retornará None.
        
        if recipients is None or len(recipients) == 0:
            logger.error("Nenhum recipiente foi especificado. Envio do e-mail será descartado...")
            return None

        if date is None:
            logger.warning("Nenhuma data foi passada. Utilizando D-1")
            date = constants.YESTERDAY

        logger.info("Iniciando a construção e envio do e-mail...")

        user = os.getenv("SMTP_USER")
        password = os.getenv("SMTP_PASSWORD")
        body_path = os.path.join(constants.HTML_PATH, "mail_body.html")

        with open(body_path, "r", encoding="utf-8") as file:
            mail_body = file.read().format(**{
                "date": date.strftime("%d/%m/%Y")
            })

        try:
            mail = MIMEMultipart('mixed')
            mail["From"] = f"Teste - enviado por {user}"
            mail["To"] = ", ".join(recipients)
            mail["Subject"] = f"Conferência de fichas do dia {date.strftime("%d/%m/%Y")}"
            mail.attach(MIMEText(mail_body, "html"))
            
            logger.info("Lendo o arquivo das fichas de remessa válidas...")
            tickets_path = os.path.join(constants.CSV_PATH, "fichas_validadas.xlsx")
            self.attach_file_to_mail(date=date, mail=mail, file_path=tickets_path)

            logger.info("Lendo o arquivo das fichas de remessa discrepantes...")
            tickets_path = os.path.join(constants.CSV_PATH, "fichas_discrepantes.xlsx")
            self.attach_file_to_mail(date=date, mail=mail, file_path=tickets_path)

            with smtplib.SMTP("smtp.office365.com", 587) as server:
                server.ehlo()
                server.starttls(context=ssl.create_default_context())
                server.ehlo()
                server.login(user, password)
                server.sendmail(user, recipients, mail.as_string())
                server.quit()
            
            logger.info("Email enviado com sucesso!")
        except Exception as e:
            logger.error("Ocorreu um erro ao enviar o e-mail.")
            logger.error(f"Motivo: {e}")
            tb_str = traceback.format_exc()
            logger.error(f"\n{tb_str}")
        
        return None
