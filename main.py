import sys
import time
from datetime import datetime, timedelta

from src.utils import constants
from src.utils.logger import logger, log_execution_time
from src.app import App

@log_execution_time
def main():
    app = App()
    date = datetime(2025, 5, 6)

    # Inicia comparativo de fichas de remessa
    app.check_shipping_tickets(date=date)

    # Inicia exportação para .xlsx
    app.generate_csv_files(date=date)

    # Envia o e-mail com os arquivos para os destinatários especificados
    # recipients = [
    #     "adriano.lipski@princesadoscampos.com.br",
    #     "thelma.silva@princesadoscampos.com.br",
    #     "marlon.siqueira@princesadoscampos.com.br",
    #     "nayara.roca@princesadoscampos.com.br",
    #     # "wesley.soares@princesadoscampos.com.br",
    #     # "erinelson.santos@princesadoscampos.com.br"
    # ]
    # app.send_email(date=date, recipients=recipients)
    logger.success("Processo concluído com sucesso!")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        logger.warning("Aviso! Argumentos de terminal serão ignorados.")

    try:
        main()
    except Exception as e:
        logger.error(f"Erro durante a execução: {str(e)}")
        sys.exit(1)
    sys.exit(0)
