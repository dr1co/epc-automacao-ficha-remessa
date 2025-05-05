import sys
import os
import time
import traceback
from datetime import datetime
from functools import wraps
from loguru import logger

LOGS_FOLDER = 'logs'
DEBUG_FOLDER = os.path.join(LOGS_FOLDER, 'debug')
ERROR_FOLDER = os.path.join(LOGS_FOLDER, 'error')
INFO_FOLDER = os.path.join(LOGS_FOLDER, 'info')

def get_timestamp() -> str:
    '''retorna um timestamp no formato YYYYMMDD_HHMMSS'''
    return datetime.now().strftime("%Y%m%d_%H%M")
def get_date() -> str:
    '''retorna um data no formato YYYYMMDD'''
    return datetime.now().strftime("%Y%m%d")

FILE_DEBUG = f"app_debug_{get_timestamp()}.log"
FILE_INFO = f"app_info_{get_timestamp()}.log"
ERROR_INFO = f"app_error_{get_timestamp()}.log"
log_size = 5
logger.remove()  # Remove a configuração padrão do logger

# Logger para o terminal apenas com mensagens de nível DEBUG
logger.add(sys.stderr, level="DEBUG")

# Logger para o arquivo de log com mensagens de nível DEBUG
logger.add(os.path.join(DEBUG_FOLDER, get_date(), FILE_DEBUG), rotation=f"{log_size*2} MB", compression="zip", level='DEBUG')

# Logger para o arquivo de log com mensagens de nível INFO
logger.add(os.path.join(INFO_FOLDER, get_date(), FILE_INFO), rotation=f"{log_size} MB", compression="zip", level='INFO')

# Logger para o arquivo de log com mensagens de nível INFO
logger.add(os.path.join(ERROR_FOLDER, get_date(), ERROR_INFO), rotation=f"{log_size} MB", compression="zip", level='WARNING')

def log_execution_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.info(f"Iniciando o método {func.__name__}")
        try:
            result = func(*args, **kwargs)
            logger.success(f"Método {func.__name__} executado com sucesso")
            return result
        except Exception as e:
            logger.error(f"Erro ao executar o método {func.__name__}: {e}")
            tb_str = traceback.format_exc()
            logger.error(f"\n{tb_str}")
        finally:
            elapsed_time = time.time() - start_time
            hours, remainder = divmod(elapsed_time, 3600)
            minutes, seconds = divmod(remainder, 60)
            formatted_time = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"
            logger.info(f"Tempo gasto no método {func.__name__}: {formatted_time}")
            logger.info("-" * 100)

    return wrapper
