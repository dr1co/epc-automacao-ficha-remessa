import sys
import time
import msvcrt
import threading
from datetime import datetime

from src.utils.logger import logger, log_execution_time
from src.app import App

def wait_for_key_or_timeout(timeout=60):
    """
    Waits for a specified timeout or until a key is pressed, whichever comes first.
    """
    key_pressed = threading.Event()

    def check_key_press():
        while not key_pressed.is_set():
            if msvcrt.kbhit():  # Check if a key has been pressed
                msvcrt.getch()  # Consume the key press
                key_pressed.set()

    # Start a thread to listen for key press
    key_thread = threading.Thread(target=check_key_press, daemon=True)
    key_thread.start()

    print(f"Waiting for {timeout} seconds or a key press...")
    start_time = time.time()

    while time.time() - start_time < timeout:
        if key_pressed.is_set():
            print("Key pressed! Exiting early.")
            return
        time.sleep(0.1)

    print("Timeout reached.")

@log_execution_time
def main():
    app = App()
    app.check_shipping_tickets()
    t=15
    wait_for_key_or_timeout(t)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        logger.warning("Aviso! Argumentos de terminal serão ignorados.")

    try:
        main()
    except Exception as e:
        logger.error(f"Erro durante a execução: {str(e)}")
        sys.exit(1) 
    sys.exit(0) 
