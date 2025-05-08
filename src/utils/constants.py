import os
from datetime import datetime, timedelta

DATA_PATH = "database"
SQL_PATH = os.path.join("src", "repositories")
HTML_PATH = os.path.join("src", "html")
CSV_PATH = os.path.join(DATA_PATH, "csv")
YESTERDAY = datetime.now() - timedelta(days=1)