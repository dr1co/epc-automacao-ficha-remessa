import os
from datetime import datetime, timedelta

DATA_PATH = "database"
SQL_PATH = os.path.join("src", "repositories")
YESTERDAY = datetime.now() - timedelta(days=1)