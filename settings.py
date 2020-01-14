# import os
# import platform
# import re
import sys
from pathlib import Path
from dotenv import load_dotenv

sys.path.append('')

# @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
# define execution and root directories and load .env file
EXECUTION_DIR = Path('.').resolve()
ROOT_DIR = EXECUTION_DIR / 'src'
ENV_PATH = ROOT_DIR / '.env'
load_dotenv(ENV_PATH)
