import logging
import sys

import pandas as pd

# logging configuration
logging.getLogger(__name__).addHandler(logging.NullHandler())
logging.basicConfig(
    stream=sys.stdout, level=logging.INFO, format="%(levelname)s: %(message)s"
)

# pandas options
pd.set_option("future.no_silent_downcasting", True)
