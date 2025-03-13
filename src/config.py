"""Config-file for the dash Databricks planning App.

Enviroment variables are set and the Databricks session is initalized.
"""

import os

from databricks.connect import DatabricksSession

# Set environment variables
os.environ["DATABRICKS_CLUSTER_ID"] = "0114-143132-czexiq9e"

# Initialize Databricks session
spark = DatabricksSession.builder.getOrCreate()
