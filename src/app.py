"""Main file of the dash application.

Starts the dash-Server and integrates all functions.
"""

#           --Dev_table_name--
#      advancinglake.default.databricks_plan

import databricks_utils  # noqa: F401
from layout import app

if __name__ == "__main__":
    app.run_server(debug=True, port=8)
