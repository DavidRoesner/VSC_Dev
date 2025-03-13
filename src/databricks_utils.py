"""Functions for accessing Databricks tables.

Methods for loading, saving data and retrieving primary keys.
"""

# --Main Imports--
from datetime import datetime

# --Third-party imports--
import pyspark
from dash.dependencies import Input, Output, State
from delta import DeltaTable
from pyspark.sql.functions import col
from pyspark.sql.types import *  # noqa: F403

# --Local application imports--
from config import spark
from layout import app


def load_data(table_name: str) -> tuple[list[dict], "pyspark.sql.types.StructType"]:
    """Load data dynamically based on user input.

    Args:
        table_name (str): The name of the table to load data from.

    Returns:
        tuple: A tuple containing:
            - list[dict]: A list of dictionaries representing the table data.
            - pyspark.sql.types.StructType: The schema of the target table.

    Raises:
        Exception: If the table cannot be loaded or if an error occurs during the data collection process.

    """
    try:
        df = spark.table(table_name)
        data = df.collect()
        data = [row.asDict() for row in data]
        table_schema = df.schema
    except Exception as e:
        raise Exception(f"Failed to load data from table '{table_name}': {str(e)}")  # noqa: B904

    return data, table_schema


def save_to_databricks(changed_rows_id: list, table_name: str, row_data: list[dict]) -> None:
    """Save data to a Databricks table with automatic schema matching, updating only changed cells.

    Args:
        changed_rows_id (list): Unique identifier of the cell changed via user input.
        table_name (str): The name of the Databricks table where the data will be saved.
        row_data (list[dict]): The data in the ag grid.

    Returns:
        None: This function does not return a value.

    Raises:
        Exception: If there is an error during the data saving process.

    """
    try:
        # Dynamisch die eindeutigen Schlüssel abrufen
        unique_keys = get_primary_keys(table_name)
        if not unique_keys:
            raise ValueError(f"❌ Keine Primärschlüssel gefunden für {table_name}.")

        row_data = [row_data[index] for index in changed_rows_id]
        changed_data = spark.createDataFrame(row_data, schema=spark.table(table_name).schema)

        # Construct the merge condition using the unique keys
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in unique_keys])

        # Load the target table
        target_df = DeltaTable.forName(spark, table_name)

        # Get the list of columns to update (excluding the unique keys)
        columns_to_update = [col for col in changed_data.columns if col not in unique_keys]

        # Execute the merge operation, updating only the changed columns
        (
            target_df.alias("target")
            .merge(changed_data.alias("source"), merge_condition)
            .whenMatchedUpdate(set={col: f"source.{col}" for col in columns_to_update})
            .whenNotMatchedInsertAll()
            .execute()
        )
    except Exception as e:
        raise Exception(f"Failed to save data to Databricks table '{table_name}': {str(e)}")  # noqa: B904

    return None


# Callback to load data based on the table name entered by the user (triggered by button click or Enter)
@app.callback(
    [Output("table-grid", "rowData"), Output("table-grid", "columnDefs"), Output("loading-output-1", "children")],
    [Input("load-data-button", "n_clicks"), Input("table-name-input", "n_submit")],
    State("table-name-input", "value"),
    prevent_initial_call=True,
)
def load_data_callback(n_clicks: int, n_submit: int, table_name: str) -> tuple[list[dict], list[dict]]:
    """Load data dynamically based on user interaction.

    Args:
        n_clicks (int): Number of times the load button has been clicked.
        n_submit (int): Number of times the Enter key has been pressed in an input field.
        table_name (str): The name of the table to load data from.

    Returns:
        tuple: A tuple containing:
            - list[dict]: The loaded data as a list of dictionaries.
            - list[dict]: Column definitions for the frontend table.

    Raises:
        Exception: If an error occurs during data loading.

    """
    try:
        if (n_clicks > 0 or n_submit > 0) and table_name:
            data, table_schema = load_data(table_name)

            if data:
                columnDefs = generate_column_defs(data, table_schema, table_name)  # noqa: N806

                return data, columnDefs, "Data successfully loaded"
            else:
                return [], [], "no Table name provided or button clicked"

    except Exception as e:
        print(f"Error loading data: {str(e)}")
        return [], [], f"Error loading data: {str(e)}"

    return [], [], "no table was selected"


def get_column_types(table_schema: pyspark.sql.types.StructType) -> list[dict]:
    """Map schema target table to Dash AG-Grid column types.

    Args:
        table_schema (pyspark.sql.types.StructType): The schema of the target table.

    Returns:
        list[dict]: A list of dictionaries mapping column names to Dash AG-Grid column types.

    """
    type_mapping = {
        "IntegerType": "numericColumn",
        "LongType": "numericColumn",
        "FloatType": "numericColumn",
        "DoubleType": "numericColumn",
        "DecimalType": "numericColumn",
        "StringType": "textColumn",
        "BooleanType": "booleanColumn",
        "DateType": "dateColumn",
        "TimestampType": "dateColumn",
    }

    # Create a list of dictionaries that map each column name to its AG-Grid type
    column_types = [
        {"column": field.name, "type": type_mapping.get(type(field.dataType).__name__, "textColumn")}
        for field in table_schema
    ]

    return column_types


def generate_column_defs(data: list[dict], table_schema: pyspark.sql.types.StructType, table_name: str) -> list:
    """Generate column definitions with appropriate data types for Dash AG-Grid.

    Args:
        data (list): A list of dictionaries representing rows of data from the target table.
        table_schema (pyspark.sql.types.StructType): The schema of the table.
        table_name (str): The name of the target table.

    Returns:
        list: A list of dictionaries representing column definitions for Dash AG-Grid.

    """
    # Get column types based on the schema
    column_types = get_column_types(table_schema)

    unique_keys = get_primary_keys(table_name) or []

    # Generate column definitions dynamically
    columnDefs = [  # noqa: N806
        {
            "headerName": col["column"],
            "field": col["column"],
            "editable": True not in unique_keys,
            "type": col["type"],
            "filter": True,
            "cellStyle": {"backgroundColor": "rgba(241, 137, 0, 0.2)"} if col["column"] in unique_keys else {},
        }
        for col in column_types
    ]
    return columnDefs


def get_primary_keys(table_name: str) -> list:
    """Extract primary keys from target table using databricks system information.

    Args:
        table_name (str): The name of the target table.

    Returns:
        list: A list of dictionaries representing the primary keys found.

    """
    catalog, schema, table = table_name.split(".")
    key_table = "advancinglake.default.databricks_plan_config_table"
    key_column = "column_name"
    df_keys = (
        spark.table(key_table)
        .filter(col("table_catalog") == catalog)  # Filter für Catalog
        .filter(col("table_schema") == schema)  # Filter für Schema
        .filter(col("table_name") == table)  # FIlter für table_name
        .select(key_column)
    )
    key_list = [row[key_column] for row in df_keys.collect()]
    return key_list


# Callback for saving changes made by the user in the dcc.store object
@app.callback(
    Output("changed-rows-store", "data"),
    Input("table-grid", "cellValueChanged"),
    State("changed-rows-store", "data"),
    prevent_initial_call=True,
)
def track_changes(changed_cells: list, changed_rows_id: list) -> list:
    """Speichert geänderte Zeilen.

    Args:
        changed_cells (list): list containing row data of changed row.
        changed_rows_id (list): Unique identifier of the cell changed via user input.

    Returns:
        changed_rows_id (list): list containing Ids of changed rows.

    """
    if not changed_rows_id:
        changed_rows_id = []

    if changed_cells:
        changed_rows_id.extend([int(entry.get("rowId")) for entry in changed_cells])
        print(changed_rows_id)
    return changed_rows_id


# Callback for saving the changes
@app.callback(
    Output("loading-output-2", "children"),
    Input("Saving-button", "n_clicks"),
    State("changed-rows-store", "data"),
    State("table-name-input", "value"),
    State("table-grid", "rowData"),
    prevent_initial_call=True,
)
def save_data(n_clicks: int, changed_rows_id: list, table_name: str, row_data: list[dict]) -> str:
    """Save the provided row data to Databricks after converting date strings to date objects.

    Args:
        n_clicks (int): Number of clicks indicating the action to save data.
        changed_rows_id (list): List containing the identifier of the data to be saved.
        table_name (str): Name of the table where data will be saved.
        row_data (list[dict]): The data in the ag grid.

    Returns:
        str: A success message if data is saved, an error message otherwise.

    Raises:
        ValueError: If there is an issue converting DateDATE to a date object.
        Exception: If any other error occurs during the saving process.

    """
    if n_clicks > 0:
        try:
            # Convert the DateDATE column back to date format
            for row in row_data:
                if "DateDATE" in row:
                    try:
                        # Convert the string to a datetime object
                        date_str = row["DateDATE"]
                        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
                        row["DateDATE"] = date_obj
                    except ValueError as e:
                        raise ValueError(f"Error converting DateDATE to date: {str(e)}")  # noqa: B904

            save_to_databricks(changed_rows_id, table_name, row_data)
            return "Data was successfully saved!"
        except Exception as e:
            return f"Error with saving data: {str(e)}"

    return "No Changes to be saved."
