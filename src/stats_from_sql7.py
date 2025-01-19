import os
import sys
import json
import logging
import argparse
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

# Configure logging with timestamped log files
date_now = datetime.now().strftime("%d-%m-%Y")
logging.basicConfig(
    filename=f"Log_Files/Log_Info_{date_now}.log",
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.DEBUG,
    filemode='w',
    force=True
)
logger = logging.getLogger()

# Function to load configuration
def load_config(config_file):
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file '{config_file}' not found.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format in configuration file. Details: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error loading config file: {e}")
        sys.exit(1)

# Function to connect to SQL Server
def connect_to_sql_server(connection_string):
    try:
        logger.info(f"Connecting with connection string: {connection_string}")
        engine = create_engine(connection_string)
        connection = engine.connect()
        logger.info("Connected to SQL Server successfully!")
        return connection
    except Exception as e:
        logger.error(f"Error connecting to SQL Server: {e}")
        return None

# Function to fetch statistics for specified columns
def fetch_stats_from_sql(table_name, columns, conn, schema="dbo"):
    stats = {}
    try:
        row_count_query = f"SELECT COUNT(*) AS row_count FROM {schema}.{table_name}"
        try:
            row_count = pd.read_sql(row_count_query, conn).iloc[0]["row_count"]
            stats["row_count"] = row_count
        except Exception as e:
            logger.error(f"Error fetching row count for table '{table_name}': {e}")
            stats["row_count"] = "Error"

        for column in columns:
            col_name = column["ColumnName"]
            data_type = column["DataType"].lower()
            get_stats = column.get("GetStats", "no").strip().lower()

            if get_stats != "yes":
                continue

            try:
                if data_type in ["integer", "float", "decimal"]:
                    query = f"""
                    SELECT
                        MIN({col_name}) AS min,
                        MAX({col_name}) AS max,
                        SUM({col_name}) AS sum,
                        AVG({col_name}) AS avg
                    FROM {schema}.{table_name}
                    """
                elif data_type in ["nvarchar", "varchar"]:
                    query = f"""
                    SELECT
                        {col_name} AS value,
                        COUNT(*) AS frequency
                    FROM {schema}.{table_name}
                    GROUP BY {col_name}
                    ORDER BY frequency DESC
                    """
                elif data_type == "bit":
                    query = f"""
                    SELECT
                        {col_name} AS value,
                        COUNT(*) AS frequency
                    FROM {schema}.{table_name}
                    GROUP BY {col_name}
                    """
                elif data_type == "datetime":
                    query = f"""
                    SELECT
                        MIN({col_name}) AS min_date,
                        MAX({col_name}) AS max_date
                    FROM {schema}.{table_name}
                    """
                else:
                    logger.warning(f"Unsupported data type '{data_type}' for column '{col_name}'. Skipping...")
                    continue

                df = pd.read_sql(query, conn)
                if not df.empty:
                    if data_type in ["nvarchar", "varchar", "bit"]:
                        frequencies = [f"{row['value']} ({row['frequency']})" for _, row in df.iterrows()]
                        stats[col_name] = {
                            "distinct_count": len(frequencies),
                            "frequency": frequencies
                        }
                    else:
                        stats[col_name] = df.to_dict(orient="records")
            except Exception as e:
                logger.error(f"Error executing query for column '{col_name}' in table '{table_name}': {e}")
    except Exception as e:
        logger.error(f"Error fetching stats for table '{table_name}': {e}")
    return stats

# Function to write statistics to a .txt file
def write_stats_to_file(stats, file_path):
    try:
        logger.info(f"Writing stats to file: {file_path}")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as f:
            for table, table_stats in stats.items():
                f.write(f"Table Name: {table}\n")
                f.write(f"  Total Row Count: {table_stats.get('row_count', 'N/A')}\n")
                for column, column_stats in table_stats.items():
                    if column == "row_count":
                        continue
                    f.write(f"\nColumn: {column}\n")
                    if isinstance(column_stats, dict) and "frequency" in column_stats:
                        f.write(f"  distinct_count: {column_stats['distinct_count']}\n")
                        f.write("  frequency:\n")
                        for freq in column_stats["frequency"]:
                            value, count = freq.split(" (")
                            count = count.rstrip(")")
                            f.write(f"    {value}: {count}\n")
                    elif isinstance(column_stats, list):
                        for record in column_stats:
                            for key, value in record.items():
                                f.write(f"  {key}: {value}\n")
                f.write("\n")
    except Exception as e:
        logger.error(f"Error writing to file: {e}")

# Function to process a single database
def process_database(database_name, database_config, output_dir):
    connection_string = database_config["connection_string"]
    tables = database_config["tables"]

    conn = connect_to_sql_server(connection_string)
    if conn:
        stats = {}
        for table_name, table_config in tables.items():
            schema = table_config.get("Schema", "dbo")
            columns = table_config["columns"]

            table_stats = fetch_stats_from_sql(table_name, columns, conn, schema)
            if table_stats:
                stats[table_name] = table_stats

        output_file = os.path.join(output_dir, f"{database_name}_from_sql_stats.txt")
        write_stats_to_file(stats, output_file)
        conn.close()
    else:
        logger.error(f"Failed to process database: {database_name}")

# Main function
def main():
    parser = argparse.ArgumentParser(description="Process statistics for a specific database.")
    parser.add_argument("database_name", help="Name of the database to process (e.g., 'Database1' or 'Database2')")
    parser.add_argument("--config", default="configuration3.json", help="Path to the configuration file")
    args = parser.parse_args()

    config_file_path = args.config
    config = load_config(config_file_path)

    output_dir = config.get("output_directory", "./output")
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    database_name = args.database_name
    if database_name not in config:
        logger.error(f"Database '{database_name}' not found in configuration.")
        sys.exit(1)

    process_database(database_name, config[database_name], output_dir)

if __name__ == "__main__":
    main()
