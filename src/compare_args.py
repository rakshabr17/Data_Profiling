import argparse
import json

def parse_stats_file(file_path):
    """
    Parse the stats file and convert it to a dictionary of stats.
    """
    stats = {}
    current_table = None
    current_column = None

    with open(file_path, 'r') as f:
        lines = f.readlines()

    for line in lines:
        line = line.strip()
        if not line:
            continue

        if line.startswith("Table Name:"):
            current_table = line.split(":", 1)[1].strip()
            stats[current_table] = {"TableStats": {}, "ColumnStats": {}}
            current_column = None

        elif line.startswith("Column:"):
            current_column = line.split(":", 1)[1].strip()
            if current_table:
                stats[current_table]["ColumnStats"][current_column] = {}

        elif ":" in line:
            key, value = map(str.strip, line.split(":", 1))
            if value.isdigit():
                value = int(value)
            elif value.replace('.', '', 1).isdigit():
                value = float(value)

            if current_table and not current_column:
                stats[current_table]["TableStats"][key] = value
            elif current_table and current_column:
                stats[current_table]["ColumnStats"][current_column][key] = value

    return stats

def compare_stats(stats1, stats2):
    """
    Compare the statistics of two databases.
    """
    differences = {}

    for table, table_stats1 in stats1.items():
        table_stats2 = stats2.get(table, {"TableStats": {}, "ColumnStats": {}})

        for key, value1 in table_stats1.get("TableStats", {}).items():
            value2 = table_stats2.get("TableStats", {}).get(key)
            if value1 != value2:
                if table not in differences:
                    differences[table] = {"TableStats": {}, "ColumnStats": {}}
                differences[table]["TableStats"][key] = {
                    "Database1": value1,
                    "Database2": value2
                }

        for column, column_stats1 in table_stats1.get("ColumnStats", {}).items():
            column_stats2 = table_stats2.get("ColumnStats", {}).get(column, {})
            for key, value1 in column_stats1.items():
                value2 = column_stats2.get(key)
                if value1 != value2:
                    if table not in differences:
                        differences[table] = {"TableStats": {}, "ColumnStats": {}}
                    if column not in differences[table]["ColumnStats"]:
                        differences[table]["ColumnStats"][column] = {}
                    differences[table]["ColumnStats"][column][key] = {
                        "Database1": value1,
                        "Database2": value2
                    }

    return differences

def write_differences_to_file(differences, output_file_path):
    """
    Write the differences to an output file in a structured format.
    """
    with open(output_file_path, 'w') as f:
        for table, table_diff in differences.items():
            f.write(f"Table Name: {table}\n")

            if "TableStats" in table_diff and table_diff["TableStats"]:
                f.write("  Table-Level Stats:\n")
                for key, diff in table_diff["TableStats"].items():
                    f.write(f"    {key}:\n")
                    f.write(f"      Database1: {diff['Database1']}\n")
                    f.write(f"      Database2: {diff['Database2']}\n")

            if "ColumnStats" in table_diff and table_diff["ColumnStats"]:
                f.write("  Column-Level Stats:\n")
                for column, column_diff in table_diff["ColumnStats"].items():
                    f.write(f"    Column: {column}\n")
                    for key, diff in column_diff.items():
                        f.write(f"      {key}:\n")
                        f.write(f"        Database1: {diff['Database1']}\n")
                        f.write(f"        Database2: {diff['Database2']}\n")

            f.write("\n")

def main():
    parser = argparse.ArgumentParser(description="Compare statistics of two databases.")
    parser.add_argument("--stats_file1", required=True, help="Path to the first stats file.")
    parser.add_argument("--stats_file2", required=True, help="Path to the second stats file.")
    parser.add_argument("--output_file", required=True, help="Path to the output differences file.")
    args = parser.parse_args()

    stats1 = parse_stats_file(args.stats_file1)
    stats2 = parse_stats_file(args.stats_file2)

    differences = compare_stats(stats1, stats2)
    write_differences_to_file(differences, args.output_file)

if __name__ == "__main__":
    main()