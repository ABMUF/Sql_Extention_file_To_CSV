import os
import re
import pandas as pd
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

def parse_sql_insert(statement):
    # Example parsing function; adjust based on your SQL statement structure
    # This function assumes the statement starts with "INSERT INTO"
    table_name = re.search(r"INSERT INTO\s+(\w+)\s+", statement, re.IGNORECASE).group(1)
    values_match = re.search(r"VALUES\s+\((.*)\)", statement, re.IGNORECASE)
    if values_match:
        values = values_match.group(1)
        # Simulated parsing, assuming values are comma-separated
        rows = [tuple(val.strip() for val in values.split(','))]
        column_names = ["col1", "col2", "col3"]  # Example column names
        df = pd.DataFrame(rows, columns=column_names)
        return table_name, df
    else:
        raise ValueError("Could not parse VALUES from SQL statement")

def process_batch(batch_info):
    batch, sql_file, folder_path = batch_info
    all_dfs = {}
    errors = []

    for statement in batch:
        try:
            table_name, df = parse_sql_insert(statement)
            if table_name in all_dfs:
                all_dfs[table_name] = pd.concat([all_dfs[table_name], df], ignore_index=True)
            else:
                all_dfs[table_name] = df
        except Exception as e:
            errors.append(f"Error processing statement:\n{statement}\nError: {e}\n\n")
            continue

    sql_filename = os.path.splitext(os.path.basename(sql_file))[0]

    for table_name, df in all_dfs.items():
        csv_filename = os.path.join(folder_path, f"{sql_filename}_{table_name}.csv")
        try:
            if os.path.exists(csv_filename):
                df.to_csv(csv_filename, mode='a', index=False, header=False, encoding='utf-8')
            else:
                df.to_csv(csv_filename, mode='w', index=False, encoding='utf-8')
            print(f"Saved {len(df)} rows from {table_name} table to {csv_filename}")
        except Exception as e:
            errors.append(f"Error writing to CSV for table {table_name}:\n{e}\n\n")
            continue

    # Log errors
    if errors:
        error_log_file = os.path.join(folder_path, "errors.log")
        with open(error_log_file, "a", encoding='utf-8') as error_file:
            error_file.write("\n".join(errors))

    # Free memory
    del all_dfs
    import gc
    gc.collect()

    return len(batch), len(errors)

def process_sql_file(sql_file, folder_path, batch_size):
    print(f"Processing SQL file: {sql_file}")
    print(f"Output folder: {folder_path}")

    with open(sql_file, 'r', encoding='utf-8') as file:
        sql_content = file.read()

    insert_statements = re.split(r"(?i)\);?\s*INSERT INTO", sql_content)
    insert_statements = ["INSERT INTO" + statement for statement in insert_statements if statement.strip()]

    total_statements = len(insert_statements)
    batches = [insert_statements[i:i + batch_size] for i in range(0, total_statements, batch_size)]

    print(f"Suggested batch size: {batch_size}")
    print(f"Processing {total_statements} statements in {len(batches)} batches...")

    total_processed = 0
    total_errors = 0

    with Pool(processes=cpu_count()) as pool:
        results = list(tqdm(pool.imap_unordered(process_batch, [(batch, sql_file, folder_path) for batch in batches]), total=len(batches), desc="Processing batches"))

    for processed, errors in results:
        total_processed += processed
        total_errors += errors

    print(f"Total statements processed: {total_processed}")
    print(f"Total errors encountered: {total_errors}")

    return total_processed

def main():
    sql_file = "SQL/Sessions.sql"
    folder_path = "output"
    batch_size = 1000

    total_processed = process_sql_file(sql_file, folder_path, batch_size)

    print(f"Processing completed for SQL file: {sql_file}")
    print(f"Total statements processed: {total_processed}")

if __name__ == "__main__":
    main()
