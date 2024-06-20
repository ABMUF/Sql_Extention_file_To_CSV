import os
import re
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import time

def parse_sql_insert(sql_insert):
    if not re.match(r"(?i)^INSERT INTO", sql_insert):
        sql_insert = "INSERT INTO " + sql_insert

    table_name_match = re.search(r"(?i)INSERT INTO\s+`?([^`\s]+)`?\s*\(", sql_insert)
    if table_name_match:
        table_name = table_name_match.group(1)
    else:
        raise ValueError("Table name not found in SQL statement.")

    column_names_match = re.search(r"\(([^)]+)\)\s*VALUES", sql_insert)
    if column_names_match:
        column_names = column_names_match.group(1).replace('`', '').split(',')
        column_names = [name.strip() for name in column_names]
    else:
        raise ValueError("Column names not found in SQL statement.")

    values_match = re.findall(r"\(([^)]+)\)", sql_insert.split("VALUES", 1)[1])
    rows = []
    for value_group in values_match:
        values = re.split(r",(?=(?:[^']*'[^']*')*[^']*$)", value_group)  # Handles commas within quotes
        values = [v.strip().strip("'") for v in values]
        rows.append(values)

    if not rows:
        raise ValueError("No rows found in SQL statement.")

    if len(column_names) != len(rows[0]):
        raise ValueError(f"Column count does not match value count in statement: {sql_insert}")

    df = pd.DataFrame(rows, columns=column_names)
    return table_name, df

def write_to_csv(all_dfs, sql_file, folder_path):
    sql_filename = os.path.splitext(os.path.basename(sql_file))[0]

    for table_name, df in all_dfs.items():
        csv_filename = os.path.join(folder_path, f"{sql_filename}_{table_name}.csv")
        if os.path.exists(csv_filename):
            df.to_csv(csv_filename, mode='a', header=False, index=False)
        else:
            df.to_csv(csv_filename, mode='w', header=True, index=False)
        print(f"Saved {len(df)} rows from {table_name} table to {csv_filename}")

def extract_id(statement):
    id_match = re.search(r"VALUES\s*\(\s*([^,]+)", statement)
    if id_match:
        return id_match.group(1).strip().strip("'")
    return None

def process_sql_file(sql_file, folder_path):
    with open(sql_file, 'r', encoding='utf-8') as file:
        sql_content = file.read()

    all_dfs = {}
    insert_statements = re.split(r"(?i)\);?\s*INSERT INTO", sql_content)
    insert_statements = ["INSERT INTO" + statement for statement in insert_statements if statement.strip()]

    total_statements = len(insert_statements)
    batch_size = 1000
    batch_counter = 0

    for i, statement in enumerate(insert_statements):
        try:
            table_name, df = parse_sql_insert(statement)
            if table_name in all_dfs:
                all_dfs[table_name] = pd.concat([all_dfs[table_name], df], ignore_index=True)
            else:
                all_dfs[table_name] = df

            batch_counter += 1

            if batch_counter >= batch_size:
                write_to_csv(all_dfs, sql_file, folder_path)
                all_dfs.clear()
                batch_counter = 0

        except Exception as e:
            print(f"Error processing statement {i+1} in file {sql_file}: {str(e)}")
            with open(os.path.join(folder_path, "errors.log"), "a", encoding='utf-8') as error_file:
                error_file.write(f"Error processing statement {i+1} in file {sql_file}:\n{statement}\nError: {e}\n\n")
            if i > 0 and i < len(insert_statements) - 1:
                prev_statement = insert_statements[i - 1]
                next_statement = insert_statements[i + 1]

                prev_id = extract_id(prev_statement)
                next_id = extract_id(next_statement)

                print(f"ID before error: {prev_id}")
                print(f"ID after error: {next_id}")

        print(f"Processed {i+1}/{total_statements} statements ({(i+1)/total_statements*100:.2f}%) in file {sql_file}")

    if all_dfs:
        write_to_csv(all_dfs, sql_file, folder_path)

def process_sql_files_in_parallel(folder_path, num_workers=4):
    sql_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.sql')]

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(process_sql_file, sql_file, folder_path) for sql_file in sql_files]

        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing SQL files"):
            try:
                future.result()
            except Exception as e:
                print(f"Error processing file in parallel: {str(e)}")

if __name__ == "__main__":
    folder_path = "SQL/"
    start_time = time.time()
    process_sql_files_in_parallel(folder_path, num_workers=os.cpu_count())
    end_time = time.time()
    print(f"Finished processing all SQL files in {end_time - start_time:.2f} seconds")
