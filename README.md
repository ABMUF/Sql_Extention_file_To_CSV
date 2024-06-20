
# SQL to CSV Converter

This repository contains a Python script to convert SQL `INSERT INTO` statements into CSV files. The script processes SQL files in parallel, utilizing multiple CPU cores for efficient data handling, and outputs the resulting data into corresponding CSV files.

## Features

- **Parallel Processing**: Utilizes all available CPU cores to process SQL files concurrently.
- **Batch Processing**: Processes data in batches to handle large files efficiently.
- **Error Handling**: Logs errors encountered during processing to an `errors.log` file.
- **Comprehensive Progress Reporting**: Provides detailed progress updates during processing.
- **Flexible Parsing**: Handles complex SQL insert statements, including those with commas within quoted values.

## Prerequisites

- Python 3.x
- Pandas library
- tqdm library

Install the required libraries using pip:

```sh
pip install pandas tqdm
```

## Usage

1. **Place SQL Files**: Place your `.sql` files containing `INSERT INTO` statements into the `SQL/` directory.

2. **Run the Script**: Execute the script to process the SQL files and generate corresponding CSV files.

```sh
python sql_to_csv.py
```

## Script Overview

### Functions

- **parse_sql_insert(sql_insert)**: Parses a single SQL `INSERT INTO` statement and returns a DataFrame.
- **write_to_csv(all_dfs, sql_file, folder_path)**: Writes the accumulated DataFrames to CSV files.
- **extract_id(statement)**: Extracts the first ID from a SQL statement.
- **process_sql_file(sql_file, folder_path)**: Processes an SQL file, splitting it into batches and converting each to CSV.
- **process_sql_files_in_parallel(folder_path, num_workers=4)**: Processes multiple SQL files in parallel.

### Process Flow

1. **Read SQL File**: The script reads the content of each SQL file in the `SQL/` directory.
2. **Split Statements**: It splits the content into individual `INSERT INTO` statements.
3. **Parse Statements**: Each statement is parsed to extract table names, column names, and values.
4. **Batch Processing**: The data is processed in batches, and DataFrames are accumulated.
5. **Write to CSV**: The accumulated DataFrames are written to CSV files.
6. **Error Logging**: Any errors encountered during parsing or writing are logged to `errors.log`.

## Example

Consider an example SQL file `example.sql` with the following content:

```sql
INSERT INTO `users` (`id`, `name`, `email`) VALUES
(1, 'John Doe', 'john@example.com'),
(2, 'Jane Doe', 'jane@example.com');
```

Running the script will generate a CSV file `SQL/example_users.csv` with the following content:

```csv
id,name,email
1,John Doe,john@example.com
2,Jane Doe,jane@example.com
```

## Error Handling

Any errors encountered during the processing of SQL statements are logged in the `errors.log` file in the output directory. The log includes details of the error and the specific SQL statement that caused it.

## Performance Optimization

- **Parallel Processing**: The script uses the `concurrent.futures.ProcessPoolExecutor` for parallel processing, ensuring efficient use of CPU resources.
- **Batch Processing**: Data is processed in configurable batch sizes to handle large SQL files without overwhelming memory.

## Contribution

Feel free to contribute to this project by opening issues or submitting pull requests. Ensure that your code adheres to the project's coding standards and is well-documented.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact

For any questions or suggestions, please open an issue or contact the project maintainer.

a.almoufti.a@gmail.com

Enjoy converting your SQL files to CSV!
