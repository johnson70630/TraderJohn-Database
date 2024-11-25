from decimal import Decimal
from typing import Dict, List, Any
from datetime import datetime, date
import pandas as pd
from itertools import islice

def format_value(value: Any) -> str:
    """Format a value for table display."""
    if value is None:
        return 'NULL'
    elif isinstance(value, (date, datetime)):
        return value.strftime('%Y-%m-%d')
    elif isinstance(value, Decimal):
        return str(float(value))
    elif isinstance(value, bool):
        return str(int(value))
    return str(value)

def format_table_in_chunks(results: List[Dict[str, Any]], chunk_size: int = 5) -> List[str]:
    """Format results as multiple tables with fewer columns."""
    if not results:
        return ["Empty set"]
    
    columns = list(results[0].keys())
    chunks = []
    
    # Split columns into chunks
    for i in range(0, len(columns), chunk_size):
        chunk_columns = columns[i:i + chunk_size]
        
        # Calculate column widths for this chunk
        col_widths = {col: len(col) for col in chunk_columns}
        for row in results:
            for col in chunk_columns:
                width = len(format_value(row[col]))
                col_widths[col] = max(col_widths[col], width)
        
        # Create header
        header = "+"
        for col in chunk_columns:
            header += "-" * (col_widths[col] + 2) + "+"
        
        column_names = "|"
        for col in chunk_columns:
            column_names += f" {col}{' ' * (col_widths[col] - len(col))} |"
        
        # Create rows
        rows = []
        for row in results:
            row_str = "|"
            for col in chunk_columns:
                value = format_value(row[col])
                row_str += f" {value}{' ' * (col_widths[col] - len(value))} |"
            rows.append(row_str)
        
        # Combine chunk
        chunk_table = [
            f"Columns {i+1}-{i+len(chunk_columns)} of {len(columns)}:",
            header,
            column_names,
            header,
            *rows,
            header
        ]
        
        chunks.append("\n".join(chunk_table))
    
    # Add row count to the last chunk
    chunks[-1] += f"\n\n{len(results)} rows in set"
    
    return chunks

def convert_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Automatically detect and convert columns with date-like data to datetime64.
    :param df: Input pandas DataFrame.
    :return: DataFrame with date columns converted.
    """
    for column in df.columns:
        # Try converting the column to datetime
        try:
            df[column] = pd.to_datetime(df[column], errors='ignore')
        except Exception as e:
            print(f"Could not convert column {column} to datetime: {e}")
    return df

def format_nested_fields(fields, indent=2):
    """
    Recursively formats nested fields for display, handling dictionaries and lists.
    :param fields: The fields to format (can be a dict, list, or primitive type).
    :param indent: The indentation level for nested structures.
    :return: A formatted string representation of the fields.
    """
    formatted = ""
    spacer = " " * indent  # Indentation for nested fields

    if isinstance(fields, dict):
        for key, value in fields.items():
            formatted += f"{spacer}• {key}:"
            if isinstance(value, (dict, list)):
                formatted += "\n" + format_nested_fields(value, indent + 2)
            else:
                formatted += f" {value}\n"
    elif isinstance(fields, list):
        for item in fields:
            formatted += f"{spacer}- {format_nested_fields(item, indent + 2)}\n"
    else:
        formatted += f"{fields}"

    return formatted

def infer_mysql_data_type(series: pd.Series) -> str:
    """
    Infers the MySQL data type for a pandas Series.
    :param series: Pandas Series representing a column.
    :return: MySQL data type as a string.
    """
    if pd.api.types.is_integer_dtype(series):
        return "INT"
    elif pd.api.types.is_float_dtype(series):
        return "FLOAT"
    elif pd.api.types.is_datetime64_any_dtype(series):
        return "DATETIME"
    elif pd.api.types.is_string_dtype(series):
        return "VARCHAR(255)"
    else:
        return "TEXT" 

def batch(iterable, n=1):
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            break
        yield batch
