from decimal import Decimal
from typing import Dict, List, Any
from datetime import datetime, date


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

def format_table(results: List[Dict[str, Any]]) -> str:
    """Format results as an ASCII table."""
    if not results:
        return "Empty set"
    
    # Get column names from first row
    columns = list(results[0].keys())
    
    # Get maximum width for each column
    col_widths = {col: len(col) for col in columns}
    for row in results:
        for col in columns:
            width = len(format_value(row[col]))
            col_widths[col] = max(col_widths[col], width)
    
    # Create the table header
    header = "+"
    for col in columns:
        header += "-" * (col_widths[col] + 2) + "+"
    
    column_names = "|"
    for col in columns:
        column_names += f" {col}{' ' * (col_widths[col] - len(col))} |"
    
    # Create the table body
    rows = []
    for row in results:
        row_str = "|"
        for col in columns:
            value = format_value(row[col])
            row_str += f" {value}{' ' * (col_widths[col] - len(value))} |"
        rows.append(row_str)
    
    # Combine all parts
    table = [
        header,
        column_names,
        header,
        *rows,
        header
    ]
    
    # Add result count
    table.append(f"\n{len(results)} rows in set\n")
    
    return "\n".join(table)