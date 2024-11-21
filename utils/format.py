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