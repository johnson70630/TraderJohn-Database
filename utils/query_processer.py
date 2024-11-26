import json
from typing import List, Dict, Any


def process_mongodb_results(results: List[Dict[str, Any]]) -> List[str]:
    """
    Process MongoDB query results into JSON chunks suitable for sending as replies.

    Args:
        results: List of dictionaries representing MongoDB query results.

    Returns:
        A list of strings, where each string is a JSON-formatted chunk of the results.
    """
    if not results:
        return ["No results found."]
    
    # Prepare JSON-style results
    text_chunks = []
    chunk_size = 10  # Number of documents per chunk

    for i in range(0, len(results), chunk_size):
        chunk = results[i:i + chunk_size]
        json_output = json.dumps(chunk, indent=2, default=str)  # Convert to pretty JSON
        text_chunks.append(f"```json\n{json_output}\n```")
    
    if len(results) > chunk_size:
        text_chunks.append(f"Showing first {chunk_size} of {len(results)} results.")

    return text_chunks
