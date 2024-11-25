import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.tag import pos_tag
from typing import Dict, List, Tuple, Any, Optional, Union
import re
import logging

# Setup logging properly
logger = logging.getLogger(__name__)

class QueryGenerator:
    def __init__(self):
        # Download required NLTK data
        for package in ['punkt', 'averaged_perceptron_tagger', 'stopwords', 'wordnet']:
            try:
                nltk.download(package, quiet=True)
            except Exception as e:
                logging.warning(f"Failed to download {package}: {str(e)}")

        self.stop_words = set(stopwords.words('english'))
        
        # Expanded keyword mappings for SQL components
        self.KEYWORDS = {
            'select': {
                'words': ['show', 'display', 'get', 'find', 'search', 'list', 'what', 'give', 'fetch', 'retrieve', 'select'],
                'aggregates': {
                    'count': ['count', 'number of', 'how many', 'total number'],
                    'sum': ['sum', 'total', 'add up'],
                    'avg': ['average', 'mean', 'typical'],
                    'max': ['maximum', 'highest', 'most', 'top', 'largest'],
                    'min': ['minimum', 'lowest', 'least', 'smallest'],
                }
            },
            'from': ['from', 'in', 'within', 'of'],
            'where': {
                'words': ['where', 'with', 'has', 'have', 'having', 'that', 'whose', 'which'],
                'operators': {
                    '=': ['equals', 'equal to', 'is', 'matches', 'same as'],
                    '>': ['greater than', 'more than', 'above', 'over', 'exceeds'],
                    '<': ['less than', 'lower than', 'below', 'under'],
                    '>=': ['at least', 'greater than or equal to', 'minimum'],
                    '<=': ['at most', 'less than or equal to', 'maximum'],
                    '!=': ['not equal to', 'different from', 'not'],
                    'LIKE': ['like', 'contains', 'similar to', 'starts with', 'ends with'],
                    'IN': ['in', 'among', 'within', 'one of'],
                    'BETWEEN': ['between', 'from', 'range']
                }
            },
            'group_by': ['group by', 'grouped by', 'organize by', 'categorize by', 'per', 'by each'],
            'order_by': ['order by', 'ordered by', 'sort by', 'sorted by', 'arrange by', 'arranged by'],
            'direction': {
                'ASC': ['ascending', 'asc', 'increasing'],
                'DESC': ['descending', 'desc', 'decreasing', 'reverse']
            }
        }

    def _extract_conditions(self, text: str) -> List[Tuple[str, str, str]]:
        """
        Extract WHERE conditions with improved natural language understanding.
        """
        conditions = []
        text = text.lower()
        
        # Natural language patterns for conditions
        patterns = [
            # Which/whose patterns
            (r'(?:which|whose|with|having)\s+(\w+)\s+(?:is|are)\s+(?:greater|more|higher|larger)\s+than\s+(\d+(?:\.\d+)?)', '>'),
            (r'(?:which|whose|with|having)\s+(\w+)\s+(?:is|are)\s+(?:less|lower|smaller)\s+than\s+(\d+(?:\.\d+)?)', '<'),
            (r'(?:which|whose|with|having)\s+(\w+)\s+(?:is|are)\s+at\s+least\s+(\d+(?:\.\d+)?)', '>='),
            (r'(?:which|whose|with|having)\s+(\w+)\s+(?:is|are)\s+at\s+most\s+(\d+(?:\.\d+)?)', '<='),
            
            # Equality patterns
            (r'(?:which|whose|with|having)\s+(\w+)\s+(?:is|are)\s+(?:equal\s+to|exactly)?\s+[\'"]*([^\'"\s]+)[\'"]*', '='),
            
            # Not equal patterns
            (r'(?:which|whose|with|having)\s+(\w+)\s+(?:is|are)\s+not\s+[\'"]*([^\'"\s]+)[\'"]*', '!='),
            
            # Traditional WHERE patterns (as fallback)
            (r'where\s+(\w+)\s*(>|<|>=|<=|=|!=)\s*(\d+(?:\.\d+)?)', None),
            (r'where\s+(\w+)\s+equals?\s+[\'"]*([^\'"\s]+)[\'"]*', '=')
        ]
        
        # Check each pattern
        for pattern, default_operator in patterns:
            matches = re.finditer(pattern, text)
            for match in matches:
                if default_operator is None:  # For patterns that include operator
                    field, operator, value = match.groups()
                else:
                    field, value = match.groups()
                    operator = default_operator
                    
                # Clean up the value
                value = value.strip("'\"")
                
                # Avoid duplicate conditions
                if (field, operator, value) not in conditions:
                    conditions.append((field, operator, value))
        
        return conditions

    def _extract_aggregate_functions(self, text: str) -> List[Tuple[str, str, str]]:
        """
        Extract aggregate functions with improved natural language understanding.
        Returns list of (function, field, alias) tuples.
        """
        aggregates = []
        text = text.lower()
        
        # Natural language patterns for aggregates
        patterns = [
            # Count patterns
            (r'(?:count|number|how\s+many)\s+(?:of\s+)?(\w+)', 'COUNT'),
            
            # Sum patterns
            (r'(?:sum|total|add\s+up)\s+(?:of\s+)?(\w+)', 'SUM'),
            
            # Average patterns
            (r'(?:average|mean|avg)\s+(?:of\s+)?(\w+)', 'AVG'),
            
            # Max/Min patterns
            (r'(?:maximum|highest|max|biggest|largest)\s+(?:of\s+)?(\w+)', 'MAX'),
            (r'(?:minimum|lowest|min|smallest)\s+(?:of\s+)?(\w+)', 'MIN')
        ]
        
        for pattern, func in patterns:
            matches = re.finditer(pattern, text)
            for match in matches:
                field = match.group(1)
                alias = f"{func.lower()}_{field}"
                aggregates.append((func, field, alias))
                
        return aggregates

    def _extract_table_name(self, text: str, available_tables: List[str]) -> str:
        """Find table name in text and verify against available tables."""
        text = text.lower()
        tables_lower = [t.lower() for t in available_tables]
        
        # Direct match in available tables
        for table in available_tables:
            if table.lower() in text:
                return table
        
        # Look for table name after FROM/IN
        for keyword in ['from', 'in']:
            pattern = f"{keyword}\\s+([\\w_]+)"
            match = re.search(pattern, text)
            if match and match.group(1).lower() in tables_lower:
                return match.group(1)
        
        # If no match found, return None
        return None

    def _extract_order_by(self, text: str) -> Optional[Tuple[str, str]]:
        """Extract ORDER BY clause with improved handling."""
        text = text.lower()
        
        # Check for ordering keywords with direction
        order_pattern = r'(?:order|sort|arrange)(?:ed)?\s+by\s+([\w\s]+?)(?:\s+(desc|asc|descending|ascending))?(?:\s|$)'
        match = re.search(order_pattern, text)
        
        if match:
            field = match.group(1).strip()
            direction = 'DESC' if match.group(2) and match.group(2).startswith('desc') else 'ASC'
            return (field, direction)
            
        return None

    def extract_query_components(self, text: str, available_tables: List[str]) -> Dict[str, Any]:
        """Extract all query components with improved natural language processing."""
        text = self._clean_text(text)
        components = {
            'select': ['*'],
            'from': None,
            'where': [],
            'group_by': [],
            'order_by': None,
            'limit': None,
            'aggregates': []
        }

        # Extract table name
        components['from'] = self._extract_table_name(text, available_tables)

        # Handle aggregate functions
        aggregates = self._extract_aggregate_functions(text)
        if aggregates:
            components['aggregates'] = aggregates
            # If we have aggregates, don't select all columns
            if components['select'] == ['*']:
                components['select'] = []

        # Handle column selection
        select_pattern = r'(?:show|get|find|display|select)\s+([\w\s,]+)\s+(?:from|in)\s+(\w+)'
        select_match = re.search(select_pattern, text, re.IGNORECASE)
        
        if select_match and not aggregates:  # Only if no aggregates found
            columns_str = select_match.group(1)
            columns = []
            
            # Split by comma and 'and'
            for part in columns_str.split(','):
                if ' and ' in part:
                    columns.extend([col.strip() for col in part.split(' and ')])
                else:
                    columns.append(part.strip())
                    
            # Clean up columns
            columns = [col for col in columns if col and col != '*']
            if columns:
                components['select'] = columns

        # Extract WHERE conditions
        conditions = self._extract_conditions(text)
        if conditions:
            components['where'] = conditions

        # Handle GROUP BY
        group_patterns = [
            r'group(?:ed)?\s+by\s+([\w\s,]+)',
            r'by\s+each\s+([\w\s,]+)',
            r'per\s+([\w\s,]+)'
        ]
        
        for pattern in group_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                groups = [g.strip() for g in match.group(1).split(',')]
                components['group_by'] = groups
                break

        # Handle ORDER BY
        order_by = self._extract_order_by(text)
        if order_by:
            components['order_by'] = order_by

        # Handle LIMIT/TOP N
        top_n_pattern = r'(?:top|first|last)\s+(\d+)(?:\s+(?:rows?|records?|results?|entries?|lines?))?'
        top_n_match = re.search(top_n_pattern, text, re.IGNORECASE)
        if top_n_match:
            components['limit'] = int(top_n_match.group(1))

        return components

    def generate_sql_query(self, components: Dict[str, Any]) -> str:
        """Generate SQL query from components."""
        # Handle SELECT clause with aggregates
        select_parts = []
        
        # Add aggregate functions
        for func, field, alias in components.get('aggregates', []):
            select_parts.append(f"{func}({field}) AS {alias}")
        
        # Add normal columns
        if components['select'] and components['select'] != ['*']:
            select_parts.extend(self._clean_column_name(col) for col in components['select'])
        
        # If no columns specified and no aggregates, select all
        if not select_parts:
            select_clause = "SELECT *"
        else:
            select_clause = f"SELECT {', '.join(select_parts)}"

        # Handle FROM clause
        from_clause = f"FROM {components['from']}" if components['from'] else ""

        # Handle WHERE clause
        where_clause = ""
        if components['where']:
            conditions = []
            for field, operator, value in components['where']:
                if isinstance(value, (int, float)) or value.isdigit():
                    conditions.append(f"{self._clean_column_name(field)} {operator} {value}")
                else:
                    conditions.append(f"{self._clean_column_name(field)} {operator} '{value}'")
            where_clause = "WHERE " + " AND ".join(conditions)

        # Handle GROUP BY clause
        group_clause = ""
        if components['group_by']:
            group_clause = "GROUP BY " + ", ".join(self._clean_column_name(field) for field in components['group_by'])

        # Handle ORDER BY clause
        order_clause = ""
        if components['order_by']:
            field, direction = components['order_by']
            order_clause = f"ORDER BY {self._clean_column_name(field)} {direction}"

        # Handle LIMIT clause
        limit_clause = f"LIMIT {components['limit']}" if components['limit'] is not None else ""

        # Combine all clauses
        query_parts = [
            select_clause,
            from_clause,
            where_clause,
            group_clause,
            order_clause,
            limit_clause
        ]

        # Join non-empty clauses with spaces
        query = " ".join(part for part in query_parts if part)
        return query.strip()

    def _clean_text(self, text: str) -> str:
        """Clean and normalize input text."""
        return text.lower().strip()

    def _clean_column_name(self, column_name: str) -> str:
        """Clean and format column names."""
        column_name = column_name.strip()
        if ' ' in column_name:
            return f"`{column_name}`"
        return column_name
    