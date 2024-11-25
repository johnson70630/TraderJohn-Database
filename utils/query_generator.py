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
        
        # Define comparison words
        self.comparisons = {
            'greater': '>', 'more': '>', 'higher': '>', 'larger': '>', 'above': '>',
            'less': '<', 'lower': '<', 'smaller': '<', 'below': '<', 'under': '<',
            'equals': '=', 'is': '=', 'equal': '=', 'same': '=',
            'at least': '>=', 'minimum': '>=', 'not less': '>=',
            'at most': '<=', 'maximum': '<=', 'not more': '<=',
            'not': '!=', 'different': '!='
        }
        
        # Define aggregate functions
        self.aggregates = {
            'count': ['count', 'number', 'how many'],
            'sum': ['sum', 'total', 'add'],
            'avg': ['average', 'mean', 'typical'],
            'max': ['maximum', 'highest', 'most', 'largest'],
            'min': ['minimum', 'lowest', 'least', 'smallest']
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
        """Extract query components using NLTK."""
        tokens = word_tokenize(text.lower())
        tagged = pos_tag(tokens)
        
        components = {
            'select': ['*'],
            'from': None,
            'where': [],
            'group_by': [],
            'order_by': None,
            'aggregates': []
        }
        
        # Extract table name and columns
        table_found = False
        columns = []
        agg_found = False
        current_agg = None
        group_by = False
        order_by = False
        condition_field = None
        
        for i, (word, tag) in enumerate(tagged):
            if word in available_tables:
                components['from'] = word
                table_found = True
                continue
                
            # Skip stop words
            if word in self.stop_words:
                continue
                
            # Handle aggregates
            for agg_type, synonyms in self.aggregates.items():
                if word in synonyms:
                    agg_found = True
                    current_agg = agg_type
                    continue
                    
            # Handle columns and aggregates
            if tag.startswith('NN') and word not in self.stop_words:
                if agg_found:
                    components['aggregates'].append((current_agg, word, f"{current_agg}_{word}"))
                    agg_found = False
                elif group_by:
                    components['group_by'].append(word)
                elif condition_field is None and any(comp in tokens[max(0, i-2):i] for comp in self.comparisons.keys()):
                    condition_field = word
                elif condition_field and word not in self.stop_words:
                    operator = None
                    for comp, op in self.comparisons.items():
                        if comp in ' '.join(tokens[max(0, i-3):i]):
                            operator = op
                            break
                    if operator:
                        components['where'].append((condition_field, operator, word))
                        condition_field = None
                else:
                    columns.append(word)
            
            # Handle GROUP BY
            if word in ['group', 'grouped'] and i+1 < len(tagged) and tagged[i+1][0] == 'by':
                group_by = True
                continue
            
            # Handle ORDER BY
            if word in ['order', 'ordered', 'sort', 'sorted'] and i+1 < len(tagged) and tagged[i+1][0] == 'by':
                order_by = True
                continue
                
            if order_by and tag.startswith('NN'):
                direction = 'DESC' if 'desc' in tokens[i:i+2] else 'ASC'
                components['order_by'] = (word, direction)
                order_by = False
        
        # Update select columns if found
        if columns and not components['aggregates']:
            components['select'] = columns
        
        return components

    def generate_sql_query(self, components: Dict[str, Any]) -> str:
        """Generate SQL query from components."""
        # Handle SELECT clause
        select_parts = []
        
        # Add aggregate functions
        for func, field, alias in components['aggregates']:
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
                if value.isdigit():
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

        # Combine all clauses
        query_parts = [
            select_clause,
            from_clause,
            where_clause,
            group_clause,
            order_clause
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
    