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
        for package in ['punkt', 'averaged_perceptron_tagger', 'stopwords', 'wordnet', 'maxent_ne_chunker_tab',
                        'words',]:
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
            'join': {
                'inner': ['join', 'joined with', 'combined with', 'matching'],
                'left': ['left join', 'including all from left', 'keeping all from'],
                'right': ['right join', 'including all from right'],
                'outer': ['outer join', 'full join', 'including all']
            },
            'where': {
                'words': ['where', 'with', 'has', 'have', 'having', 'that', 'whose', 'which'],
                'operators': {
                    '=': ['equals', 'equal to', 'is', 'matches', 'same as'],
                    '>': ['greater than', 'more than', 'above', 'over', 'exceeds'],
                    '<': ['less than', 'lower than', 'below', 'under'],
                    '>=': ['greater than or equal to', 'at least', 'minimum'],
                    '<=': ['less than or equal to', 'at most', 'maximum'],
                    '!=': ['not equal to', 'different from', 'not'],
                    'LIKE': ['like', 'contains', 'similar to', 'starts with', 'ends with'],
                    'IN': ['in', 'among', 'within', 'one of'],
                    'BETWEEN': ['between', 'from', 'range']
                }
            },
            'group_by': ['group by', 'grouped by', 'organize by', 'categorize by'],
            'order_by': ['order by', 'ordered by', 'sort by', 'sorted by', 'arrange by', 'arranged by'],
            'direction': {
                'ASC': ['ascending', 'asc', 'increasing'],
                'DESC': ['descending', 'desc', 'decreasing', 'reverse']
            },
            'limit': ['limit', 'top', 'first', 'last', 'only']
        }

    def _clean_text(self, text: str) -> str:
        """Clean and normalize input text."""
        text = text.lower().strip()
        text = re.sub(r'\s+', ' ', text)  # Normalize whitespace
        return text

    def _clean_column_name(self, column_name: str) -> str:
        """
        Clean and format column names that might contain spaces or special characters.
        Wraps column names with backticks if they contain spaces.
        """
        column_name = column_name.strip()
        if ' ' in column_name:
            return f"`{column_name}`"
        return column_name

    def _extract_aggregate_functions(self, text: str) -> List[Tuple[str, str]]:
        """Extract aggregate functions and their target columns using NLTK."""
        aggregates = []
        tokens = word_tokenize(text.lower())
        tagged = pos_tag(tokens)
        
        for agg_type, phrases in self.KEYWORDS['select']['aggregates'].items():
            for phrase in phrases:
                if phrase in text:
                    # Find the target column after the aggregate keyword
                    pattern = f"{phrase}\\s+(?:of\\s+)?([\\w_]+)"
                    match = re.search(pattern, text)
                    if match:
                        return (agg_type, match.group(1))
        return None

    def _extract_table_name(self, tokens: List[str]) -> Optional[str]:
        """Extract table name from tokens."""
        text = ' '.join(tokens)
        # Common patterns for table references
        patterns = [
            r'from\s+([a-zA-Z_]\w*)',
            r'in\s+([a-zA-Z_]\w*)\s+table',
            r'from\s+the\s+([a-zA-Z_]\w*)',
            r'in\s+([a-zA-Z_]\w*)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)
                
        # Look for plural nouns that might be table names
        tagged = pos_tag(tokens)
        for word, tag in tagged:
            if tag in ['NNS', 'NNPS'] and word not in self.stop_words:
                return word
        
        return None

    def _extract_conditions(self, tokens: List[str]) -> List[Tuple[str, str, str]]:
        """Extract WHERE conditions from tokens."""
        conditions = []
        
        # Define condition patterns
        number_comparison_pattern = r'(\w+)\s*(>|<|>=|<=|=|!=)\s*(\d+(?:\.\d+)?)'
        text_comparison_pattern = r'(\w+)\s+(?:is|equals|equal to)\s+[\'"]*([^\'"\s]+)[\'"]*'
        
        # Check for numeric comparisons
        number_matches = re.finditer(number_comparison_pattern, text.lower())
        for match in number_matches:
            field, operator, value = match.groups()
            conditions.append((field, operator, value))
        
        # Check for text comparisons
        text_matches = re.finditer(text_comparison_pattern, text.lower())
        for match in text_matches:
            field, value = match.groups()
            conditions.append((field, '=', value))

        # Check for special operators from keywords
        for operator_type, phrases in self.KEYWORDS['where']['operators'].items():
            for phrase in phrases:
                pattern = f"([\\w_]+)\\s+(?:{phrase})\\s+([\\w\\s.']+)"
                matches = re.finditer(pattern, text.lower())
                for match in matches:
                    field = match.group(1)
                    value = match.group(2).strip()
                    
                    # Handle special cases
                    if operator_type == 'LIKE':
                        value = f"%{value}%"
                    elif operator_type == 'IN':
                        value = f"({value.replace(' ', ', ')})"
                    elif operator_type == 'BETWEEN':
                        value = value.replace(' and ', ' AND ')
                    
                    conditions.append((field, operator_type, value))
        
        return conditions

    def _extract_order_by(self, text: str) -> Optional[Tuple[str, str]]:
        """Extract ORDER BY clause using NLTK."""
        text = text.lower()
        
        # Check for ordering keywords
        order_pattern = r'order(?:ed)?\s+by\s+([\w\s]+?)(?:\s+(desc|asc|descending|ascending))?(?:\s|$)'
        match = re.search(order_pattern, text, re.IGNORECASE)
        
        if match:
            field = match.group(1).strip()
            direction = match.group(2).upper() if match.group(2) else 'ASC'
            
            # Normalize direction
            if direction.startswith('DESC'):
                direction = 'DESC'
            elif direction.startswith('ASC'):
                direction = 'ASC'
                
            return (field, direction)
        
        # Check for alternative sorting patterns
        sort_pattern = r'sort(?:ed)?\s+by\s+([\w\s]+?)(?:\s+(desc|asc|descending|ascending))?(?:\s|$)'
        match = re.search(sort_pattern, text, re.IGNORECASE)
        
        if match:
            field = match.group(1).strip()
            direction = match.group(2).upper() if match.group(2) else 'ASC'
            
            # Normalize direction
            if direction.startswith('DESC'):
                direction = 'DESC'
            elif direction.startswith('ASC'):
                direction = 'ASC'
                
            return (field, direction)
            
        return None

    def _extract_group_by(self, tokens: List[str]) -> List[str]:
        """Extract GROUP BY fields."""
        text = ' '.join(tokens)
        group_by_fields = []
        for group_keyword in self.KEYWORDS['group']:
            pattern = f"{group_keyword}\\s+([\\w_,\\s]+)"
            match = re.search(pattern, text)
            if match:
                fields = match.group(1).split(',')
                group_by_fields.extend([f.strip() for f in fields])
        return group_by_fields

    def _extract_limit(self, tokens: List[str]) -> Optional[int]:
        """Extract LIMIT value."""
        text = ' '.join(tokens)
        for limit_keyword in self.KEYWORDS['limit']:
            pattern = f"{limit_keyword}\\s+(\\d+)"
            match = re.search(pattern, text)
            if match:
                return int(match.group(1))
        return None

    def extract_query_components(self, text: str, available_tables: List[str]) -> Dict[str, Any]:
        """
        Extract all query components with improved WHERE clause handling.
        """
        text = self._clean_text(text)
        tokens = word_tokenize(text)
        tagged = pos_tag(tokens)
        
        components = {
            'select': ['*'],
            'from': None,
            'where': [],
            'group_by': [],
            'order_by': None,
            'limit': None,
            'aggregates': []
        }

        # Handle various query patterns
        patterns = [
            # "show all _id in country" or "show all _id from country"
            r'^show\s+all\s+(\w+)(?:\s+in|\s+from)\s+(\w+)$',
            # "find all cars"
            r'^find\s+all\s+(\w+)$',
            # "cars find all"
            r'^(\w+)\s+find\s+all$',
            # Simple name
            r'^(\w+)$'
        ]

        for pattern in patterns:
            match = re.match(pattern, cleaned_text)
            if match:
                logger.info(f"Matched pattern: {pattern}")
                logger.info(f"Match groups: {match.groups()}")
                
                if pattern.endswith('(\w+)$'):  # Simple name pattern
                    components['from'] = match.group(1)
                    break
                elif 'in' in pattern or 'from' in pattern:
                    field = match.group(1)
                    table = match.group(2)
                    components['select'] = [field]
                    components['from'] = table
                    break
                else:  # "find all" patterns
                    table = match.group(1)
                    components['from'] = table
                    break

        # If no pattern matched, try original parsing
        if not components['from']:
            # Extract components as before
            aggregate_info = self._identify_aggregate_function(tokens)
            if aggregate_info:
                components['aggregate'] = aggregate_info
                components['select'] = [f"{aggregate_info[0]}({aggregate_info[1]})"]

            components['from'] = self._extract_table_name(tokens)
            components['where'] = self._extract_conditions(tokens)
            components['order_by'] = self._extract_order_by(tokens)
            components['group_by'] = self._extract_group_by(tokens)
            components['limit'] = self._extract_limit(tokens)

        logger.info(f"Generated components: {components}")
        return components

    def generate_sql_query(self, components: Dict[str, Any]) -> str:
        """Generate SQL query from components with improved WHERE clause handling."""
        # Handle SELECT clause
        if not components['select'] or components['select'] == ['*']:
            select_clause = "SELECT *"
        else:
            select_clause = f"SELECT {', '.join(self._clean_column_name(col) for col in components['select'])}"

        # Handle FROM clause
        if not components['from']:
            raise ValueError("No table name specified")
        from_clause = f"FROM {components['from']}"

        # Handle WHERE clause
        where_clause = ""
        if components['where']:
            conditions = []
            for field, operator, value in components['where']:
                if isinstance(value, (int, float)) or value.isdigit():
                    conditions.append(f"{self._clean_column_name(field)} {operator} {value}")
                else:
                    conditions.append(f"{self._clean_column_name(field)} {operator} '{value}'")
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)

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
            order_clause,
            limit_clause
        ]

        # Join non-empty clauses with spaces
        query = " ".join(part for part in query_parts if part)
        return query.strip()


    def _clean_column_name(self, column_name: str) -> str:
        """Clean and format column names."""
        column_name = column_name.strip()
        if ' ' in column_name:
            return f"`{column_name}`"
        return column_name