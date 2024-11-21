import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.tag import pos_tag
from typing import Dict, List, Tuple, Any, Optional, Union
import re
import logging

class QueryGenerator:
    def __init__(self):
        # Download required NLTK data
        for package in ['punkt', 'averaged_perceptron_tagger', 'stopwords', 'wordnet', 'punkt_tab', 'averaged_perceptron_tagger_eng']:
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
            'group': ['group by', 'grouped by', 'organize by', 'categorize by'],
            'order': ['order by', 'sort by', 'arranged by', 'ranked by'],
            'limit': ['limit', 'top', 'first', 'last', 'only']
        }

    def _clean_text(self, text: str) -> str:
        """Clean and normalize input text."""
        text = text.lower().strip()
        text = re.sub(r'\s+', ' ', text)  # Normalize whitespace
        return text

    def _identify_aggregate_function(self, tokens: List[str]) -> Optional[Tuple[str, str]]:
        """Identify aggregate functions and their target columns."""
        text = ' '.join(tokens)
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
        text = ' '.join(tokens)
        
        # Handle different condition patterns
        for operator_type, phrases in self.KEYWORDS['where']['operators'].items():
            for phrase in phrases:
                pattern = f"([\\w_]+)\\s+(?:{phrase})\\s+([\\w\\s.']+)"
                matches = re.finditer(pattern, text)
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

    def _extract_order_by(self, tokens: List[str]) -> Optional[Tuple[str, str]]:
        """Extract ORDER BY clause components."""
        text = ' '.join(tokens)
        for order_keyword in self.KEYWORDS['order']:
            pattern = f"{order_keyword}\\s+([\\w_]+)(?:\\s+(asc|desc))?"
            match = re.search(pattern, text)
            if match:
                field = match.group(1)
                direction = match.group(2).upper() if match.group(2) else 'ASC'
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

    def extract_query_components(self, text: str) -> Dict[str, Any]:
        """Extract all query components from natural language text."""
        cleaned_text = self._clean_text(text)
        tokens = word_tokenize(cleaned_text)
        logger.info(f"Cleaned text: {cleaned_text}")
        logger.info(f"Tokens: {tokens}")

        components = {
            'select': ['*'],  # Default
            'from': None,
            'where': [],
            'group_by': [],
            'order_by': None,
            'limit': None,
            'aggregate': None
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
        """Generate SQL query from components."""
        if not components.get('from'):
            return ""

        # Build SELECT clause
        select_clause = f"SELECT {', '.join(components['select'])}"

        # Build FROM clause
        from_clause = f"\nFROM {components['from']}"

        # Build WHERE clause
        where_clause = ""
        if components['where']:
            conditions = []
            for field, operator, value in components['where']:
                if operator in ('IN', 'BETWEEN'):
                    conditions.append(f"{field} {operator} {value}")
                elif operator == 'LIKE':
                    conditions.append(f"{field} {operator} '{value}'")
                elif value.replace('.', '').isdigit():
                    conditions.append(f"{field} {operator} {value}")
                else:
                    conditions.append(f"{field} {operator} '{value}'")
            where_clause = "\nWHERE " + " AND ".join(conditions)

        # Build GROUP BY clause
        group_clause = ""
        if components['group_by']:
            group_clause = "\nGROUP BY " + ", ".join(components['group_by'])

        # Build ORDER BY clause
        order_clause = ""
        if components['order_by']:
            field, direction = components['order_by']
            order_clause = f"\nORDER BY {field} {direction}"

        # Build LIMIT clause
        limit_clause = ""
        if components['limit']:
            limit_clause = f"\nLIMIT {components['limit']}"

        # Combine all clauses
        query = select_clause + from_clause + where_clause + group_clause + order_clause + limit_clause

        return query
    
    def generate_mongodb_query(self, components: Dict[str, Any]) -> Dict[str, Any]:
        """Generate MongoDB query from components."""
        query = {'find': {}}
        
        # Handle field selection
        if components['select'] != ['*']:
            query['projection'] = {field: 1 for field in components['select']}
            # Always include _id unless explicitly excluded or another field is specified
            if '_id' not in components['select']:
                query['projection']['_id'] = 0

        # Handle where conditions
        if components['where']:
            for field, operator, value in components['where']:
                try:
                    if '.' in value:
                        value = float(value)
                    else:
                        value = int(value)
                except ValueError:
                    value = value.strip("'\"")
                
                if operator == '=':
                    query['find'][field] = value
                elif operator == '>':
                    query['find'][field] = {'$gt': value}
                elif operator == '<':
                    query['find'][field] = {'$lt': value}
                elif operator == '>=':
                    query['find'][field] = {'$gte': value}
                elif operator == '<=':
                    query['find'][field] = {'$lte': value}
                elif operator == '!=':
                    query['find'][field] = {'$ne': value}
                elif operator == 'LIKE':
                    query['find'][field] = {'$regex': value.replace('%', ''), '$options': 'i'}
                elif operator == 'IN':
                    values = [v.strip().strip("'\"") for v in value.strip('()').split(',')]
                    query['find'][field] = {'$in': values}
        
        # Add sort
        if components['order_by']:
            field, direction = components['order_by']
            query['sort'] = {field: -1 if direction == 'DESC' else 1}
        
        # Add limit
        if components['limit']:
            query['limit'] = components['limit']
        else:
            query['limit'] = 10  # Default limit
            
        return query