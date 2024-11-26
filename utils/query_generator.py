import nltk
from nltk.corpus import stopwords
from typing import Dict, List, Tuple, Any, Optional
import re
import logging

class QueryGenerator:
    def __init__(self):
        """Initialize the Query Generator with enhanced pattern matching"""
        try:
            nltk.download('punkt', quiet=True)
            nltk.download('stopwords', quiet=True)
        except Exception as e:
            logging.warning(f"Failed to download NLTK data: {str(e)}")

        self.stop_words = set(stopwords.words('english'))
        
        self.sort_patterns = {
            'ASC': [
                r'ascending',
                r'asc',
                r'increasing',
                r'growing',
                r'smaller\s+to\s+larger',
                r'lowest\s+to\s+highest'
            ],
            'DESC': [
                r'descending',
                r'desc',
                r'decreasing',
                r'declining',
                r'larger\s+to\s+smaller',
                r'highest\s+to\s+lowest'
            ]
        }
        
        self.condition_patterns = {
            'equals': ('=', [
                r'([a-zA-Z_]\w*)\s+(?:is|equals|equal\s+to|=|identical\s+to|matches|same\s+as)\s+[\'"]*([^\'"\s]+)[\'"]*',
                r'whose\s+([a-zA-Z_]\w*)\s+(?:is|equals|matches)\s+[\'"]*([^\'"\s]+)[\'"]*',
                r'([a-zA-Z_]\w*)\s+(?:with\s+value|value)\s+[\'"]*([^\'"\s]+)[\'"]*'
            ]),
            'greater': ('>', [
                r'([a-zA-Z_]\w*)\s*(?:>|greater\s+than|more\s+than|larger\s+than|bigger\s+than|higher\s+than|exceeds|above|over)\s*(\d*\.?\d*)',
                r'([a-zA-Z_]\w*)\s+(?:exceeding|greater|more|larger|bigger|higher)\s+than\s*(\d*\.?\d*)',
                r'([a-zA-Z_]\w*)\s+at\s+least\s*(\d*\.?\d*)'
            ]),
            'less': ('<', [
                r'([a-zA-Z_]\w*)\s*(?:<|less\s+than|smaller\s+than|lower\s+than|under|below)\s*(\d*\.?\d*)',
                r'([a-zA-Z_]\w*)\s+(?:not\s+exceeding|not\s+more\s+than|at\s+most)\s*(\d*\.?\d*)',
                r'([a-zA-Z_]\w*)\s+(?:within|under|below)\s*(\d*\.?\d*)'
            ]),
            'not_equals': ('!=', [
                r'([a-zA-Z_]\w*)\s+(?:not|!=|differs\s+from|different\s+from)\s+(?:equal\s+to|equals|=)\s+[\'"]*([^\'"\s]+)[\'"]*',
                r'([a-zA-Z_]\w*)\s+(?:is\s+not|isnt|is\s+different\s+from)\s+[\'"]*([^\'"\s]+)[\'"]*',
                r'([a-zA-Z_]\w*)\s+(?:excluding|except|other\s+than)\s+[\'"]*([^\'"\s]+)[\'"]*'
            ])
        }
        
        self.agg_patterns = {
            ('MAX', 'max_'): [
                r'(?:find|get|show)\s+(?:the\s+)?(?:maximum|highest|largest|biggest|greatest|peak)\s+([a-zA-Z_]\w*)',
                r'(?:highest|largest|biggest|maximum|greatest|peak)\s+([a-zA-Z_]\w*)',
                r'max\s+([a-zA-Z_]\w*)'
            ],
            ('MIN', 'min_'): [
                r'(?:find|get|show)\s+(?:the\s+)?(?:minimum|lowest|smallest|least|bottom)\s+([a-zA-Z_]\w*)',
                r'(?:lowest|smallest|minimum|least|bottom)\s+([a-zA-Z_]\w*)',
                r'min\s+([a-zA-Z_]\w*)'
            ],
            ('AVG', 'avg_'): [
                r'(?:find|get|show)\s+(?:the\s+)?(?:average|mean|typical|expected)\s+([a-zA-Z_]\w*)',
                r'(?:mean|average|typical|expected)\s+([a-zA-Z_]\w*)',
                r'avg\s+([a-zA-Z_]\w*)'
            ],
            ('SUM', 'sum_'): [
                r'(?:find|get|show)\s+(?:the\s+)?(?:sum|total|aggregate)\s+(?:of\s+)?([a-zA-Z_]\w*)',
                r'(?:total|sum|aggregate)\s+([a-zA-Z_]\w*)',
                r'sum\s+up\s+([a-zA-Z_]\w*)'
            ],
            ('COUNT', 'count_'): [
                r'count\s+(?:the\s+)?(?:total\s+)?(?:number\s+of\s+)?([a-zA-Z_]\w*)',
                r'how\s+many\s+([a-zA-Z_]\w*)',
                r'number\s+of\s+([a-zA-Z_]\w*)',
                r'quantity\s+of\s+([a-zA-Z_]\w*)'
            ]
        }

        self.group_patterns = [
            r'grouped?\s+by\s+([a-zA-Z_]\w*)',
            r'group\s+(?:by|on|using)\s+([a-zA-Z_]\w*)',
            r'categorize(?:d)?\s+by\s+([a-zA-Z_]\w*)',
            r'organize(?:d)?\s+by\s+([a-zA-Z_]\w*)',
            r'split\s+by\s+([a-zA-Z_]\w*)',
            r'partition(?:ed)?\s+by\s+([a-zA-Z_]\w*)',
            r'(?:records|data|documents|results)\s+by\s+([a-zA-Z_]\w*)'  # Added new pattern
        ]
        
    def extract_query_components(self, text: str, available_tables: List[str]) -> Dict[str, Any]:
        """Extract query components from natural language text."""
        # Store original text for field name extraction
        original_text = text
        # Convert to lower case for keyword matching only
        text = text.lower()
        
        components = {
            'select': ['*'],
            'from': None,
            'where': [],
            'having': [],
            'group_by': [],
            'order_by': None,
            'aggregates': [],
            'limit': None
        }
        
        # Extract table name using lowercase text
        components['from'] = self._extract_table_name(text, available_tables)
        if not components['from']:
            return components
            
        # Extract table name
        components['from'] = self._extract_table_name(text, available_tables)
        if not components['from']:
            return components
            
        # Check for GROUP BY with enhanced patterns
        for pattern in self.group_patterns:
            group_match = re.search(pattern, original_text)
            if group_match:
                group_field = group_match.group(1)  # Keep original case
                components['group_by'].append(group_field)
                
                if 'count' in text:
                    components['select'] = [group_field]
                    components['aggregates'] = [('COUNT', '*', 'count_total')]
                    
                    # Check for ORDER BY after GROUP BY
                    order_match = re.search(r'(?:order|sort)\s+by\s+([a-zA-Z_]\w*)(?:\s+(desc|asc|descending|ascending))?', text, re.IGNORECASE)
                    if order_match:
                        field = order_match.group(1)
                        direction = self._parse_sort_direction(order_match.group(2) if order_match.group(2) else '')
                        components['order_by'] = (field, direction)
                    
                    return components
                break
            
        # Handle regular COUNT queries
        if 'count' in text:
            components['aggregates'] = [('COUNT', '*', 'count_total')]
            components['select'] = []
            components['where'] = self._extract_conditions(original_text)
            return components
            
        # Check HAVING patterns
        if 'having' in text:
            # Use case-sensitive matching for field names
            match = re.search(r'([a-zA-Z_]\w*)\s+having\s+(?:average|avg)\s+([a-zA-Z_]\w*)\s*(>|<|=)\s*(\d+)', original_text)
            if match:
                group_field, agg_field = match.group(1), match.group(2)  # Keep original case
                op, value = match.group(3), match.group(4)
                components['select'] = [group_field]
                components['group_by'] = [group_field]
                components['having'] = [(f"AVG({agg_field})", op, value)]
                
                # Check for order by
                if 'order by' in text:
                    order_match = re.search(r'order\s+by\s+([a-zA-Z_]\w*)(?:\s+(desc|asc))?', original_text)
                    if order_match:
                        field = order_match.group(1)  # Keep original case
                        direction = order_match.group(2).upper() if order_match.group(2) else 'ASC'
                        components['order_by'] = (field, direction)
                
                return components
        
        # Extract columns
        if not components['aggregates']:
            cols = self._extract_columns(original_text)  # Use original text
            if cols:
                components['select'] = cols
                
        # Extract conditions using original text for field names
        components['where'] = self._extract_conditions(original_text)
        
        # Extract ORDER BY using original text for field names
        if any(word in text for word in ['order by', 'sort by']):
            order_match = re.search(r'(?:order|sort)\s+by\s+([a-zA-Z_]\w*)(?:\s+(desc|asc))?', original_text)
            if order_match:
                field = order_match.group(1)  # Keep original case
                direction = order_match.group(2).upper() if order_match.group(2) else 'ASC'
                components['order_by'] = (field, direction)
                
        return components
    
    def _extract_columns(self, text: str) -> List[str]:
        """Extract column names preserving case."""
        # Don't extract columns if just "show cars" or "find cars"
        if re.match(r'(?i)(?:show|find|display|get)\s+(?:all\s+)?cars\b', text):
            return ['*']
        
        # Replace 'and' with comma for column lists
        text = re.sub(r'(?i)\s+and\s+', ', ', text)
        
        patterns = [
            r'(?i)(?:find|show|select|get|display)\s+([a-zA-Z_]\w*(?:\s*,\s*[a-zA-Z_]\w*)*?)(?:\s+from|\s+in|\s+where|\s+order|\s+$)',
            r'(?i)(?:calculate|compute)\s+([a-zA-Z_]\w*(?:\s*,\s*[a-zA-Z_]\w*)*?)(?:\s+from|\s+in|\s+where|\s+$)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                cols = match.group(1).strip().split(',')
                cleaned_cols = [col.strip() for col in cols if col.strip() and col.strip() != '*' and col.strip().lower() != 'cars']
                if cleaned_cols:
                    return cleaned_cols
        
        return ['*']

    
    def _extract_conditions(self, text: str) -> List[Tuple[str, str, str]]:
        """Extract WHERE conditions preserving field name case."""
        conditions = []
        
        # Split by AND (case insensitive)
        subconditions = re.split(r'(?i)\s+and\s+', text)
        
        for subtext in subconditions:
            # Check between pattern first
            between_match = re.search(r'([a-zA-Z_]\w*)\s+between\s+(\d+)\s+and\s+(\d+)', subtext)
            if between_match:
                field = between_match.group(1)  # Keep original case
                low, high = between_match.group(2), between_match.group(3)
                conditions.extend([
                    (field, '>=', low),
                    (field, '<=', high)
                ])
                continue
                
            # Check other conditions
            for op_type, (op_symbol, patterns) in self.condition_patterns.items():
                for pattern in patterns:
                    match = re.search(pattern, subtext)
                    if match:
                        field = match.group(1)  # Keep original case
                        value = match.group(2)
                        conditions.append((field, op_symbol, value))
                        break
                    
        return conditions
    
    def _extract_aggregation(self, text: str) -> Optional[Tuple[str, str, str]]:
        """Extract aggregation function and field."""
        for (agg_type, prefix), pattern_list in self.agg_patterns.items():
            for pattern in pattern_list:
                match = re.search(pattern, text)
                if match:
                    field = match.group(1)
                    if field and field not in ['in', 'from', 'the']:
                        return (agg_type, field, f"{prefix}{field}")
        return None
    
    def _extract_table_name(self, text: str, available_tables: List[str]) -> Optional[str]:
        """Extract table name."""
        text = text
        
        # Try direct matches first
        for table in available_tables:
            if table in text:
                return table
                
        # Try looking after common prepositions
        for prep in ['from', 'in', 'of']:
            match = re.search(f'{prep}\\s+(\\w+)', text)
            if match:
                table_name = match.group(1)
                if table_name in [t for t in available_tables]:
                    return next(t for t in available_tables if t == table_name)
                    
        return None

    def _extract_limit(self, text: str) -> Optional[int]:
        """Extract LIMIT clause from text."""
        patterns = [
            r'top\s+(\d+)',
            r'first\s+(\d+)',
            r'limit\s+(\d+)',
            r'show\s+(\d+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text.lower())
            if match:
                return int(match.group(1))
        return None

    def generate_sql_query(self, components: Dict[str, Any]) -> str:
        """Generate SQL query with improved formatting."""
        parts = []
        
        # SELECT clause
        if components['aggregates']:
            agg_type, field, alias = components['aggregates'][0]
            if components['group_by']:
                # If we have GROUP BY, include both group field and aggregate
                group_fields = ', '.join(components['group_by'])
                parts.append(f"SELECT {group_fields}, {agg_type}({field}) AS {alias}")
            else:
                parts.append(f"SELECT {agg_type}({field}) AS {alias}")
        else:
            parts.append(f"SELECT {', '.join(components['select'])}")
            
        # FROM clause
        if components['from']:
            parts.append(f"FROM {components['from']}")
            
        # WHERE clause
        if components['where']:
            conditions = []
            for field, op, value in components['where']:
                if value.replace('.', '').isdigit():
                    conditions.append(f"{field} {op} {value}")
                else:
                    conditions.append(f"{field} {op} '{value}'")
            parts.append("WHERE " + " AND ".join(conditions))
            
        # GROUP BY clause
        if components['group_by']:
            parts.append(f"GROUP BY {', '.join(components['group_by'])}")
            
        # HAVING clause
        if components['having']:
            conditions = []
            for field, op, value in components['having']:
                conditions.append(f"{field} {op} {value}")
            parts.append("HAVING " + " AND ".join(conditions))
            
        # ORDER BY clause
        if components['order_by']:
            field, direction = components['order_by']
            parts.append(f"ORDER BY {field} {direction}")
            
        # LIMIT clause
        if components['limit'] is not None:
            parts.append(f"LIMIT {components['limit']}")
            
        return " ".join(parts)
    
    def _parse_sort_direction(self, text: str) -> str:
        """Enhanced sort direction parsing with more patterns"""
        text = text.lower().strip()
        
        # Check DESC patterns first
        for desc_pattern in self.sort_patterns['DESC']:
            if re.search(desc_pattern, text, re.IGNORECASE):
                return 'DESC'
        
        # Check ASC patterns
        for asc_pattern in self.sort_patterns['ASC']:
            if re.search(asc_pattern, text, re.IGNORECASE):
                return 'ASC'
        
        # Default to ASC if no match
        return 'ASC'
