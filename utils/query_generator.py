import nltk
from nltk.corpus import stopwords
from typing import Dict, List, Tuple, Any, Optional
import re
import logging

class QueryGenerator:
    def __init__(self):
        """Initialize the Query Generator with basic NLP capabilities"""
        try:
            nltk.download('punkt', quiet=True)
            nltk.download('stopwords', quiet=True)
        except Exception as e:
            logging.warning(f"Failed to download NLTK data: {str(e)}")

        self.stop_words = set(stopwords.words('english'))
        
        self.agg_patterns = {
            ('MAX', 'max_'): [
                r'(?:find|get|show)\s+(?:the\s+)?maximum\s+(\w+)',
                r'highest\s+(\w+)',
                r'max\s+(\w+)'
            ],
            ('MIN', 'min_'): [
                r'(?:find|get|show)\s+(?:the\s+)?minimum\s+(\w+)',
                r'lowest\s+(\w+)',
                r'min\s+(\w+)'
            ],
            ('AVG', 'avg_'): [
                r'(?:find|get|show)\s+(?:the\s+)?average\s+(\w+)',
                r'mean\s+(\w+)',
                r'avg\s+(\w+)'
            ],
            ('SUM', 'sum_'): [
                r'(?:find|get|show)\s+(?:the\s+)?(?:sum|total)\s+(?:of\s+)?(\w+)',
                r'total\s+(\w+)'
            ],
            ('COUNT', 'count_'): [
                r'count\s+(?:the\s+)?(?:total\s+)?(\w+)',
                r'how\s+many\s+(\w+)'
            ]
        }

        self.condition_patterns = {
            'equals': ('=', [
                r'(\w+)\s+(?:is|equals|equal\s+to|=)\s+[\'"]*([^\'"\s]+)[\'"]*',
                r'whose\s+(\w+)\s+is\s+[\'"]*([^\'"\s]+)[\'"]*'
            ]),
            'greater': ('>', [
                r'(\w+)\s+(?:greater|more|higher|bigger|>)\s+than\s+(\d+)',
                r'(\w+)\s+above\s+(\d+)'
            ]),
            'less': ('<', [
                r'(\w+)\s+(?:less|lower|smaller|<)\s+than\s+(\d+)',
                r'(\w+)\s+below\s+(\d+)'
            ]),
            'not_equals': ('!=', [
                r'(\w+)\s+(?:not|!=)\s+(?:equal\s+to|equals|=)\s+[\'"]*([^\'"\s]+)[\'"]*',
                r'(\w+)\s+is\s+not\s+[\'"]*([^\'"\s]+)[\'"]*'
            ])
        }

    def extract_query_components(self, text: str, available_tables: List[str]) -> Dict[str, Any]:
        """Extract query components from natural language text."""
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
        
        # 1. Extract table name
        components['from'] = self._extract_table_name(text, available_tables)
        if not components['from']:
            return components
            
        # 2. Handle COUNT queries first
        if 'count' in text:
            components['aggregates'] = [('COUNT', '*', 'count_total')]
            components['select'] = []
            components['where'] = self._extract_conditions(text)
            return components
            
        # 3. Check HAVING patterns
        if 'having' in text:
            match = re.search(r'(\w+)\s+having\s+(?:average|avg)\s+(\w+)\s*(>|<|=)\s*(\d+)', text)
            if match:
                group_field, agg_field, op, value = match.groups()
                components['select'] = [group_field]
                components['group_by'] = [group_field]
                components['having'] = [(f"AVG({agg_field})", op, value)]
                
                # Check for order by in having clause
                if 'order by' in text:
                    order_match = re.search(r'order\s+by\s+(\w+)(?:\s+(desc|asc))?', text)
                    if order_match:
                        field = order_match.group(1)
                        direction = order_match.group(2).upper() if order_match.group(2) else 'ASC'
                        components['order_by'] = (field, direction)
                
                return components
        
        # 4. Check for TOP N queries
        if 'top' in text:
            components['limit'] = self._extract_limit(text)
            # Don't include 'top N' in the SELECT clause
            if components['select'] == ['*']:
                components['select'] = ['*']
        
        # 5. Extract columns for non-COUNT queries
        if not components['aggregates']:
            cols = self._extract_columns(text)
            if cols:
                components['select'] = cols
                
        # 6. Extract conditions
        components['where'] = self._extract_conditions(text)
        
        # 7. Extract ORDER BY
        if any(word in text for word in ['order by', 'sort by']):
            order_match = re.search(r'(?:order|sort)\s+by\s+(\w+)(?:\s+(desc|asc))?', text)
            if order_match:
                field = order_match.group(1)
                direction = order_match.group(2).upper() if order_match.group(2) else 'ASC'
                components['order_by'] = (field, direction)
                
        return components
    
    def _extract_columns(self, text: str) -> List[str]:
        """Extract column names with improved pattern matching."""
        # Don't extract columns if just "show cars" or "find cars"
        if re.match(r'(?:show|find|display|get)\s+(?:all\s+)?cars\b', text):
            return ['*']
        
        # Replace 'and' with comma for column lists
        text = text.replace(' and ', ', ')
        
        patterns = [
            r'(?:find|show|select|get|display)\s+([\w\s,]+?)(?:\s+from|\s+in|\s+where|\s+order|\s+$)',
            r'(?:calculate|compute)\s+([\w\s,]+?)(?:\s+from|\s+in|\s+where|\s+$)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                cols = match.group(1).strip().split(',')
                cleaned_cols = [col.strip() for col in cols if col.strip() and col.strip() != '*' and col.strip() != 'cars']
                if cleaned_cols:
                    return cleaned_cols
        
        return ['*']

    
    def _extract_conditions(self, text: str) -> List[Tuple[str, str, str]]:
        """Extract WHERE conditions with improved pattern matching."""
        conditions = []
        
        # Split by AND
        subconditions = re.split(r'\s+and\s+', text)
        
        for subtext in subconditions:
            # Check between pattern first
            between_match = re.search(r'(\w+)\s+between\s+(\d+)\s+and\s+(\d+)', subtext)
            if between_match:
                field, low, high = between_match.groups()
                conditions.extend([
                    (field, '>=', low),
                    (field, '<=', high)
                ])
                continue
                
            # Check equals/not equals patterns
            eq_match = re.search(r'(\w+)\s+(?:is\s+|=\s*)(?:not\s+)?[\'"]?(\w+)[\'"]?', subtext)
            if eq_match:
                field, value = eq_match.groups()
                operator = '!=' if 'not' in subtext else '='
                if field != 'top':  # Skip if it's a TOP N query
                    conditions.append((field, operator, value))
                continue
                
            # Check greater/less than
            compare_patterns = [
                (r'(\w+)\s*(?:>|greater\s+than|more\s+than)\s*(\d+)', '>'),
                (r'(\w+)\s*(?:<|less\s+than|smaller\s+than)\s*(\d+)', '<'),
                (r'(\w+)\s*(?:>=|greater\s+than\s+or\s+equal\s+to)\s*(\d+)', '>='),
                (r'(\w+)\s*(?:<=|less\s+than\s+or\s+equal\s+to)\s*(\d+)', '<=')
            ]
            
            for pattern, operator in compare_patterns:
                match = re.search(pattern, subtext)
                if match:
                    conditions.append((match.group(1), operator, match.group(2)))
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
        text = text.lower()
        
        # Try direct matches first
        for table in available_tables:
            if table.lower() in text:
                return table
                
        # Try looking after common prepositions
        for prep in ['from', 'in', 'of']:
            match = re.search(f'{prep}\\s+(\\w+)', text)
            if match:
                table_name = match.group(1)
                if table_name in [t.lower() for t in available_tables]:
                    return next(t for t in available_tables if t.lower() == table_name)
                    
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