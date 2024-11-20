import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.tag import pos_tag
from typing import Dict, List, Tuple, Any
import re

# Download required NLTK data
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('stopwords')

class QueryGenerator:
    OPERATOR_MAPPING = {
        '=': '=',
        'equals': '=',
        'equal': '=',
        'is': '=',
        'greater': '>',
        'less': '<',
        'greater than': '>',
        'less than': '<',
        'not': '!=',
        'not equal': '!=',
        'like': 'LIKE',
        'contains': 'LIKE',
        'in': 'IN',
        'between': 'BETWEEN',
        'greater equal': '>=',
        'less equal': '<='
    }
    def __init__(self):
        self.stop_words = set(stopwords.words('english'))
        # Add custom keywords for database operations
        self.query_keywords = {
            'select': ['show', 'display', 'get', 'find', 'search', 'list', 'what'],
            'where': ['where', 'which', 'with', 'has', 'have'],
            'order': ['order', 'sort', 'arrange'],
            'group': ['group', 'combine', 'aggregate'],
            'limit': ['limit', 'top', 'first', 'last']
        }
        
        self.aggregation_keywords = {
            'count': ['count', 'how many', 'number of'],
            'sum': ['sum', 'total'],
            'average': ['average', 'mean', 'avg'],
            'maximum': ['maximum', 'max', 'highest', 'most'],
            'minimum': ['minimum', 'min', 'lowest', 'least']
        }
    def _normalize_operator(self, operator: str) -> str:
        """將自然語言運算符轉換為SQL運算符"""
        operator = operator.lower()
        return self.OPERATOR_MAPPING.get(operator, operator)
    
    def extract_query_components(self, text: str) -> Dict[str, Any]:
        if not text or not isinstance(text, str):
            raise ValueError("Invalid input text")

        """Extract query components from natural language text"""
        tokens = word_tokenize(text.lower())
        pos_tags = pos_tag(tokens)
        
        components = {
            'select': [],
            'from': None,
            'where': [],
            'order_by': None,
            'group_by': [],
            'limit': None,
            'aggregation': None
        }
        
        # Extract table/collection name
        for token, tag in pos_tags:
            if tag in ['NN', 'NNS'] and token not in self.stop_words:
                components['from'] = token
                break
        
        # Extract fields to select
        for token, tag in pos_tags:
            if tag in ['NN', 'NNS'] and token not in self.stop_words:
                if token != components['from']:
                    components['select'].append(token)
        
        # Extract conditions (where clause)
        for i, (token, tag) in enumerate(pos_tags):
            if token in self.query_keywords['where']:
                try:
                    field = pos_tags[i+1][0]
                    operator = pos_tags[i+2][0]
                    value = pos_tags[i+3][0]
                    components['where'].append((field, operator, value))
                except IndexError:
                    continue
        
        # Extract ordering
        for i, (token, _) in enumerate(pos_tags):
            if token in self.query_keywords['order']:
                try:
                    field = pos_tags[i+2][0]
                    direction = 'DESC' if 'desc' in text.lower() else 'ASC'
                    components['order_by'] = (field, direction)
                except IndexError:
                    continue
        
        # Extract limit
        for i, (token, _) in enumerate(pos_tags):
            if token in self.query_keywords['limit']:
                try:
                    limit = pos_tags[i+1][0]
                    if limit.isdigit():
                        components['limit'] = int(limit)
                except IndexError:
                    continue
        
        # Extract aggregation
        for agg_type, keywords in self.aggregation_keywords.items():
            if any(keyword in text.lower() for keyword in keywords):
                components['aggregation'] = agg_type
                
        return components

    def generate_sql_query(self, components: Dict[str, Any]) -> str:
        if not isinstance(components, dict):
            raise TypeError("Components must be a dictionary")
        if not all(key in components for key in ['select', 'from', 'where']):
            raise ValueError("Missing required query components")
        
        """Generate SQL query from components"""
        select_clause = "SELECT "
        if components['aggregation']:
            if components['select']:
                select_clause += f"{components['aggregation'].upper()}({components['select'][0]})"
            else:
                select_clause += f"{components['aggregation'].upper()}(*)"
        else:
            select_clause += "*" if not components['select'] else ", ".join(components['select'])
        
        from_clause = f" FROM {components['from']}" if components['from'] else ""
        
        where_clause = ""
        if components['where']:
            conditions = []
            for field, op, value in components['where']:
                if value.isdigit():
                    conditions.append(f"{field} {op} {value}")
                else:
                    conditions.append(f"{field} {op} '{value}'")
            where_clause = " WHERE " + " AND ".join(conditions)
        
        group_clause = ""
        if components['group_by']:
            group_clause = " GROUP BY " + ", ".join(components['group_by'])
        
        order_clause = ""
        if components['order_by']:
            field, direction = components['order_by']
            order_clause = f" ORDER BY {field} {direction}"
        
        limit_clause = f" LIMIT {components['limit']}" if components['limit'] else ""
        
        return select_clause + from_clause + where_clause + group_clause + order_clause + limit_clause

    def generate_mongodb_query(self, components: Dict[str, Any]) -> Dict[str, Any]:
        """Generate MongoDB query from components"""
        pipeline = []
        
        # Match stage (where conditions)
        if components['where']:
            match_conditions = {}
            for field, op, value in components['where']:
                if value.isdigit():
                    value = int(value)
                if op == '=':
                    match_conditions[field] = value
                elif op == '>':
                    match_conditions[field] = {'$gt': value}
                elif op == '<':
                    match_conditions[field] = {'$lt': value}
            if match_conditions:
                pipeline.append({'$match': match_conditions})
        
        # Group stage (aggregation)
        if components['aggregation']:
            group_stage = {
                '_id': None,
            }
            if components['select']:
                field = components['select'][0]
                if components['aggregation'] == 'count':
                    group_stage['count'] = {'$sum': 1}
                elif components['aggregation'] == 'sum':
                    group_stage['sum'] = {'$sum': f'${field}'}
                elif components['aggregation'] == 'average':
                    group_stage['avg'] = {'$avg': f'${field}'}
                elif components['aggregation'] == 'maximum':
                    group_stage['max'] = {'$max': f'${field}'}
                elif components['aggregation'] == 'minimum':
                    group_stage['min'] = {'$min': f'${field}'}
            pipeline.append({'$group': group_stage})
        
        # Sort stage
        if components['order_by']:
            field, direction = components['order_by']
            pipeline.append({'$sort': {field: 1 if direction == 'ASC' else -1}})
        
        # Limit stage
        if components['limit']:
            pipeline.append({'$limit': components['limit']})
        
        # Project stage (select fields)
        if components['select'] and not components['aggregation']:
            project = {field: 1 for field in components['select']}
            pipeline.append({'$project': project})
        
        return pipeline if pipeline else {'find': {}}