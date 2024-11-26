from typing import Dict, Any, List, Tuple
import re

class MongoDBQueryGenerator:
    def generate_mongo_query(self, sql_query: str) -> Dict[str, Any]:
        """
        Convert SQL query to MongoDB query string.
        
        Args:
            sql_query: SQL query string.
            
        Returns:
            MongoDB query in shell format.
        """
        # Parse the SQL query
        components = self._parse_sql_query(sql_query)
        
        # Convert to aggregation pipeline
        pipeline = self._create_pipeline(components)
        
        # Return both the collection name and pipeline
        return {
            "collection": components['from'],
            "pipeline": pipeline
        }

    def _parse_sql_query(self, sql_query: str) -> Dict[str, Any]:
        """Parse SQL query and extract its components."""
        components = {
            'select': [],
            'from': None,
            'where': [],
            'group_by': [],
            'order_by': None,
            'limit': None
        }
        
        # Extract SELECT clause
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_query, re.IGNORECASE)
        if select_match:
            select_fields = [field.strip() for field in select_match.group(1).split(',')]
            components['select'] = select_fields
        
        # Extract FROM clause
        from_match = re.search(r'FROM\s+(\w+)', sql_query, re.IGNORECASE)
        if from_match:
            components['from'] = from_match.group(1)
        
        # Extract WHERE clause
        where_match = re.search(r'WHERE\s+(.*?)(?:\s+GROUP BY|\s+ORDER BY|\s+LIMIT|$)', sql_query, re.IGNORECASE)
        if where_match:
            components['where'] = self._extract_conditions(where_match.group(1))
        
        return components

    def _extract_conditions(self, where_clause: str) -> List[Tuple[str, str, Any]]:
        """Extract conditions from the WHERE clause."""
        conditions = []
        
        # Split by AND
        sub_conditions = re.split(r'\s+AND\s+', where_clause, re.IGNORECASE)
        
        for condition in sub_conditions:
            # Check for various comparison operators
            operators = {
                '<=': '<=',
                '>=': '>=',
                '!=': '!=',
                '=': '=',
                '<': '<',
                '>': '>'
            }
            
            for op_symbol, op_name in operators.items():
                if op_symbol in condition:
                    field, value = condition.split(op_symbol)
                    # Just strip whitespace but preserve case
                    field = field.strip()
                    value = value.strip().strip("'\"")
                    
                    # Convert value to number if possible
                    try:
                        if '.' in value:
                            value = float(value)
                        else:
                            value = int(value)
                    except ValueError:
                        pass  # Keep as string if conversion fails
                        
                    conditions.append((field, op_name, value))
                    break
                    
        return conditions

    def _create_pipeline(self, components: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create MongoDB aggregation pipeline from components."""
        pipeline = []
        
        # Handle filters (WHERE)
        if components['where']:
            match_stage = self._convert_conditions(components['where'])
            pipeline.append({"$match": match_stage})
        
        # Handle projections (SELECT)
        if components['select'] and components['select'] != ['*']:
            project_stage = {
                "_id": 0  # Always exclude _id
            }
            for field in components['select']:
                project_stage[field] = 1
            if project_stage:
                pipeline.append({"$project": project_stage})
        
        return pipeline

    def _convert_conditions(self, conditions: List[Tuple[str, str, Any]]) -> Dict[str, Any]:
        """Convert SQL conditions to MongoDB query filters."""
        mongo_filters = {}
        
        operator_map = {
            '=': None,  # direct equality
            '>': '$gt',
            '<': '$lt',
            '>=': '$gte',
            '<=': '$lte',
            '!=': '$ne'
        }
        
        for field, op, value in conditions:
            if op == '=':
                mongo_filters[field] = value
            else:
                mongo_operator = operator_map[op]
                if mongo_operator:
                    mongo_filters[field] = {mongo_operator: value}
                
        return mongo_filters