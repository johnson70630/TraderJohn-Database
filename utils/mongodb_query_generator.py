from typing import Dict, Any, List, Tuple
import re

class MongoDBQueryGenerator:
    def generate_mongo_query(self, sql_query: str) -> Dict[str, Any]:
        """Convert SQL query to MongoDB query string."""
        components = self._parse_sql_query(sql_query)
        pipeline = []
        
        # Handle WHERE conditions first
        if components['where']:
            match_stage = self._convert_conditions(components['where'])
            if match_stage:
                pipeline.append({"$match": match_stage})
        
        # Handle GROUP BY and aggregations
        if components['group_by']:
            group_stage = {
                "$group": {
                    "_id": f"${components['group_by'][0]}"
                }
            }
            
            # Add aggregations to group stage
            if components['aggregates']:
                for agg_type, field, alias in components['aggregates']:
                    if agg_type == 'COUNT':
                        group_stage["$group"][alias] = {"$sum": 1}
                    elif agg_type == 'SUM':
                        group_stage["$group"][alias] = {"$sum": f"${field}"}
                    elif agg_type == 'AVG':
                        group_stage["$group"][alias] = {"$avg": f"${field}"}
                    elif agg_type == 'MAX':
                        group_stage["$group"][alias] = {"$max": f"${field}"}
                    elif agg_type == 'MIN':
                        group_stage["$group"][alias] = {"$min": f"${field}"}
            
            pipeline.append(group_stage)
            
            # Add project stage to rename _id back to original field name
            project_stage = {
                "$project": {
                    "_id": 0,
                    components['group_by'][0]: "$_id"
                }
            }
            
            # Add aggregated fields to project stage
            if components['aggregates']:
                for _, _, alias in components['aggregates']:
                    project_stage["$project"][alias] = 1
            
            pipeline.append(project_stage)
        
        # Handle ORDER BY
        if components['order_by']:
            field, direction = components['order_by']
            sort_direction = -1 if direction.upper() == 'DESC' else 1
            pipeline.append({"$sort": {field: sort_direction}})
        
        # Handle LIMIT
        if components['limit']:
            pipeline.append({"$limit": components['limit']})
        
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
            'having': [],
            'limit': None,
            'aggregates': []
        }
        
        # Extract SELECT clause
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_query, re.IGNORECASE)
        if select_match:
            select_fields = [field.strip() for field in select_match.group(1).split(',')]
            components['select'] = select_fields

            # Check for aggregation functions in SELECT
            for field in select_fields:
                agg_match = re.search(r'(COUNT|SUM|AVG|MIN|MAX)\((.*?)\)\s+AS\s+(\w+)', field, re.IGNORECASE)
                if agg_match:
                    components['aggregates'].append(
                        (agg_match.group(1).upper(), agg_match.group(2).strip(), agg_match.group(3))
                    )
        
        # Extract FROM clause
        from_match = re.search(r'FROM\s+(\w+)', sql_query, re.IGNORECASE)
        if from_match:
            components['from'] = from_match.group(1)
        
        # Extract WHERE clause
        where_match = re.search(r'WHERE\s+(.*?)(?:\s+GROUP BY|\s+ORDER BY|\s+HAVING|\s+LIMIT|$)', sql_query, re.IGNORECASE)
        if where_match:
            components['where'] = self._extract_conditions(where_match.group(1))
        
        # Extract GROUP BY clause
        group_match = re.search(r'GROUP BY\s+(.*?)(?:\s+HAVING|\s+ORDER BY|\s+LIMIT|$)', sql_query, re.IGNORECASE)
        if group_match:
            components['group_by'] = [field.strip() for field in group_match.group(1).split(',')]
        
        # Extract HAVING clause
        having_match = re.search(r'HAVING\s+(.*?)(?:\s+ORDER BY|\s+LIMIT|$)', sql_query, re.IGNORECASE)
        if having_match:
            components['having'] = self._extract_conditions(having_match.group(1))
        
        # Extract ORDER BY clause
        order_match = re.search(r'ORDER BY\s+(.*?)(?:\s+LIMIT|$)', sql_query, re.IGNORECASE)
        if order_match:
            order_parts = order_match.group(1).strip().split()
            components['order_by'] = (order_parts[0], order_parts[1] if len(order_parts) > 1 else 'ASC')
        
        # Extract LIMIT clause
        limit_match = re.search(r'LIMIT\s+(\d+)', sql_query, re.IGNORECASE)
        if limit_match:
            components['limit'] = int(limit_match.group(1))
        
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
    
    def _convert_having_conditions(self, conditions: List[Tuple[str, str, Any]]) -> Dict[str, Any]:
        """Convert SQL HAVING conditions to MongoDB format."""
        mongo_conditions = {}
        
        for condition in conditions:
            field, operator, value = condition
            # Remove any function wrappers from field name (e.g., AVG(field) -> field)
            clean_field = re.sub(r'\w+\((.*?)\)', r'\1', field)
            
            try:
                value = float(value) if '.' in value else int(value)
            except (ValueError, TypeError):
                pass
                
            if operator == '>':
                mongo_conditions[clean_field] = {"$gt": value}
            elif operator == '<':
                mongo_conditions[clean_field] = {"$lt": value}
            elif operator == '>=':
                mongo_conditions[clean_field] = {"$gte": value}
            elif operator == '<=':
                mongo_conditions[clean_field] = {"$lte": value}
            elif operator == '=':
                mongo_conditions[clean_field] = value
            elif operator == '!=':
                mongo_conditions[clean_field] = {"$ne": value}
                
        return mongo_conditions