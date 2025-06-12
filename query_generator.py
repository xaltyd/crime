# query_generator.py
import sqlite3
import json
from datetime import datetime, timedelta
import re
from typing import Dict, List, Tuple, Any

class QueryGenerator:
    """Generates SQL queries from natural language questions about CT criminal justice data."""
    
    def __init__(self, db_path: str = 'records.db'):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        
        # Common statute patterns AND description keywords
        self.statute_patterns = {
            'gun': ['53a-216%', '53a-217%'],
            'weapon': ['53a-216%', '53a-217%', '53-206%'],
            'drug': ['21a-%'],
            'assault': ['53a-59%', '53a-60%', '53a-61%'],
            'theft': ['53a-119%', '53a-122%', '53a-123%', '53a-124%', '53a-125%'],
            'burglary': ['53a-101%', '53a-102%', '53a-103%'],
            'robbery': ['53a-133%', '53a-134%', '53a-135%', '53a-136%'],
            'murder': ['53a-54%'],
            'manslaughter': ['53a-55%', '53a-56%'],
            'dui': ['14-227a%', '14-227m%'],
            'domestic': ['53a-61%', '53a-64%'],
            'sex': ['53a-7%', '53a-65%', '53a-70%', '53a-71%', '53a-72%', '53a-73%']  # Added sex offense statutes
        }
        
        # Description keywords for searching in description field
        self.description_keywords = {
            'gun': ['firearm', 'pistol', 'weapon'],
            'drug': ['narcotic', 'controlled substance', 'heroin', 'cocaine', 'marijuana'],
            'sex': ['sexual', 'sex', 'rape', 'assault 1st deg', 'assault 2nd deg', 'assault 3rd deg'],
            'assault': ['assault', 'battery'],
            'theft': ['larceny', 'theft', 'stolen']
        }
        
        # Court location mappings - based on actual data
        self.court_locations = {
            'hartford': ['Hartford GA 14', 'LOCAL POLICE HARTFORD'],
            'new haven': ['New Haven GA 23', 'LOCAL POLICE NEW HAVEN'],
            'bridgeport': ['Bridgeport GA 2', 'LOCAL POLICE BRIDGEPORT'],
            'stamford': ['Stamford GA 1', 'LOCAL POLICE STAMFORD'],
            'norwalk': ['Norwalk GA 20', 'LOCAL POLICE NORWALK'],
            'waterbury': ['Waterbury GA 4', 'LOCAL POLICE WATERBURY'],
            'new london': ['New London GA 10', 'LOCAL POLICE NEW LONDON'],
            'norwich': ['Norwich GA 21', 'LOCAL POLICE NORWICH'],
            'middletown': ['Middletown GA 9', 'LOCAL POLICE MIDDLETOWN'],
            'meriden': ['Meriden GA 7', 'LOCAL POLICE MERIDEN'],
            'manchester': ['Manchester GA 12', 'LOCAL POLICE MANCHESTER'],
            'danbury': ['Danbury GA 3 and JD', 'LOCAL POLICE DANBURY'],
            'derby': ['Derby GA 5', 'LOCAL POLICE DERBY'],
            'new britain': ['New Britain GA 15', 'LOCAL POLICE NEW BRITAIN'],
            'milford': ['Milford GA 22', 'LOCAL POLICE MILFORD'],
            'torrington': ['Torrington GA 18', 'LOCAL POLICE TORRINGTON']
        }
    
    def parse_time_period(self, text: str) -> Tuple[str, str]:
        """Parse time period from natural language."""
        today = datetime.now()
        
        # Check for specific patterns
        if 'last 30 days' in text or 'past 30 days' in text or 'last month' in text:
            start_date = today - timedelta(days=30)
        elif 'last 3 months' in text or 'past 3 months' in text:
            start_date = today - timedelta(days=90)
        elif 'last 6 months' in text or 'past 6 months' in text:
            start_date = today - timedelta(days=180)
        elif 'last year' in text or 'past year' in text:
            start_date = today - timedelta(days=365)
        elif 'this year' in text:
            start_date = datetime(today.year, 1, 1)
        elif 'this month' in text:
            start_date = datetime(today.year, today.month, 1)
        else:
            # Default to last year
            start_date = today - timedelta(days=365)
        
        # Format dates as M/D/YYYY to match database format
        # Note: Windows doesn't support %-m and %-d, so we need a different approach
        start_str = f"{start_date.month}/{start_date.day}/{start_date.year}"
        end_str = f"{today.month}/{today.day}/{today.year}"
        return start_str, end_str
    
    def identify_charge_type(self, text: str) -> List[str]:
        """Identify charge types from natural language."""
        text_lower = text.lower()
        identified_charges = []
        
        for charge_type, patterns in self.statute_patterns.items():
            if charge_type in text_lower or any(synonym in text_lower for synonym in self.get_synonyms(charge_type)):
                identified_charges.extend(patterns)
        
        return identified_charges
    
    def get_synonyms(self, charge_type: str) -> List[str]:
        """Get synonyms for charge types."""
        synonyms = {
            'gun': ['firearm', 'weapon', 'pistol', 'handgun'],
            'drug': ['narcotic', 'controlled substance', 'drugs'],
            'assault': ['battery', 'attack'],
            'theft': ['larceny', 'stealing', 'stolen'],
            'dui': ['drunk driving', 'dwi', 'operating under influence']
        }
        return synonyms.get(charge_type, [])
    
    def identify_location(self, text: str) -> List[str]:
        """Identify court locations from natural language."""
        text_lower = text.lower()
        locations = []
        
        for location, variants in self.court_locations.items():
            if location in text_lower:
                locations.extend(variants)
        
        return locations
    
    def generate_query(self, question: str) -> Tuple[str, Dict[str, Any]]:
        """Generate SQL query from natural language question."""
        question_lower = question.lower()
        
        # Determine what type of data to query
        if any(word in question_lower for word in ['inmate', 'prisoner', 'incarcerated', 'doc']):
            return self._generate_inmate_query(question)
        elif any(word in question_lower for word in ['pending', 'awaiting trial', 'not convicted']):
            return self._generate_pending_query(question)
        elif any(word in question_lower for word in ['convicted', 'conviction', 'sentence', 'guilty']):
            return self._generate_conviction_query(question)
        else:
            # Default to searching all case types
            return self._generate_general_query(question)
    
    def _generate_conviction_query(self, question: str) -> Tuple[str, Dict[str, Any]]:
        """Generate query for conviction data."""
        params = {}
        where_clauses = []
        
        # Parse time period
        start_date, end_date = self.parse_time_period(question)
        where_clauses.append("(c.arrest_date BETWEEN ? AND ? OR c.sentenced_date BETWEEN ? AND ?)")
        params['start_date1'] = start_date
        params['end_date1'] = end_date
        params['start_date2'] = start_date
        params['end_date2'] = end_date
        
        # Parse location - check both court and arresting_agency
        locations = self.identify_location(question)
        if locations:
            location_clause = " OR ".join(["c.court LIKE ?" for _ in locations])
            location_clause += " OR " + " OR ".join(["c.arresting_agency LIKE ?" for _ in locations])
            where_clauses.append(f"({location_clause})")
            for i, loc in enumerate(locations):
                params[f'location_court{i}'] = f'%{loc}%'
            for i, loc in enumerate(locations):
                params[f'location_agency{i}'] = f'%{loc}%'
        
        # Parse charge type - search in both statute AND description
        question_lower = question.lower()
        charge_patterns = self.identify_charge_type(question)
        
        # Check for keywords that should search description
        description_search = []
        for keyword, patterns in self.description_keywords.items():
            if keyword in question_lower or any(p in question_lower for p in patterns):
                description_search.append(f'%{keyword}%')
                for pattern in patterns:
                    description_search.append(f'%{pattern}%')
        
        if charge_patterns or description_search:
            charge_clauses = []
            
            if charge_patterns:
                charge_clauses.append(" OR ".join(["ch.statute LIKE ?" for _ in charge_patterns]))
                for i, pattern in enumerate(charge_patterns):
                    params[f'charge_statute{i}'] = pattern
            
            if description_search:
                charge_clauses.append(" OR ".join(["ch.description LIKE ?" for _ in description_search]))
                for i, pattern in enumerate(description_search):
                    params[f'charge_desc{i}'] = pattern
            
            where_clauses.append(f"({' OR '.join(charge_clauses)})")
        
        # Determine what to count/return
        if 'how many' in question.lower():
            query = f"""
            SELECT COUNT(DISTINCT c.docket_no) as total_cases,
                   COUNT(DISTINCT c.last_first_name) as unique_defendants,
                   COUNT(ch.id) as total_charges,
                   GROUP_CONCAT(DISTINCT c.court) as courts,
                   GROUP_CONCAT(DISTINCT c.arresting_agency) as agencies
            FROM conviction c
            LEFT JOIN conviction_charges ch ON c.id = ch.case_id
            WHERE c.version = (SELECT MAX(version) FROM conviction WHERE docket_no = c.docket_no)
            """
        else:
            query = f"""
            SELECT c.docket_no, c.last_first_name, c.arrest_date, c.sentenced_date,
                   c.court, c.arresting_agency, ch.statute, ch.description, 
                   ch.verdict_finding, s.sentence_text
            FROM conviction c
            LEFT JOIN conviction_charges ch ON c.id = ch.case_id
            LEFT JOIN conviction_sentences s ON c.id = s.case_id AND s.sentence_type = 'OVERALL'
            WHERE c.version = (SELECT MAX(version) FROM conviction WHERE docket_no = c.docket_no)
            """
        
        if where_clauses:
            query += " AND " + " AND ".join(where_clauses)
        
        # Add grouping for count queries
        if 'how many' in question.lower():
            query += " GROUP BY c.court, c.arresting_agency"
        else:
            query += " ORDER BY c.sentenced_date DESC LIMIT 100"
        
        return query, params
    
    def _generate_pending_query(self, question: str) -> Tuple[str, Dict[str, Any]]:
        """Generate query for pending cases."""
        params = {}
        where_clauses = []
        
        # Similar structure to conviction query but for pending table
        start_date, end_date = self.parse_time_period(question)
        where_clauses.append("p.arrest_date BETWEEN ? AND ?")
        params['start_date'] = start_date
        params['end_date'] = end_date
        
        locations = self.identify_location(question)
        if locations:
            location_clause = " OR ".join(["p.court LIKE ?" for _ in locations])
            where_clauses.append(f"({location_clause})")
            for i, loc in enumerate(locations):
                params[f'location{i}'] = f'%{loc}%'
        
        charge_patterns = self.identify_charge_type(question)
        if charge_patterns:
            charge_clause = " OR ".join(["pc.statute LIKE ?" for _ in charge_patterns])
            where_clauses.append(f"({charge_clause})")
            for i, pattern in enumerate(charge_patterns):
                params[f'charge{i}'] = pattern
        
        if 'how many' in question.lower():
            query = """
            SELECT COUNT(DISTINCT p.docket_no) as total_cases,
                   COUNT(DISTINCT p.last_first_name) as unique_defendants,
                   COUNT(pc.id) as total_charges
            FROM pending p
            LEFT JOIN pending_charges pc ON p.id = pc.case_id
            WHERE p.version = (SELECT MAX(version) FROM pending WHERE docket_no = p.docket_no)
            """
        else:
            query = """
            SELECT p.docket_no, p.last_first_name, p.arrest_date, p.hearing_date,
                   p.court, pc.statute, pc.description, p.bond_amount
            FROM pending p
            LEFT JOIN pending_charges pc ON p.id = pc.case_id
            WHERE p.version = (SELECT MAX(version) FROM pending WHERE docket_no = p.docket_no)
            """
        
        if where_clauses:
            query += " AND " + " AND ".join(where_clauses)
        
        if 'how many' in question.lower():
            query += " GROUP BY p.court"
        else:
            query += " ORDER BY p.arrest_date DESC LIMIT 100"
        
        return query, params
    
    def _generate_inmate_query(self, question: str) -> Tuple[str, Dict[str, Any]]:
        """Generate query for inmate data."""
        params = {}
        where_clauses = []
        
        # Parse time period for admission date
        if any(word in question.lower() for word in ['admitted', 'entered', 'recent']):
            start_date, end_date = self.parse_time_period(question)
            where_clauses.append("latest_admission_date BETWEEN ? AND ?")
            params['start_date'] = start_date
            params['end_date'] = end_date
        
        # Parse location (facility)
        if any(word in question.lower() for word in ['facility', 'prison', 'location']):
            # This would need facility name mapping similar to court locations
            pass
        
        # Parse offense type
        charge_keywords = self.identify_charge_type(question)
        if charge_keywords:
            offense_clause = " OR ".join(["controlling_offense LIKE ?" for _ in charge_keywords])
            where_clauses.append(f"({offense_clause})")
            for i, keyword in enumerate(charge_keywords):
                params[f'offense{i}'] = f'%{keyword}%'
        
        if 'how many' in question.lower():
            query = """
            SELECT COUNT(*) as total_inmates,
                   COUNT(DISTINCT current_location) as facilities_count,
                   GROUP_CONCAT(DISTINCT current_location) as facilities
            FROM dept_of_correction
            WHERE 1=1
            """
        else:
            query = """
            SELECT inmate_number, inmate_name, controlling_offense,
                   current_location, status, estimated_release_date
            FROM dept_of_correction
            WHERE 1=1
            """
        
        if where_clauses:
            query += " AND " + " AND ".join(where_clauses)
        
        if 'how many' not in question.lower():
            query += " ORDER BY latest_admission_date DESC LIMIT 100"
        
        return query, params
    
    def _generate_general_query(self, question: str) -> Tuple[str, Dict[str, Any]]:
        """Generate a general query that searches across multiple tables."""
        # For now, default to conviction data as it's the most complete
        return self._generate_conviction_query(question)
    
    def execute_query(self, query: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute the generated query and return results."""
        # Convert params dict to list in the order they appear in the query
        param_list = []
        
        # Count the number of ? in the query to ensure we have the right number of params
        placeholders = query.count('?')
        
        # Extract parameters in the order they were added
        # This maintains the order from the query generation
        for i in range(placeholders):
            # Try different parameter naming patterns
            found = False
            for prefix in ['start_date', 'end_date', 'location_court', 'location_agency', 
                          'charge_statute', 'charge_desc', 'offense', 'location']:
                for suffix in ['', '1', '2'] + [str(j) for j in range(20)]:
                    key = f'{prefix}{suffix}'
                    if key in params and len(param_list) < placeholders:
                        param_list.append(params[key])
                        found = True
                        break
                if found:
                    break
        
        # Ensure we have the right number of parameters
        if len(param_list) != placeholders:
            # Fall back to original method if our extraction failed
            param_list = [params[key] for key in sorted(params.keys())]
        
        try:
            self.cursor.execute(query, param_list)
            columns = [desc[0] for desc in self.cursor.description]
            results = []
            
            for row in self.cursor.fetchall():
                results.append(dict(zip(columns, row)))
            
            return results
        except Exception as e:
            print(f"Query execution error: {e}")
            print(f"Query: {query}")
            print(f"Params: {param_list}")
            raise
    
    def close(self):
        """Close database connection."""
        self.conn.close()


# Example usage and testing
if __name__ == "__main__":
    generator = QueryGenerator()
    
    # Test questions
    test_questions = [
        "How many people in the last 3 months in Hartford had a gun charge?",
        "Show me all drug convictions in New Haven this year",
        "How many pending assault cases are there in Bridgeport?",
        "List inmates with weapon charges admitted in the last 6 months"
    ]
    
    for question in test_questions:
        print(f"\nQuestion: {question}")
        query, params = generator.generate_query(question)
        print(f"Generated Query: {query}")
        print(f"Parameters: {params}")
        
        # Execute and show results
        results = generator.execute_query(query, params)
        print(f"Results: {len(results)} records found")
        if results and len(results) > 0:
            print(f"Sample: {results[0]}")
    
    generator.close()
