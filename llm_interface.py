# llm_interface.py
import json
import requests
from typing import Dict, List, Any, Optional
from datetime import datetime
from simple_query_generator import CTJusticeQuerySystem  # Updated import name

class LLMQueryBot:
    """LLM interface that uses the working query system."""
    
    def __init__(self, db_path: str = 'records.db', model: str = 'mistral:7b-instruct-v0.2-q4_K_M'):
        self.query_system = CTJusticeQuerySystem(db_path)
        self.model = model
        self.ollama_url = "http://localhost:11434/api/generate"
        
        # Verify Ollama is running
        try:
            requests.get("http://localhost:11434/api/tags")
        except:
            raise ConnectionError("Ollama is not running. Please start Ollama first.")
    
    def _call_ollama(self, prompt: str, temperature: float = 0.1) -> str:
        """Call Ollama API."""
        response = requests.post(self.ollama_url, json={
            "model": self.model,
            "prompt": prompt,
            "temperature": temperature,
            "stream": False
        })
        
        if response.status_code == 200:
            return response.json()['response']
        else:
            raise Exception(f"Ollama error: {response.status_code}")
    
    def process_query(self, user_question: str) -> Dict[str, Any]:
        """Process a natural language query and return results."""
        
        # Understand the question
        understanding = self._understand_question(user_question)
        
        # Execute appropriate query based on understanding
        try:
            results = self._execute_appropriate_query(user_question, understanding)
            
            # Interpret results
            interpretation = self._interpret_results(user_question, results, understanding)
            
            return {
                'question': user_question,
                'understanding': understanding,
                'results': results,
                'interpretation': interpretation,
                'timestamp': datetime.now().isoformat(),
                'success': True
            }
            
        except Exception as e:
            return {
                'question': user_question,
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
                'success': False
            }
    
    def _understand_question(self, question: str) -> Dict[str, Any]:
        """Parse the question to understand intent."""
        prompt = f"""Analyze this criminal justice question and extract key information.
Question: "{question}"

Identify:
1. Query type: "count", "list", "trend", or "detail"
2. Crime type: "sex", "drug", "gun", "assault", "theft", or "general"
3. Location: Extract city name (Hartford, New Haven, Meriden, Bridgeport, etc.) or "all"
4. Time period: "recent" (30 days), "6months", "year", or "all"
5. Data source: "conviction", "pending", "inmate", or "general"

Respond with ONLY a JSON object like:
{{"query_type": "count", "crime_type": "sex", "location": "meriden", "time_period": "6months", "data_source": "conviction"}}
"""
        
        response = self._call_ollama(prompt)
        
        # Try to parse JSON from response
        try:
            # Find JSON in response
            start = response.find('{')
            end = response.rfind('}') + 1
            if start >= 0 and end > start:
                return json.loads(response[start:end])
        except:
            pass
        
        # Fallback parsing
        question_lower = question.lower()
        understanding = {
            'query_type': 'count' if 'how many' in question_lower else 'list',
            'crime_type': 'general',
            'location': 'all',
            'time_period': 'all',
            'data_source': 'general'
        }
        
        # Simple keyword detection
        if 'sex' in question_lower:
            understanding['crime_type'] = 'sex'
        elif 'drug' in question_lower:
            understanding['crime_type'] = 'drug'
        elif 'gun' in question_lower or 'weapon' in question_lower:
            understanding['crime_type'] = 'gun'
        
        # Location detection
        for city in ['hartford', 'new haven', 'meriden', 'bridgeport', 'waterbury']:
            if city in question_lower:
                understanding['location'] = city
                break
        
        # Time period
        if 'recent' in question_lower or 'last month' in question_lower:
            understanding['time_period'] = 'recent'
        elif '6 month' in question_lower or 'six month' in question_lower:
            understanding['time_period'] = '6months'
        elif 'year' in question_lower:
            understanding['time_period'] = 'year'
        
        # Data source
        if 'inmate' in question_lower or 'doc' in question_lower:
            understanding['data_source'] = 'inmate'
        elif 'pending' in question_lower:
            understanding['data_source'] = 'pending'
        elif 'convict' in question_lower:
            understanding['data_source'] = 'conviction'
        
        return understanding
    
    def _execute_appropriate_query(self, question: str, understanding: Dict[str, Any]) -> Any:
        """Execute the appropriate query based on understanding."""
        
        crime_type = understanding.get('crime_type', 'general')
        location = understanding.get('location', 'all')
        query_type = understanding.get('query_type', 'count')
        data_source = understanding.get('data_source', 'general')
        
        # Handle different query types
        if data_source == 'inmate':
            return self.query_system.inmate_count_by_offense(crime_type)
        
        elif crime_type == 'sex' and query_type == 'count':
            if location != 'all':
                return self.query_system.sex_offenses_by_location(location)
            else:
                # Get total for all locations
                return self._get_total_by_crime_type('sex')
        
        elif crime_type == 'gun' and query_type == 'count':
            if location != 'all':
                return self.query_system.gun_charges_by_location(location)
            else:
                return self._get_total_by_crime_type('gun')
        
        elif crime_type == 'drug' and query_type == 'list':
            return self.query_system.drug_arrests_recent(
                location if location != 'all' else None
            )
        
        else:
            # Default to summary stats
            return self.query_system.get_summary_stats()
    
    def _get_total_by_crime_type(self, crime_type: str) -> Dict[str, Any]:
        """Get totals for a crime type across all locations."""
        if crime_type == 'sex':
            query = """
            SELECT COUNT(DISTINCT c.docket_no) as total_cases
            FROM conviction c
            JOIN conviction_charges ch ON c.id = ch.case_id
            WHERE LOWER(ch.description) LIKE '%sex%'
            """
        elif crime_type == 'gun':
            query = """
            SELECT COUNT(DISTINCT c.docket_no) as total_cases
            FROM conviction c
            JOIN conviction_charges ch ON c.id = ch.case_id
            WHERE ch.statute LIKE '53a-216%' OR ch.statute LIKE '53a-217%'
            OR LOWER(ch.description) LIKE '%gun%' OR LOWER(ch.description) LIKE '%firearm%'
            """
        else:
            query = "SELECT COUNT(*) as total_cases FROM conviction"
        
        self.query_system.cursor.execute(query)
        result = self.query_system.cursor.fetchone()
        
        return {
            'crime_type': crime_type,
            'total_cases': result[0],
            'scope': 'all locations'
        }
    
    def _interpret_results(self, question: str, results: Any, understanding: Dict[str, Any]) -> str:
        """Use LLM to interpret results naturally."""
        
        # Format results for the prompt
        if isinstance(results, dict):
            results_text = json.dumps(results, indent=2)
        elif isinstance(results, list):
            results_text = f"Found {len(results)} records:\n"
            for i, r in enumerate(results[:3]):
                results_text += f"{i+1}. {json.dumps(r, indent=2)}\n"
        else:
            results_text = str(results)
        
        prompt = f"""You are helping interpret criminal justice data for Connecticut.

User asked: "{question}"

Query understanding: {json.dumps(understanding)}

Results from database:
{results_text}

Important context:
- Courts like "Meriden GA 7" handle cases from multiple towns, not just Meriden
- "total_by_local_police" shows cases where the arrest was made by that city's police specifically
- Many sex offense cases are for "failure to register" rather than new offenses
- Dates are stored as text (MM/DD/YYYY) so time filtering uses year patterns

Provide a clear, accurate response that:
1. Directly answers their question with specific numbers
2. Clarifies whether the data is from the court (multiple towns) or just local police
3. Notes the time period covered
4. Is concise (under 150 words)

Response:"""
        
        response = self._call_ollama(prompt, temperature=0.3)
        return response.strip()
    
    def close(self):
        """Clean up resources."""
        self.query_system.close()


# Direct query interface without LLM
class DirectQueryBot:
    """Direct query interface that doesn't require LLM."""
    
    def __init__(self, db_path: str = 'records.db'):
        self.query_system = CTJusticeQuerySystem(db_path)
    
    def ask(self, question: str) -> str:
        """Process question and return formatted answer."""
        q_lower = question.lower()
        
        # Parse question type
        if 'how many' in q_lower:
            # Count queries
            if 'sex' in q_lower and 'meriden' in q_lower:
                # Check if asking about recent or all time
                if any(term in q_lower for term in ['recent', 'last', 'month', 'year']):
                    result = self.query_system.sex_offenses_by_location('meriden', days_back=180)
                else:
                    result = self.query_system.sex_offenses_by_location('meriden', days_back=None)
                
                # Fixed to use the correct keys
                return f"There are {result['total_cases']} sex offense cases in Meriden court {result['time_period']} ({result['total_by_local_police']} by Meriden police specifically)."
            
            elif 'gun' in q_lower and 'hartford' in q_lower:
                result = self.query_system.gun_charges_by_location('hartford')
                return f"There are {result['total_cases']} gun-related convictions in Hartford."
            
            elif 'inmate' in q_lower and 'sex' in q_lower:
                result = self.query_system.inmate_count_by_offense('sex')
                return f"There are {result['total_inmates']} inmates currently incarcerated for sex offenses."
            
            else:
                stats = self.query_system.get_summary_stats()
                return f"Database contains {stats['total_convictions']:,} convictions, {stats['total_pending']:,} pending cases, and {stats['total_inmates']:,} inmates."
        
        elif 'show' in q_lower or 'list' in q_lower:
            # List queries
            if 'drug' in q_lower:
                location = None
                for city in ['new haven', 'hartford', 'bridgeport', 'meriden']:
                    if city in q_lower:
                        location = city
                        break
                
                results = self.query_system.drug_arrests_recent(location, limit=5)
                if results:
                    response = f"Found {len(results)} recent drug cases"
                    if location:
                        response += f" in {location.title()}"
                    response += ":\n"
                    for i, case in enumerate(results[:3]):
                        response += f"{i+1}. {case['name']} - {case['description'][:50]}...\n"
                    return response
                else:
                    return "No recent drug cases found."
        
        # Default response
        return "I can help with questions like:\n- How many sex offenses in Meriden?\n- How many gun charges in Hartford?\n- Show me recent drug arrests in New Haven\n- How many inmates are in for sex offenses?"
    
    def close(self):
        self.query_system.close()


if __name__ == "__main__":
    print("QUERY SYSTEM TEST")
    print("=" * 60)
    
    # Test direct bot (no LLM needed)
    print("\n1. Testing Direct Query Bot:")
    direct_bot = DirectQueryBot()
    
    test_questions = [
        "How many sex offense convictions were in Meriden?",
        "How many gun charges in Hartford?",
        "Show me drug arrests in New Haven",
        "How many inmates are in for sex offenses?"
    ]
    
    for q in test_questions:
        print(f"\nQ: {q}")
        print(f"A: {direct_bot.ask(q)}")
    
    direct_bot.close()
    
    # Test LLM bot if available
    print("\n\n2. Testing LLM Bot:")
    try:
        llm_bot = LLMQueryBot()
        
        result = llm_bot.process_query("How many people were convicted of sex offenses in Meriden in the last 6 months?")
        
        if result['success']:
            print(f"Question: {result['question']}")
            print(f"Understanding: {result['understanding']}")
            print(f"Results: {result['results']}")
            print(f"\nInterpretation: {result['interpretation']}")
        else:
            print(f"Error: {result['error']}")
        
        llm_bot.close()
        
    except ConnectionError:
        print("Ollama not running. Skipping LLM test.")
