# llm_query_examples.py
"""
Example queries that an LLM could use with the simplified schema with separate charge tables
Shows how to query the relational structure while maintaining simplicity
"""

import sqlite3
import json

# Example queries for LLM to answer user questions

# 1. "Who was convicted of assault in the last year?"
ASSAULT_CONVICTIONS_QUERY = """
SELECT DISTINCT
    c.docket_no,
    c.defendant_name,
    c.sentenced_date,
    cc.description,
    c.overall_sentence
FROM convictions c
JOIN conviction_charges cc ON c.id = cc.case_id
WHERE cc.description LIKE '%assault%'
    AND c.sentenced_date >= date('now', '-1 year')
    AND c.version = (SELECT MAX(version) FROM convictions c2 WHERE c2.docket_no = c.docket_no)
ORDER BY c.sentenced_date DESC;
"""

# 2. "Show me all pending cases with bonds over $10,000"
HIGH_BOND_PENDING_QUERY = """
SELECT DISTINCT
    p.docket_no,
    p.defendant_name,
    p.bond_amount,
    p.bond_type,
    p.next_hearing_date,
    COUNT(pc.id) as charge_count
FROM pending p
LEFT JOIN pending_charges pc ON p.id = pc.case_id
WHERE CAST(REPLACE(REPLACE(p.bond_amount, '

# Example of how an LLM would use these queries
class LLMQueryInterface:
    def __init__(self, db_path='records.db'):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
    
    def answer_question(self, question):
        """
        Example method showing how an LLM would map questions to queries
        """
        question_lower = question.lower()
        
        if 'assault' in question_lower and 'convicted' in question_lower:
            self.cursor.execute(ASSAULT_CONVICTIONS_QUERY)
            results = self.cursor.fetchall()
            return f"Found {len(results)} assault convictions in the last year."
        
        elif 'bond' in question_lower and ('high' in question_lower or '10000' in question):
            self.cursor.execute(HIGH_BOND_PENDING_QUERY)
            results = self.cursor.fetchall()
            return f"Found {len(results)} pending cases with bonds over $10,000."
        
        elif 'paid' in question_lower and 'percent' in question_lower:
            self.cursor.execute(PAYMENT_STATISTICS_QUERY)
            result = self.cursor.fetchone()
            return f"{result[2]}% of convicted cases have been paid in full ({result[1]} out of {result[0]} cases)."
        
        # ... and so on for other question patterns
    
    def get_case_charges(self, docket_no):
        """
        Extract and format charges from JSON for easy reading
        """
        query = "SELECT charges_json FROM convictions WHERE docket_no = ? ORDER BY version DESC LIMIT 1"
        self.cursor.execute(query, (docket_no,))
        result = self.cursor.fetchone()
        
        if result and result[0]:
            charges = json.loads(result[0])
            formatted_charges = []
            for i, charge in enumerate(charges, 1):
                formatted_charges.append(
                    f"{i}. {charge.get('Statute', 'Unknown')} - {charge.get('Description', 'No description')}"
                    f"\n   Plea: {charge.get('Plea', 'N/A')}, Verdict: {charge.get('Verdict Finding', 'N/A')}"
                )
            return "\n".join(formatted_charges)
        return "No charges found"

# Benefits of this simplified schema for LLM usage:
# 1. No complex JOINs needed - all data is in one table per case type
# 2. Summary fields (charge_statutes_list, charge_descriptions_list) allow text searching without JSON parsing
# 3. Financial totals are pre-calculated
# 4. Views provide clean, latest-version-only data
# 5. JSON storage maintains detail while keeping structure simple
# 6. Versioning is transparent - LLM can ignore it or use it as needed, ''), ',', '') AS REAL) > 10000
    AND p.version = (SELECT MAX(version) FROM pending p2 WHERE p2.docket_no = p.docket_no)
GROUP BY p.id
ORDER BY CAST(REPLACE(REPLACE(p.bond_amount, '

# Example of how an LLM would use these queries
class LLMQueryInterface:
    def __init__(self, db_path='records.db'):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
    
    def answer_question(self, question):
        """
        Example method showing how an LLM would map questions to queries
        """
        question_lower = question.lower()
        
        if 'assault' in question_lower and 'convicted' in question_lower:
            self.cursor.execute(ASSAULT_CONVICTIONS_QUERY)
            results = self.cursor.fetchall()
            return f"Found {len(results)} assault convictions in the last year."
        
        elif 'bond' in question_lower and ('high' in question_lower or '10000' in question):
            self.cursor.execute(HIGH_BOND_PENDING_QUERY)
            results = self.cursor.fetchall()
            return f"Found {len(results)} pending cases with bonds over $10,000."
        
        elif 'paid' in question_lower and 'percent' in question_lower:
            self.cursor.execute(PAYMENT_STATISTICS_QUERY)
            result = self.cursor.fetchone()
            return f"{result[2]}% of convicted cases have been paid in full ({result[1]} out of {result[0]} cases)."
        
        # ... and so on for other question patterns
    
    def get_case_charges(self, docket_no):
        """
        Extract and format charges from JSON for easy reading
        """
        query = "SELECT charges_json FROM convictions WHERE docket_no = ? ORDER BY version DESC LIMIT 1"
        self.cursor.execute(query, (docket_no,))
        result = self.cursor.fetchone()
        
        if result and result[0]:
            charges = json.loads(result[0])
            formatted_charges = []
            for i, charge in enumerate(charges, 1):
                formatted_charges.append(
                    f"{i}. {charge.get('Statute', 'Unknown')} - {charge.get('Description', 'No description')}"
                    f"\n   Plea: {charge.get('Plea', 'N/A')}, Verdict: {charge.get('Verdict Finding', 'N/A')}"
                )
            return "\n".join(formatted_charges)
        return "No charges found"

# Benefits of this simplified schema for LLM usage:
# 1. No complex JOINs needed - all data is in one table per case type
# 2. Summary fields (charge_statutes_list, charge_descriptions_list) allow text searching without JSON parsing
# 3. Financial totals are pre-calculated
# 4. Views provide clean, latest-version-only data
# 5. JSON storage maintains detail while keeping structure simple
# 6. Versioning is transparent - LLM can ignore it or use it as needed, ''), ',', '') AS REAL) DESC;
"""

# 3. "What percentage of convicted cases have been paid in full?"
PAYMENT_STATISTICS_QUERY = """
SELECT 
    COUNT(DISTINCT docket_no) as total_cases,
    SUM(CASE WHEN payment_status = 'PAID IN FULL' THEN 1 ELSE 0 END) as paid_in_full,
    ROUND(100.0 * SUM(CASE WHEN payment_status = 'PAID IN FULL' THEN 1 ELSE 0 END) / COUNT(*), 2) as percent_paid
FROM convictions c
WHERE c.version = (SELECT MAX(version) FROM convictions c2 WHERE c2.docket_no = c.docket_no);
"""

# 4. "Find all cases with modified sentences"
MODIFIED_SENTENCES_QUERY = """
SELECT 
    c.docket_no,
    c.defendant_name,
    c.sentenced_date,
    c.overall_sentence,
    c.modified_sentence_text,
    c.modified_sentence_date,
    COUNT(cc.id) as charge_count
FROM convictions c
LEFT JOIN conviction_charges cc ON c.id = cc.case_id
WHERE c.has_modified_sentence = 1
    AND c.version = (SELECT MAX(version) FROM convictions c2 WHERE c2.docket_no = c.docket_no)
GROUP BY c.id;
"""

# 5. "Who has the most charges in a single case?"
MOST_CHARGES_QUERY = """
WITH charge_counts AS (
    SELECT 
        c.docket_no,
        c.defendant_name,
        COUNT(cc.id) as charge_count,
        'Convicted' as case_type
    FROM convictions c
    JOIN conviction_charges cc ON c.id = cc.case_id
    WHERE c.version = (SELECT MAX(version) FROM convictions c2 WHERE c2.docket_no = c.docket_no)
    GROUP BY c.id
    
    UNION ALL
    
    SELECT 
        p.docket_no,
        p.defendant_name,
        COUNT(pc.id) as charge_count,
        'Pending' as case_type
    FROM pending p
    JOIN pending_charges pc ON p.id = pc.case_id
    WHERE p.version = (SELECT MAX(version) FROM pending p2 WHERE p2.docket_no = p.docket_no)
    GROUP BY p.id
)
SELECT * FROM charge_counts
ORDER BY charge_count DESC
LIMIT 10;
"""

# 6. "Show all DUI cases from 2024"
DUI_CASES_2024_QUERY = """
SELECT DISTINCT
    c.docket_no,
    c.defendant_name,
    c.arrest_date,
    cc.statute,
    cc.description,
    c.overall_sentence,
    c.total_cost,
    c.payment_status
FROM convictions c
JOIN conviction_charges cc ON c.id = cc.case_id
WHERE (cc.description LIKE '%DUI%' 
       OR cc.description LIKE '%driving under%'
       OR cc.statute LIKE '%14-227a%')
    AND c.arrest_date LIKE '%2024%'
    AND c.version = (SELECT MAX(version) FROM convictions c2 WHERE c2.docket_no = c.docket_no);
"""

# 7. "What are the unpaid fines for each court?"
UNPAID_BY_COURT_QUERY = """
SELECT 
    c.court,
    COUNT(DISTINCT c.docket_no) as total_cases,
    SUM(CAST(c.total_cost AS REAL) - CAST(c.amount_paid AS REAL)) as total_unpaid
FROM convictions c
WHERE c.payment_status != 'PAID IN FULL'
    AND c.version = (SELECT MAX(version) FROM convictions c2 WHERE c2.docket_no = c.docket_no)
GROUP BY c.court
ORDER BY total_unpaid DESC;
"""

# 8. "Find defendants with multiple cases"
REPEAT_DEFENDANTS_QUERY = """
WITH all_defendants AS (
    SELECT defendant_name, docket_no, 'Conviction' as case_type
    FROM convictions
    WHERE version = (SELECT MAX(version) FROM convictions c2 WHERE c2.docket_no = convictions.docket_no)
    
    UNION ALL
    
    SELECT defendant_name, docket_no, 'Pending' as case_type
    FROM pending
    WHERE version = (SELECT MAX(version) FROM pending p2 WHERE p2.docket_no = pending.docket_no)
)
SELECT 
    defendant_name,
    COUNT(DISTINCT docket_no) as case_count,
    GROUP_CONCAT(docket_no || ' (' || case_type || ')', ', ') as all_cases
FROM all_defendants
GROUP BY defendant_name
HAVING case_count > 1
ORDER BY case_count DESC;
"""

# 9. "Show details of all charges for a specific case"
def get_case_with_charges(docket_no):
    """
    Query to get complete case information including all charges
    """
    return f"""
    SELECT 
        c.*,
        GROUP_CONCAT(
            cc.statute || ' - ' || cc.description || 
            ' (Fine: ' || cc.fine || ', Fees: ' || cc.fees || ')',
            '\n'
        ) as all_charges
    FROM convictions c
    LEFT JOIN conviction_charges cc ON c.id = cc.case_id
    WHERE c.docket_no = '{docket_no}'
        AND c.version = (SELECT MAX(version) FROM convictions WHERE docket_no = '{docket_no}')
    GROUP BY c.id;
    """

# 10. "What cases are scheduled for hearing this week?"
UPCOMING_HEARINGS_QUERY = """
SELECT 
    p.docket_no,
    p.defendant_name,
    p.next_hearing_date,
    p.hearing_purpose,
    p.custody_status,
    p.bond_amount,
    COUNT(pc.id) as charge_count
FROM pending p
LEFT JOIN pending_charges pc ON p.id = pc.case_id
WHERE p.next_hearing_date BETWEEN date('now') AND date('now', '+7 days')
    AND p.version = (SELECT MAX(version) FROM pending p2 WHERE p2.docket_no = p.docket_no)
GROUP BY p.id
ORDER BY p.next_hearing_date;
"""

# Example of how an LLM would use these queries
class LLMQueryInterface:
    def __init__(self, db_path='records.db'):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
    
    def answer_question(self, question):
        """
        Example method showing how an LLM would map questions to queries
        """
        question_lower = question.lower()
        
        if 'assault' in question_lower and 'convicted' in question_lower:
            self.cursor.execute(ASSAULT_CONVICTIONS_QUERY)
            results = self.cursor.fetchall()
            return f"Found {len(results)} assault convictions in the last year."
        
        elif 'bond' in question_lower and ('high' in question_lower or '10000' in question):
            self.cursor.execute(HIGH_BOND_PENDING_QUERY)
            results = self.cursor.fetchall()
            return f"Found {len(results)} pending cases with bonds over $10,000."
        
        elif 'paid' in question_lower and 'percent' in question_lower:
            self.cursor.execute(PAYMENT_STATISTICS_QUERY)
            result = self.cursor.fetchone()
            return f"{result[2]}% of convicted cases have been paid in full ({result[1]} out of {result[0]} cases)."
        
        # ... and so on for other question patterns
    
    def get_case_charges(self, docket_no):
        """
        Extract and format charges from JSON for easy reading
        """
        query = "SELECT charges_json FROM convictions WHERE docket_no = ? ORDER BY version DESC LIMIT 1"
        self.cursor.execute(query, (docket_no,))
        result = self.cursor.fetchone()
        
        if result and result[0]:
            charges = json.loads(result[0])
            formatted_charges = []
            for i, charge in enumerate(charges, 1):
                formatted_charges.append(
                    f"{i}. {charge.get('Statute', 'Unknown')} - {charge.get('Description', 'No description')}"
                    f"\n   Plea: {charge.get('Plea', 'N/A')}, Verdict: {charge.get('Verdict Finding', 'N/A')}"
                )
            return "\n".join(formatted_charges)
        return "No charges found"

# Benefits of this simplified schema for LLM usage:
# 1. No complex JOINs needed - all data is in one table per case type
# 2. Summary fields (charge_statutes_list, charge_descriptions_list) allow text searching without JSON parsing
# 3. Financial totals are pre-calculated
# 4. Views provide clean, latest-version-only data
# 5. JSON storage maintains detail while keeping structure simple
# 6. Versioning is transparent - LLM can ignore it or use it as needed
