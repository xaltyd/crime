# simple_query_system.py
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Any

class CTJusticeQuerySystem:
    """A simplified, working query system for Connecticut criminal justice data."""
    
    def __init__(self, db_path: str = 'records.db'):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
    
    def close(self):
        self.conn.close()
    
    def sex_offenses_by_location(self, location: str, days_back: int = 180, include_pending: bool = True) -> Dict[str, Any]:
        """Get sex offense convictions AND pending cases by location."""
        # Map common location names to database values
        location_map = {
            'meriden': ('Meriden GA 7', 'LOCAL POLICE MERIDEN'),
            'hartford': ('Hartford GA 14', 'LOCAL POLICE HARTFORD'),
            'new haven': ('New Haven GA 23', 'LOCAL POLICE NEW HAVEN'),
            'bridgeport': ('Bridgeport GA 2', 'LOCAL POLICE BRIDGEPORT')
        }
        
        court, agency = location_map.get(location.lower(), (location, location))
        
        # Build query based on whether we want time filtering
        if days_back and days_back < 365 * 10:  # If looking for recent cases
            # For recent cases, use year-based filtering since dates are text
            current_year = datetime.now().year
            last_year = current_year - 1
            
            # Conviction query
            conviction_query = """
            SELECT COUNT(DISTINCT c.docket_no) as total_cases,
                   COUNT(ch.id) as total_charges,
                   GROUP_CONCAT(ch.description, '|') as charge_types,
                   COUNT(DISTINCT CASE WHEN c.arresting_agency = ? THEN c.docket_no END) as by_local_police
            FROM conviction c
            JOIN conviction_charges ch ON c.id = ch.case_id
            WHERE (c.court = ? OR c.arresting_agency = ?)
            AND LOWER(ch.description) LIKE '%sex%'
            AND (c.sentenced_date LIKE '%/2024' OR c.sentenced_date LIKE '%/2025')
            """
            
            # Pending query
            pending_query = """
            SELECT COUNT(DISTINCT p.docket_no) as total_cases,
                   COUNT(pc.id) as total_charges,
                   GROUP_CONCAT(pc.description, '|') as charge_types,
                   COUNT(DISTINCT CASE WHEN p.arresting_agency = ? THEN p.docket_no END) as by_local_police
            FROM pending p
            JOIN pending_charges pc ON p.id = pc.case_id
            WHERE (p.court = ? OR p.arresting_agency = ?)
            AND LOWER(pc.description) LIKE '%sex%'
            AND (p.arrest_date LIKE '%/2024' OR p.arrest_date LIKE '%/2025')
            """
            
            time_period = f"in 2024-2025"
        else:
            # All time queries
            conviction_query = """
            SELECT COUNT(DISTINCT c.docket_no) as total_cases,
                   COUNT(ch.id) as total_charges,
                   GROUP_CONCAT(ch.description, '|') as charge_types,
                   COUNT(DISTINCT CASE WHEN c.arresting_agency = ? THEN c.docket_no END) as by_local_police
            FROM conviction c
            JOIN conviction_charges ch ON c.id = ch.case_id
            WHERE (c.court = ? OR c.arresting_agency = ?)
            AND LOWER(ch.description) LIKE '%sex%'
            """
            
            pending_query = """
            SELECT COUNT(DISTINCT p.docket_no) as total_cases,
                   COUNT(pc.id) as total_charges,
                   GROUP_CONCAT(pc.description, '|') as charge_types,
                   COUNT(DISTINCT CASE WHEN p.arresting_agency = ? THEN p.docket_no END) as by_local_police
            FROM pending p
            JOIN pending_charges pc ON p.id = pc.case_id
            WHERE (p.court = ? OR p.arresting_agency = ?)
            AND LOWER(pc.description) LIKE '%sex%'
            """
            
            time_period = "all time"
        
        # Execute conviction query
        self.cursor.execute(conviction_query, (agency, court, agency))
        conviction_result = self.cursor.fetchone()
        
        # Execute pending query if requested
        if include_pending:
            self.cursor.execute(pending_query, (agency, court, agency))
            pending_result = self.cursor.fetchone()
        else:
            pending_result = (0, 0, None, 0)
        
        return {
            'location': location,
            'court': court,
            'conviction_cases': conviction_result[0] if conviction_result[0] else 0,
            'conviction_by_local_police': conviction_result[3] if conviction_result[3] else 0,
            'pending_cases': pending_result[0] if pending_result[0] else 0,
            'pending_by_local_police': pending_result[3] if pending_result[3] else 0,
            'total_cases': (conviction_result[0] or 0) + (pending_result[0] or 0),
            'total_by_local_police': (conviction_result[3] or 0) + (pending_result[3] or 0),
            'time_period': time_period,
            'common_charges': conviction_result[2][:200] if conviction_result[2] else None
        }
    
    def gun_charges_by_location(self, location: str, days_back: int = 365) -> Dict[str, Any]:
        """Get gun-related charges by location."""
        location_map = {
            'hartford': ('Hartford GA 14', 'LOCAL POLICE HARTFORD'),
            'new haven': ('New Haven GA 23', 'LOCAL POLICE NEW HAVEN'),
            'bridgeport': ('Bridgeport GA 2', 'LOCAL POLICE BRIDGEPORT'),
            'meriden': ('Meriden GA 7', 'LOCAL POLICE MERIDEN')
        }
        
        court, agency = location_map.get(location.lower(), (location, location))
        
        query = """
        SELECT COUNT(DISTINCT c.docket_no) as total_cases,
               COUNT(ch.id) as total_charges,
               GROUP_CONCAT(ch.statute || ' - ' || ch.description, '|') as charges
        FROM conviction c
        JOIN conviction_charges ch ON c.id = ch.case_id
        WHERE (c.court = ? OR c.arresting_agency = ?)
        AND (ch.statute LIKE '53a-216%' OR ch.statute LIKE '53a-217%' 
             OR LOWER(ch.description) LIKE '%firearm%' 
             OR LOWER(ch.description) LIKE '%gun%'
             OR LOWER(ch.description) LIKE '%pistol%'
             OR LOWER(ch.description) LIKE '%weapon%')
        """
        
        self.cursor.execute(query, (court, agency))
        result = self.cursor.fetchone()
        
        return {
            'location': location,
            'total_cases': result[0],
            'total_charges': result[1],
            'sample_charges': result[2][:300] if result[2] else None
        }
    
    def drug_arrests_recent(self, location: str = None, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent drug-related arrests."""
        params = []
        location_clause = ""
        
        if location:
            location_map = {
                'new haven': ('New Haven GA 23', 'LOCAL POLICE NEW HAVEN'),
                'hartford': ('Hartford GA 14', 'LOCAL POLICE HARTFORD'),
                'bridgeport': ('Bridgeport GA 2', 'LOCAL POLICE BRIDGEPORT'),
                'meriden': ('Meriden GA 7', 'LOCAL POLICE MERIDEN')
            }
            court, agency = location_map.get(location.lower(), (location, location))
            location_clause = "AND (c.court = ? OR c.arresting_agency = ?)"
            params.extend([court, agency])
        
        query = f"""
        SELECT c.docket_no, c.last_first_name, c.arrest_date, c.sentenced_date,
               c.court, c.arresting_agency, ch.statute, ch.description
        FROM conviction c
        JOIN conviction_charges ch ON c.id = ch.case_id
        WHERE (ch.statute LIKE '21a-%' 
               OR LOWER(ch.description) LIKE '%drug%'
               OR LOWER(ch.description) LIKE '%narcotic%'
               OR LOWER(ch.description) LIKE '%cocaine%'
               OR LOWER(ch.description) LIKE '%heroin%'
               OR LOWER(ch.description) LIKE '%marijuana%')
        {location_clause}
        ORDER BY c.id DESC
        LIMIT ?
        """
        
        params.append(limit)
        self.cursor.execute(query, params)
        
        results = []
        for row in self.cursor.fetchall():
            results.append({
                'docket_no': row[0],
                'name': row[1],
                'arrest_date': row[2],
                'sentenced_date': row[3],
                'court': row[4],
                'arresting_agency': row[5],
                'statute': row[6],
                'description': row[7]
            })
        
        return results
    
    def inmate_count_by_offense(self, offense_type: str) -> Dict[str, Any]:
        """Count inmates by offense type."""
        offense_patterns = {
            'sex': ['%sex%', '%rape%', '%assault%1st%', '%assault%2nd%', '%assault%3rd%'],
            'drug': ['%drug%', '%narcotic%', '%heroin%', '%cocaine%'],
            'gun': ['%firearm%', '%weapon%', '%pistol%', '%gun%'],
            'assault': ['%assault%', '%battery%']
        }
        
        patterns = offense_patterns.get(offense_type.lower(), [f'%{offense_type}%'])
        
        where_clauses = " OR ".join(["LOWER(controlling_offense) LIKE ?" for _ in patterns])
        query = f"""
        SELECT COUNT(*) as total_inmates,
               COUNT(DISTINCT current_location) as facilities,
               GROUP_CONCAT(controlling_offense, '|') as offenses
        FROM dept_of_correction
        WHERE {where_clauses}
        """
        
        self.cursor.execute(query, patterns)
        result = self.cursor.fetchone()
        
        return {
            'offense_type': offense_type,
            'total_inmates': result[0],
            'facilities_count': result[1],
            'sample_offenses': result[2][:200] if result[2] else None
        }
    
    def get_summary_stats(self) -> Dict[str, Any]:
        """Get overall database statistics."""
        stats = {}
        
        # Total convictions
        self.cursor.execute("SELECT COUNT(DISTINCT docket_no) FROM conviction")
        stats['total_convictions'] = self.cursor.fetchone()[0]
        
        # Total pending cases
        self.cursor.execute("SELECT COUNT(DISTINCT docket_no) FROM pending")
        stats['total_pending'] = self.cursor.fetchone()[0]
        
        # Total inmates
        self.cursor.execute("SELECT COUNT(*) FROM dept_of_correction")
        stats['total_inmates'] = self.cursor.fetchone()[0]
        
        # Most common charges
        self.cursor.execute("""
            SELECT ch.description, COUNT(*) as count
            FROM conviction_charges ch
            GROUP BY ch.description
            ORDER BY count DESC
            LIMIT 5
        """)
        stats['top_charges'] = [{'charge': row[0], 'count': row[1]} for row in self.cursor.fetchall()]
        
        return stats


# Test the system
if __name__ == "__main__":
    print("CONNECTICUT CRIMINAL JUSTICE QUERY SYSTEM")
    print("=" * 60)
    
    query_system = CTJusticeQuerySystem()
    
    # Test 1: Sex offenses in Meriden
    print("\n1. Sex offenses in Meriden:")
    result = query_system.sex_offenses_by_location('meriden')
    print(f"   Total cases: {result['total_cases']}")
    print(f"   Total charges: {result['total_charges']}")
    
    # Test 2: Gun charges in Hartford
    print("\n2. Gun charges in Hartford:")
    result = query_system.gun_charges_by_location('hartford')
    print(f"   Total cases: {result['total_cases']}")
    print(f"   Total charges: {result['total_charges']}")
    
    # Test 3: Recent drug arrests in New Haven
    print("\n3. Recent drug arrests in New Haven:")
    results = query_system.drug_arrests_recent('new haven', limit=5)
    print(f"   Found {len(results)} recent cases")
    for i, case in enumerate(results[:3]):
        print(f"   {i+1}. {case['name']} - {case['description'][:50]}...")
    
    # Test 4: Inmates for sex offenses
    print("\n4. Inmates incarcerated for sex offenses:")
    result = query_system.inmate_count_by_offense('sex')
    print(f"   Total inmates: {result['total_inmates']}")
    
    # Test 5: Summary statistics
    print("\n5. Database Summary:")
    stats = query_system.get_summary_stats()
    print(f"   Total convictions: {stats['total_convictions']:,}")
    print(f"   Total pending cases: {stats['total_pending']:,}")
    print(f"   Total inmates: {stats['total_inmates']:,}")
    print(f"   Top charges:")
    for charge in stats['top_charges'][:3]:
        print(f"     - {charge['charge'][:40]}... ({charge['count']} cases)")
    
    query_system.close()
