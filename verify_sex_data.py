# verify_sex_offense_data.py
import sqlite3
from datetime import datetime
from collections import Counter

def verify_sex_offense_data(db_path='records.db'):
    """Comprehensive verification of sex offense data for Meriden."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("COMPREHENSIVE SEX OFFENSE DATA VERIFICATION FOR MERIDEN")
    print("=" * 70)
    
    # 1. First, let's understand the date range of our data
    print("\n1. DATA DATE RANGE ANALYSIS:")
    
    # Check conviction date ranges
    cursor.execute("""
        SELECT 
            MIN(sentenced_date) as earliest,
            MAX(sentenced_date) as latest,
            COUNT(DISTINCT sentenced_date) as unique_dates
        FROM conviction
        WHERE sentenced_date IS NOT NULL
        AND sentenced_date != ''
    """)
    conv_dates = cursor.fetchone()
    print(f"\nConviction table date range:")
    print(f"  Earliest: {conv_dates[0]}")
    print(f"  Latest: {conv_dates[1]}")
    print(f"  Unique dates: {conv_dates[2]}")
    
    # Check pending date ranges
    cursor.execute("""
        SELECT 
            MIN(arrest_date) as earliest,
            MAX(arrest_date) as latest,
            COUNT(DISTINCT arrest_date) as unique_dates
        FROM pending
        WHERE arrest_date IS NOT NULL
        AND arrest_date != ''
    """)
    pend_dates = cursor.fetchone()
    print(f"\nPending table date range:")
    print(f"  Earliest: {pend_dates[0]}")
    print(f"  Latest: {pend_dates[1]}")
    print(f"  Unique dates: {pend_dates[2]}")
    
    # 2. Break down sex offenses by year
    print("\n\n2. SEX OFFENSES BY YEAR (CONVICTIONS):")
    
    # Extract year from date and count
    cursor.execute("""
        SELECT 
            SUBSTR(c.sentenced_date, -4) as year,
            COUNT(DISTINCT c.docket_no) as cases
        FROM conviction c
        JOIN conviction_charges ch ON c.id = ch.case_id
        WHERE c.court = 'Meriden GA 7'
        AND LOWER(ch.description) LIKE '%sex%'
        AND c.sentenced_date IS NOT NULL
        AND c.sentenced_date != ''
        AND LENGTH(c.sentenced_date) >= 4
        GROUP BY year
        ORDER BY year DESC
    """)
    
    print("\nCases by year (Meriden court):")
    yearly_total = 0
    for year, count in cursor.fetchall():
        if year and year.isdigit():
            print(f"  {year}: {count} cases")
            yearly_total += count
    print(f"\nTotal with valid dates: {yearly_total}")
    
    # 3. Check total without date filtering
    print("\n\n3. TOTAL SEX OFFENSE CASES (NO DATE FILTER):")
    
    # Conviction total
    cursor.execute("""
        SELECT COUNT(DISTINCT c.docket_no) as total
        FROM conviction c
        JOIN conviction_charges ch ON c.id = ch.case_id
        WHERE c.court = 'Meriden GA 7'
        AND LOWER(ch.description) LIKE '%sex%'
    """)
    conv_total = cursor.fetchone()[0]
    print(f"\nConviction sex offenses (Meriden court): {conv_total}")
    
    # Pending total
    cursor.execute("""
        SELECT COUNT(DISTINCT p.docket_no) as total
        FROM pending p
        JOIN pending_charges pc ON p.id = pc.case_id
        WHERE p.court = 'Meriden GA 7'
        AND LOWER(pc.description) LIKE '%sex%'
    """)
    pend_total = cursor.fetchone()[0]
    print(f"Pending sex offenses (Meriden court): {pend_total}")
    print(f"Combined total: {conv_total + pend_total}")
    
    # 4. Check if we're missing charges
    print("\n\n4. CHECKING FOR MISSING CHARGES:")
    
    # Count cases with and without charges
    cursor.execute("""
        SELECT 
            COUNT(DISTINCT c.docket_no) as total_cases,
            COUNT(DISTINCT CASE WHEN ch.id IS NOT NULL THEN c.docket_no END) as cases_with_charges
        FROM conviction c
        LEFT JOIN conviction_charges ch ON c.id = ch.case_id
        WHERE c.court = 'Meriden GA 7'
    """)
    case_stats = cursor.fetchone()
    print(f"\nTotal conviction cases in Meriden court: {case_stats[0]}")
    print(f"Cases with charges: {case_stats[1]}")
    print(f"Cases WITHOUT charges: {case_stats[0] - case_stats[1]}")
    
    # 5. Different search patterns
    print("\n\n5. TESTING DIFFERENT SEARCH PATTERNS:")
    
    patterns = [
        ('%sex%', 'Contains "sex"'),
        ('%sexual%', 'Contains "sexual"'),
        ('%rape%', 'Contains "rape"'),
        ('%assault%1st%', 'Assault 1st'),
        ('%assault%2nd%', 'Assault 2nd'),
        ('%child%', 'Contains "child"'),
        ('%minor%', 'Contains "minor"'),
        ('%register%', 'Contains "register"')
    ]
    
    print("\nConviction cases by pattern (Meriden court):")
    for pattern, description in patterns:
        cursor.execute("""
            SELECT COUNT(DISTINCT c.docket_no)
            FROM conviction c
            JOIN conviction_charges ch ON c.id = ch.case_id
            WHERE c.court = 'Meriden GA 7'
            AND LOWER(ch.description) LIKE ?
        """, (pattern,))
        count = cursor.fetchone()[0]
        print(f"  {description}: {count} cases")
    
    # 6. Sample of actual charges
    print("\n\n6. SAMPLE OF ACTUAL SEX-RELATED CHARGES:")
    cursor.execute("""
        SELECT ch.description, COUNT(*) as count
        FROM conviction c
        JOIN conviction_charges ch ON c.id = ch.case_id
        WHERE c.court = 'Meriden GA 7'
        AND LOWER(ch.description) LIKE '%sex%'
        GROUP BY ch.description
        ORDER BY count DESC
        LIMIT 10
    """)
    
    print("\nTop 10 sex-related charges:")
    for desc, count in cursor.fetchall():
        print(f"  {count:3d} - {desc}")
    
    # 7. Check by arresting agency
    print("\n\n7. BY ARRESTING AGENCY (LOCAL POLICE MERIDEN):")
    
    # Conviction by Meriden police
    cursor.execute("""
        SELECT COUNT(DISTINCT c.docket_no)
        FROM conviction c
        JOIN conviction_charges ch ON c.id = ch.case_id
        WHERE c.arresting_agency = 'LOCAL POLICE MERIDEN'
        AND LOWER(ch.description) LIKE '%sex%'
    """)
    conv_by_police = cursor.fetchone()[0]
    
    # Pending by Meriden police
    cursor.execute("""
        SELECT COUNT(DISTINCT p.docket_no)
        FROM pending p
        JOIN pending_charges pc ON p.id = pc.case_id
        WHERE p.arresting_agency = 'LOCAL POLICE MERIDEN'
        AND LOWER(pc.description) LIKE '%sex%'
    """)
    pend_by_police = cursor.fetchone()[0]
    
    print(f"\nConvictions by Meriden Police: {conv_by_police}")
    print(f"Pending by Meriden Police: {pend_by_police}")
    print(f"Total by Meriden Police: {conv_by_police + pend_by_police}")
    
    # 8. Debug the query being used
    print("\n\n8. DEBUGGING THE ACTUAL QUERY:")
    print("\nQuery for sex_offenses_by_location (all time):")
    
    # This mimics what simple_query_generator.py does
    court = 'Meriden GA 7'
    agency = 'LOCAL POLICE MERIDEN'
    
    # Conviction query
    cursor.execute("""
        SELECT COUNT(DISTINCT c.docket_no) as total_cases,
               COUNT(ch.id) as total_charges,
               COUNT(DISTINCT CASE WHEN c.arresting_agency = ? THEN c.docket_no END) as by_local_police
        FROM conviction c
        JOIN conviction_charges ch ON c.id = ch.case_id
        WHERE (c.court = ? OR c.arresting_agency = ?)
        AND LOWER(ch.description) LIKE '%sex%'
    """, (agency, court, agency))
    
    conv_result = cursor.fetchone()
    print(f"\nConviction results:")
    print(f"  Total cases: {conv_result[0]}")
    print(f"  Total charges: {conv_result[1]}")
    print(f"  By local police: {conv_result[2]}")
    
    # Pending query
    cursor.execute("""
        SELECT COUNT(DISTINCT p.docket_no) as total_cases,
               COUNT(pc.id) as total_charges,
               COUNT(DISTINCT CASE WHEN p.arresting_agency = ? THEN p.docket_no END) as by_local_police
        FROM pending p
        JOIN pending_charges pc ON p.id = pc.case_id
        WHERE (p.court = ? OR p.arresting_agency = ?)
        AND LOWER(pc.description) LIKE '%sex%'
    """, (agency, court, agency))
    
    pend_result = cursor.fetchone()
    print(f"\nPending results:")
    print(f"  Total cases: {pend_result[0]}")
    print(f"  Total charges: {pend_result[1]}")
    print(f"  By local police: {pend_result[2]}")
    
    print(f"\nCombined totals:")
    print(f"  Total cases: {conv_result[0] + pend_result[0]} (this is your 237)")
    print(f"  By local police: {conv_result[2] + pend_result[2]} (this is your 132)")
    
    # 9. Find cases that might be double-counted
    print("\n\n9. CHECKING THE 'OR' CONDITION:")
    print("\nThe query uses (court = ? OR arresting_agency = ?)")
    print("This might include cases from OTHER courts arrested by Meriden police!")
    
    cursor.execute("""
        SELECT c.court, COUNT(DISTINCT c.docket_no) as cases
        FROM conviction c
        JOIN conviction_charges ch ON c.id = ch.case_id
        WHERE c.arresting_agency = 'LOCAL POLICE MERIDEN'
        AND LOWER(ch.description) LIKE '%sex%'
        GROUP BY c.court
        ORDER BY cases DESC
    """)
    
    print("\nCourts handling Meriden police sex offense arrests:")
    for court, count in cursor.fetchall():
        print(f"  {court}: {count} cases")
    
    conn.close()
    
    print("\n" + "=" * 70)
    print("ANALYSIS SUMMARY:")
    print("1. The 237 number includes cases from BOTH:")
    print("   - Meriden GA 7 court (regardless of arresting agency)")
    print("   - Any court where LOCAL POLICE MERIDEN made the arrest")
    print("2. This explains why the number might seem different than expected")
    print("3. To get ONLY Meriden court cases, remove the OR condition")


if __name__ == "__main__":
    verify_sex_offense_data()
