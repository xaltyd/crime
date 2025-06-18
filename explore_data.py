# explore_data.py
import sqlite3
from collections import Counter

def explore_database(db_path='records.db'):
    """Explore the actual data structure and values in the database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("EXPLORING CT CRIMINAL JUSTICE DATABASE")
    print("=" * 60)
    
    # 1. Check court values
    print("\n1. COURT VALUES IN CONVICTION TABLE:")
    cursor.execute("SELECT DISTINCT court FROM conviction WHERE court IS NOT NULL LIMIT 20")
    courts = cursor.fetchall()
    for court in courts:
        print(f"  - {court[0]}")
    
    # 2. Check arresting agency values
    print("\n2. ARRESTING AGENCY VALUES:")
    cursor.execute("SELECT DISTINCT arresting_agency FROM conviction WHERE arresting_agency IS NOT NULL LIMIT 20")
    agencies = cursor.fetchall()
    for agency in agencies:
        print(f"  - {agency[0]}")
    
    # 3. Check for sex-related charges
    print("\n3. SEX-RELATED CHARGES:")
    cursor.execute("""
        SELECT COUNT(*) as count, ch.statute, ch.description
        FROM conviction c
        JOIN conviction_charges ch ON c.id = ch.case_id
        WHERE LOWER(ch.description) LIKE '%sex%'
        GROUP BY ch.statute, ch.description
        ORDER BY count DESC
        LIMIT 10
    """)
    sex_charges = cursor.fetchall()
    for count, statute, desc in sex_charges:
        print(f"  - {count:4d} cases: {statute} - {desc[:60]}...")
    
    # 4. Check date formats
    print("\n4. DATE FORMAT EXAMPLES:")
    cursor.execute("SELECT DISTINCT arrest_date FROM conviction WHERE arrest_date IS NOT NULL LIMIT 5")
    dates = cursor.fetchall()
    for date in dates:
        print(f"  - {date[0]}")
    
    # 5. Location analysis
    print("\n5. CASES BY LOCATION (using arresting_agency):")
    cursor.execute("""
        SELECT arresting_agency, COUNT(*) as count
        FROM conviction
        WHERE arresting_agency IS NOT NULL
        GROUP BY arresting_agency
        ORDER BY count DESC
        LIMIT 10
    """)
    locations = cursor.fetchall()
    for loc, count in locations:
        print(f"  - {loc}: {count} cases")
    
    # 6. Check if 'Meriden' appears anywhere
    print("\n6. SEARCHING FOR 'MERIDEN':")
    # In court field
    cursor.execute("SELECT COUNT(*) FROM conviction WHERE court LIKE '%Meriden%'")
    court_count = cursor.fetchone()[0]
    print(f"  - In court field: {court_count}")
    
    # In arresting_agency field
    cursor.execute("SELECT COUNT(*) FROM conviction WHERE arresting_agency LIKE '%Meriden%'")
    agency_count = cursor.fetchone()[0]
    print(f"  - In arresting_agency field: {agency_count}")
    
    # 7. Total counts
    print("\n7. TOTAL RECORD COUNTS:")
    tables = ['conviction', 'conviction_charges', 'pending', 'docket', 'dept_of_correction']
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  - {table}: {count:,} records")
        except:
            print(f"  - {table}: Not found")
    
    # 8. Sample queries that should work
    print("\n8. TESTING ACTUAL QUERIES:")
    
    # Query 1: All sex-related convictions
    cursor.execute("""
        SELECT COUNT(DISTINCT c.docket_no)
        FROM conviction c
        JOIN conviction_charges ch ON c.id = ch.case_id
        WHERE LOWER(ch.description) LIKE '%sex%'
    """)
    sex_count = cursor.fetchone()[0]
    print(f"  - Total convictions with 'sex' in description: {sex_count}")
    
    # Query 2: Recent convictions (last 30 days)
    cursor.execute("""
        SELECT COUNT(*)
        FROM conviction
        WHERE sentenced_date >= date('now', '-30 days')
        OR (sentenced_date LIKE '%2024' AND sentenced_date >= '11/06/2024')
    """)
    recent_count = cursor.fetchone()[0]
    print(f"  - Convictions in last 30 days: {recent_count}")
    
    conn.close()
    
    print("\n" + "=" * 60)
    print("INSIGHTS:")
    print("- Location data might be in 'arresting_agency' not 'court'")
    print("- Sex offenses are in description field, not statute codes")
    print("- Date format appears to be MM/DD/YYYY as text")
    print("- Consider searching both court and arresting_agency for location")


if __name__ == "__main__":
    explore_database()
