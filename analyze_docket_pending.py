# check_docket_not_in_pending.py
# Simple script to show cases that are in docket but NOT in pending

import sqlite3

def main():
    conn = sqlite3.connect('records.db')
    cursor = conn.cursor()
    
    print("=== Cases in DOCKET table but NOT in PENDING table ===\n")
    
    # Get cases in docket that are NOT in pending
    cursor.execute("""
        SELECT 
            d.docket_no, 
            d.last_first_name,
            d.arrest_date,
            d.hearing_date,
            d.court,
            d.docket_type
        FROM docket d
        WHERE d.docket_no NOT IN (
            SELECT DISTINCT docket_no FROM pending
        )
        ORDER BY d.docket_no
    """)
    
    missing_cases = cursor.fetchall()
    
    print(f"Found {len(missing_cases)} docket cases that are NOT in pending:\n")
    
    if missing_cases:
        # Show all of them
        for i, case in enumerate(missing_cases, 1):
            docket_no, name, arrest_date, hearing_date, court, docket_type = case
            print(f"{i}. Docket: {docket_no}")
            print(f"   Name: {name}")
            print(f"   Arrest Date: {arrest_date}")
            print(f"   Hearing Date: {hearing_date}")
            print(f"   Court: {court}")
            print(f"   Type: {docket_type}")
            print()
    else:
        print("All docket cases exist in pending!")
    
    # Quick check - are these in conviction instead?
    if missing_cases:
        print("\n=== Checking if these are in CONVICTION table ===\n")
        
        for case in missing_cases:
            docket_no = case[0]
            cursor.execute("""
                SELECT sentenced_date, last_first_name 
                FROM conviction 
                WHERE docket_no = ?
            """, (docket_no,))
            
            result = cursor.fetchone()
            if result:
                print(f"{docket_no} - FOUND IN CONVICTION (sentenced: {result[0]})")
            else:
                print(f"{docket_no} - NOT in conviction either")
    
    conn.close()

if __name__ == "__main__":
    main()
