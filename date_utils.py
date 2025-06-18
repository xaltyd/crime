# date_utils.py
# Utility functions for consistent date handling across the project

from datetime import datetime

def parse_date(date_str):
    """
    Convert various date formats to ISO format (YYYY-MM-DD) for SQLite DATE columns.
    
    Args:
        date_str: Date string in various formats
        
    Returns:
        ISO formatted date string (YYYY-MM-DD) or None if invalid
    """
    if not date_str or date_str == 'NULL' or date_str == '':
        return None
    
    # Remove time portion if present
    if ' ' in str(date_str):
        date_str = str(date_str).split(' ')[0]
    
    # Try different date formats
    formats = [
        '%m/%d/%Y',    # 12/14/2019 (most common from the website)
        '%m/%d/%y',    # 12/14/19
        '%Y-%m-%d',    # Already in ISO format
        '%m-%d-%Y',    # 12-14-2019
        '%m-%d-%y',    # 12-14-19
        '%d/%m/%Y',    # 14/12/2019 (European format)
        '%Y/%m/%d',    # 2019/12/14
        '%b %d, %Y',   # Dec 14, 2019
        '%B %d, %Y',   # December 14, 2019
    ]
    
    date_str = str(date_str).strip()
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    # If we can't parse it, log and return None
    print(f"Warning: Could not parse date: {date_str}")
    return None

def format_date_for_display(iso_date):
    """
    Convert ISO date back to MM/DD/YYYY for display if needed.
    
    Args:
        iso_date: Date in YYYY-MM-DD format
        
    Returns:
        Date in MM/DD/YYYY format or original if can't parse
    """
    if not iso_date:
        return ''
    
    try:
        dt = datetime.strptime(iso_date, '%Y-%m-%d')
        return dt.strftime('%m/%d/%Y')
    except:
        return iso_date

def is_valid_date(date_str):
    """Check if a string can be parsed as a valid date."""
    return parse_date(date_str) is not None

def compare_dates(date1, date2):
    """
    Compare two date strings (handles various formats).
    
    Returns:
        -1 if date1 < date2
         0 if date1 == date2
         1 if date1 > date2
        None if either date is invalid
    """
    parsed1 = parse_date(date1)
    parsed2 = parse_date(date2)
    
    if not parsed1 or not parsed2:
        return None
    
    if parsed1 < parsed2:
        return -1
    elif parsed1 > parsed2:
        return 1
    else:
        return 0

# Test the functions
if __name__ == "__main__":
    test_dates = [
        '12/14/2019',
        '1/1/2020',
        '2020-01-01',
        '01-01-20',
        'invalid date',
        '',
        None,
        '6/9/2025 10:00 AM'
    ]
    
    print("Testing date parsing:")
    for date in test_dates:
        parsed = parse_date(date)
        print(f"  {date} -> {parsed}")
    
    print("\nTesting date comparison:")
    print(f"  12/14/2019 vs 1/1/2020: {compare_dates('12/14/2019', '1/1/2020')}")
    print(f"  2020-01-01 vs 1/1/2020: {compare_dates('2020-01-01', '1/1/2020')}")
