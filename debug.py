#!/usr/bin/env python3
"""
Trace exactly what HTML content is being passed to the modified charges parser
"""

import re
from bs4 import BeautifulSoup

def analyze_html_content(html_file):
    """Analyze the HTML content to understand the structure"""
    
    with open(html_file, 'r', encoding='utf-8') as f:
        raw_html = f.read()
    
    print("=== RAW HTML ANALYSIS ===")
    print(f"Total HTML length: {len(raw_html)}")
    
    # Find the main charges table
    main_table_start = raw_html.find('id="cphBody_Datagrid1"')
    if main_table_start != -1:
        # Back up to find the <table tag
        main_table_start = raw_html.rfind('<table', 0, main_table_start)
        print(f"\nMain charges table starts at position: {main_table_start}")
        
        # Find the end of this table
        main_table_end = raw_html.find('</table>', main_table_start)
        if main_table_end != -1:
            main_table_end += 8  # Include </table>
            print(f"Main charges table ends at position: {main_table_end}")
            print(f"Main table length: {main_table_end - main_table_start}")
    
    # Find the modified charges section
    mod_section_start = raw_html.find('Modified Sentence Information')
    print(f"\nModified section starts at position: {mod_section_start}")
    
    # Find the modified charges table
    mod_table_start = raw_html.find('id="cphBody_DatagridModCharge"')
    if mod_table_start != -1:
        # Back up to find the <table tag
        mod_table_start = raw_html.rfind('<table', 0, mod_table_start)
        print(f"Modified charges table starts at position: {mod_table_start}")
        
        # Find the end of this table
        mod_table_end = raw_html.find('</table>', mod_table_start)
        if mod_table_end != -1:
            mod_table_end += 8  # Include </table>
            print(f"Modified charges table ends at position: {mod_table_end}")
            print(f"Modified table length: {mod_table_end - mod_table_start}")
            
            # Extract and save the modified table
            mod_table_html = raw_html[mod_table_start:mod_table_end]
            with open('modified_table_only.html', 'w', encoding='utf-8') as f:
                f.write(mod_table_html)
            print("\nModified table HTML saved to 'modified_table_only.html'")
            
            # Count rows in modified table
            row_count = mod_table_html.count('class="grdRow"') + mod_table_html.count('class=grdRow')
            print(f"Rows found in modified table: {row_count}")
    
    # Test what BeautifulSoup does to the HTML
    print("\n=== BEAUTIFULSOUP CONVERSION TEST ===")
    soup = BeautifulSoup(raw_html, 'html.parser')
    soup_html = str(soup)
    print(f"Original HTML length: {len(raw_html)}")
    print(f"After BeautifulSoup: {len(soup_html)}")
    print(f"Difference: {len(raw_html) - len(soup_html)} characters")
    
    # Check if the modified table is intact after BeautifulSoup
    soup_mod_table = soup.find('table', id='cphBody_DatagridModCharge')
    if soup_mod_table:
        soup_mod_html = str(soup_mod_table)
        print(f"\nModified table via BeautifulSoup: {len(soup_mod_html)} characters")
        
        # Count rows
        soup_rows = soup_mod_table.find_all('tr', class_=['grdRow', 'grdRowAlt'])
        print(f"Rows found via BeautifulSoup: {len(soup_rows)}")
        
        # Save BeautifulSoup version
        with open('modified_table_beautifulsoup.html', 'w', encoding='utf-8') as f:
            f.write(soup_mod_html)
        print("BeautifulSoup version saved to 'modified_table_beautifulsoup.html'")
    
    # Test the exact pattern used in the code
    print("\n=== TESTING ORIGINAL REGEX PATTERN ===")
    mod_table_pattern = r'Modified Sentence Information.*?<table[^>]*id="cphBody_DatagridModCharge"[^>]*>(.*?)</table>'
    mod_match = re.search(mod_table_pattern, raw_html, re.DOTALL | re.IGNORECASE)
    if mod_match:
        captured_content = mod_match.group(1)
        print(f"Regex captured content length: {len(captured_content)}")
        print(f"Full match length: {len(mod_match.group(0))}")
        
        # Check if it captures both charges
        statute_count = captured_content.count('53a-116') + captured_content.count('53a-125a')
        print(f"Statutes found in captured content: {statute_count}")
        
        # Save what the regex captures
        with open('regex_captured.html', 'w', encoding='utf-8') as f:
            f.write(mod_match.group(0))
        print("Regex captured content saved to 'regex_captured.html'")

if __name__ == "__main__":
    analyze_html_content('K10K-CR17-0338221-S.html')
