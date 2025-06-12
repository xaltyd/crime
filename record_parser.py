# record_parser.py - Fixed version
"""
Enhanced parser for conviction records that handles malformed HTML tables
"""

from bs4 import BeautifulSoup
from log import log_issue
import re

class RecordParser:
    """Parser specifically for conviction records with better sentence handling"""
    
    def parse_conviction_sentences_raw(self, soup):
        """
        Parse both Overall and Modified sentence sections
        Modified sentences use regex on raw HTML due to BeautifulSoup parsing issues
        """
        sentences = {
            'overall': None,
            'modified': [],
            'charge_sentences': {}
        }

        raw_html = soup._raw_html
        
        try:
            # Parse Overall Sentence Information (still use BeautifulSoup - this works fine)
            overall_table = soup.find('table', {'id': 'cphBody_Datagrid2', 'class': 'grdBorder'})
            if overall_table:
                overall_text = []
                rows = overall_table.find_all('tr', class_=['grdRow', 'grdRowAlt'])
                
                for row in rows:
                    cells = row.find_all('td')
                    for cell in cells:
                        text = cell.get_text(strip=True)
                        if text:
                            overall_text.append(text)
                
                if overall_text:
                    sentences['overall'] = ' '.join(overall_text)
            
            # Get docket number for debugging
            docket_no = "Unknown"
            docket_span = soup.find('span', {'id': 'cphBody_lblDocketNo'})
            if docket_span:
                docket_no = docket_span.get_text(strip=True)
            
            # SPECIAL HANDLING FOR K10K DOCKET
            if docket_no == "K10K-CR17-0338221-S":
                print("\n" + "="*60)
                print(f"SPECIAL DEBUG: Processing {docket_no}")
                print("="*60)
                print("Using original raw HTML for modified sentences")
            
            # Find the modified charges table in raw HTML using regex
            mod_table_match = re.search(
                r'<table[^>]+id="cphBody_DatagridModCharge"[^>]*>(.*?)</table>',
                raw_html,
                re.DOTALL | re.IGNORECASE
            )
            
            if mod_table_match:
                mod_table_html = mod_table_match.group(0)
                
                if docket_no == "K10K-CR17-0338221-S":
                    print(f"\nTable contains '53a-116': {'53a-116' in mod_table_html}")
                    print(f"Table contains '53a-125': {'53a-125' in mod_table_html}")
                    print(f"Modified table length: {len(mod_table_html)}")
                
                # Extract ALL rows with grdRow or grdRowAlt classes
                row_pattern = r'<tr[^>]*class="grd(?:Row|RowAlt)"[^>]*>(.*?)</tr>'
                all_rows = re.findall(row_pattern, mod_table_html, re.DOTALL | re.IGNORECASE)
                
                if docket_no == "K10K-CR17-0338221-S":
                    print(f"\nFound {len(all_rows)} rows with grdRow/grdRowAlt")
                
                # Process each row
                charge_count = 0
                for i, row_content in enumerate(all_rows):
                    # Skip spacer rows (those with colspan="11")
                    if 'colspan="11"' in row_content:
                        if docket_no == "K10K-CR17-0338221-S":
                            print(f"Row {i}: Skipping spacer row")
                        continue
                    
                    # Extract all cells from this row
                    cell_pattern = r'<td[^>]*>(.*?)</td>'
                    cells = re.findall(cell_pattern, row_content, re.DOTALL)
                    
                    if not cells or len(cells) < 10:
                        continue
                    
                    # Clean cell values - remove HTML tags
                    cell_values = []
                    for cell in cells:
                        # Remove span tags and other HTML
                        text = re.sub(r'<[^>]+>', '', cell)
                        text = text.strip()
                        cell_values.append(text)
                    
                    # Check if first cell is a statute
                    statute = cell_values[0] if cell_values else ''
                    
                    # Verify this looks like a statute (e.g., 53a-116, 53a-125a)
                    if statute and '-' in statute and (statute.startswith('53') or statute[0].isdigit()):
                        charge_count += 1
                        
                        if docket_no == "K10K-CR17-0338221-S":
                            print(f"\nFound charge row {charge_count}:")
                            print(f"  Statute: {statute}")
                            print(f"  Description: {cell_values[1] if len(cell_values) > 1 else 'N/A'}")
                            print(f"  Verdict: {cell_values[7] if len(cell_values) > 7 else 'N/A'}")
                        
                        # Create modified sentence entry with all fields
                        mod_sentence = {
                            'date': None,
                            'details': cell_values,
                            'raw_text': '\t'.join(cell_values),
                            'statute': statute.strip(),
                            'description': cell_values[1] if len(cell_values) > 1 else '',
                            'class': cell_values[2] if len(cell_values) > 2 else '',
                            'type': cell_values[3] if len(cell_values) > 3 else '',
                            'occurrence': cell_values[4] if len(cell_values) > 4 else '',
                            'offense_date': cell_values[5] if len(cell_values) > 5 else '',
                            'plea': cell_values[6] if len(cell_values) > 6 else '',
                            'verdict_finding': cell_values[7] if len(cell_values) > 7 else '',
                            'verdict_date': cell_values[8] if len(cell_values) > 8 else '',
                            'fine': cell_values[9] if len(cell_values) > 9 else '',
                            'fees': cell_values[10] if len(cell_values) > 10 else '',
                            'raw_data': cell_values
                        }
                        
                        # Extract date from verdict_date column
                        if len(cell_values) > 8 and '/' in cell_values[8]:
                            mod_sentence['date'] = cell_values[8]
                        
                        sentences['modified'].append(mod_sentence)
            
            if docket_no == "K10K-CR17-0338221-S":
                print(f"\n{'='*60}")
                print(f"TOTAL MODIFIED CHARGES FOUND: {len(sentences['modified'])}")
                for idx, charge in enumerate(sentences['modified']):
                    print(f"  {idx+1}. {charge['statute']} - {charge['description']} - {charge['verdict_finding']}")
                print(f"{'='*60}")
                
                # PAUSE FOR DEBUGGING
                if len(sentences['modified']) != 2:
                    print("\nERROR: Should have found 2 modified charges but found", len(sentences['modified']))
                input("\nPress Enter to continue...")
            
        except Exception as e:
            print(f"ERROR in parse_conviction_sentences_raw: {e}")
            import traceback
            traceback.print_exc()
            if docket_no == "K10K-CR17-0338221-S":
                input("Error occurred. Press Enter to continue...")
        
        return sentences

    def parse_conviction_charges(self, soup):
        """
        Parse charges from potentially malformed HTML tables
        This version looks for charge rows throughout the page, not just in specific tables
        """
        charges = []
        charge_headers = ['Statute', 'Description', 'Class', 'Type', 'Occ', 'Offense Date', 'Plea', 'Verdict Finding', 'Verdict Date', 'Fine', 'Fee(s)']
        
        # First try the standard approach - look for the main charges table
        charges_table = soup.find('table', class_='grdBorder', id=['cphBody_Datagrid1', 'cphBody_grdCharges'])
        if charges_table:
            # Get rows from the identified table
            rows = charges_table.find_all('tr', class_=['grdRow', 'grdRowAlt'])
            i = 0
            while i < len(rows):
                row = rows[i]
                cells = row.find_all('td')
                if cells and len(cells) >= len(charge_headers):
                    cell_values = [cell.get_text(strip=True) for cell in cells]
                    # Check if this is a charge row (not a sentence row)
                    if not cell_values[0].startswith('Sentenced:'):
                        charge = dict(zip(charge_headers, cell_values[:len(charge_headers)]))
                        charge['charge_index'] = len(charges)
                        charge['sentence_text'] = None
                        charge['charge_specific_sentence'] = None
                        
                        # Look for sentence in next row
                        if i + 1 < len(rows):
                            next_row = rows[i + 1]
                            next_cells = next_row.find_all('td')
                            if next_cells and len(next_cells) == 1:  # Sentence rows typically have 1 cell with colspan
                                sentence_text = next_cells[0].get_text(strip=True)
                                if sentence_text.startswith('Sentenced:'):
                                    charge['charge_specific_sentence'] = sentence_text.replace('Sentenced:', '').strip()
                                    i += 1  # Skip the sentence row
                        
                        charges.append(charge)
                i += 1
        
        # Now look for additional charges that might be outside the main table due to malformed HTML
        # Search all rows in the page that look like charge rows
        all_rows = soup.find_all('tr')
        
        i = 0
        while i < len(all_rows):
            row = all_rows[i]
            cells = row.find_all('td')
            if cells and len(cells) >= 11:  # Charge rows typically have 11-12 cells
                cell_values = [cell.get_text(strip=True) for cell in cells]
                
                # Check if this looks like a charge row:
                # - First cell contains a statute pattern (e.g., "53a-125a")
                # - Has the right number of cells
                # - Not already captured
                if len(cell_values) >= len(charge_headers) and re.match(r'^\d+[a-z]?-\d+[a-z]?', cell_values[0]):
                    # Check if we already have this charge
                    statute = cell_values[0]
                    already_captured = any(c.get('Statute') == statute for c in charges)
                    
                    if not already_captured and not cell_values[0].startswith('Sentenced:'):
                        charge = dict(zip(charge_headers, cell_values[:len(charge_headers)]))
                        charge['charge_index'] = len(charges)
                        charge['sentence_text'] = None
                        charge['charge_specific_sentence'] = None
                        
                        # Look for sentence in next row
                        if i + 1 < len(all_rows):
                            next_row = all_rows[i + 1]
                            next_cells = next_row.find_all('td')
                            if next_cells and len(next_cells) == 1:  # Sentence rows typically have 1 cell with colspan
                                sentence_text = next_cells[0].get_text(strip=True)
                                if sentence_text.startswith('Sentenced:'):
                                    charge['charge_specific_sentence'] = sentence_text.replace('Sentenced:', '').strip()
                                    i += 1  # Skip the sentence row
                        
                        charges.append(charge)
            i += 1
        
        # Also check the modified charges table and parse it properly
        mod_table = soup.find('table', {'id': 'cphBody_DatagridModCharge', 'class': 'grdBorder'})
        if mod_table:
            rows = mod_table.find_all('tr', class_=['grdRow', 'grdRowAlt'])
            for row in rows:
                cells = row.find_all('td')
                if cells and len(cells) >= len(charge_headers):
                    cell_values = [cell.get_text(strip=True) for cell in cells]
                    if not cell_values[0].startswith('Sentenced:') and re.match(r'^\d+[a-z]?-\d+[a-z]?', cell_values[0]):
                        # Find the matching charge and update it with modified information
                        statute = cell_values[0]
                        for charge in charges:
                            if charge.get('Statute') == statute:
                                # Store modified sentence information
                                charge['modified_sentence'] = {
                                    'verdict_finding': cell_values[7] if len(cell_values) > 7 else None,
                                    'verdict_date': cell_values[8] if len(cell_values) > 8 else None,
                                    'plea': cell_values[6] if len(cell_values) > 6 else None,
                                    'raw_data': cell_values  # Keep raw data for reference
                                }
                                break
        
        return charges
    
    def parse_conviction_record(self, soup):
        """
        Complete parsing of a conviction record with improved structure
        """
        # Parse case details using BeautifulSoup
        case_details = {}
        span_tags = soup.find_all('span', id=True)
        
        conviction_spans = [
            'cphBody_lblDocketNo', 'cphBody_lblDefendant', 
            'cphBody_lblDefendantAttorney', 'cphBody_lblDefendantBirthDate',
            'cphBody_lblArrestingAgency', 'cphBody_lblArrestDate', 
            'cphBody_lblSentDate', 'cphBody_lblCourt', 
            'cphBody_lblCost', 'cphBody_Label4'
        ]
        
        for span in span_tags:
            if span['id'] in conviction_spans:
                case_details[span['id']] = span.get_text(strip=True)
        
        # Parse sentences - check if we have raw HTML
        if hasattr(soup, '_raw_html'):
            sentences = self.parse_conviction_sentences_raw(soup)
        else:
            # Fallback if no raw HTML stored
            print("WARNING: No raw HTML found in soup object")
            sentences = {
                'overall': None,
                'modified': [],
                'charge_sentences': {}
            }
        
        # Parse charges with sentence tracking using soup
        charges = self.parse_conviction_charges(soup)
        
        return {
            'case_details': case_details,
            'sentences': sentences,
            'charges': charges
        }
