# record_parser.py
"""
Parser for Connecticut Judicial Branch court records

CRITICAL NOTE: The Connecticut Judicial website has malformed HTML, especially in the 
Modified Sentence Information section. DO NOT use BeautifulSoup for parsing modified 
charges - use regex directly on the raw HTML text instead. This is a recurring issue
that must be handled with regex parsing.
"""

from bs4 import BeautifulSoup
from log import log_issue
import re

class RecordParser:
    """Parser for both conviction and pending records"""
    
    def parse_conviction_sentences(self, soup):
        """Parse both Overall and Modified sentence sections"""
        sentences = {
            'overall': None,
            'modified': [],
            'charge_sentences': {}
        }
        
        # Parse Overall Sentence Information
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
        
        return sentences
    
    def parse_conviction_charges(self, soup):
        """
        Parse both regular charges and modified charges from conviction HTML.
        
        CRITICAL - MALFORMED HTML HANDLING:
        ===================================
        Parameter 'soup' is MISNAMED - it should be a RAW HTML STRING.
        
        The modified charges table contains malformed HTML that BeautifulSoup
        truncates, losing data. We MUST use regex parsing on raw HTML.
        
        MALFORMED HTML EXAMPLE:
        <td>
            <span id="..."></td></tr><tr class=grdRow><td colspan=11> </td></tr></span>
        </td>
        
        This has </td></tr> INSIDE a span tag, which is invalid. BeautifulSoup
        "fixes" this by truncating, causing the second modified charge to be lost.
        
        SOLUTION:
        1. Main charges table: Can use BeautifulSoup OR regex (both work)
        2. Modified charges: MUST use regex on raw HTML via _parse_modified_charges_regex()
        
        DATA FLOW:
        - Input: Raw HTML string (not BeautifulSoup object)
        - Main charges: Extracted via regex from raw HTML
        - Modified charges: Extracted via regex from raw HTML
        - Output: Merged charge list with modified data properly applied
        """
        charges = []
        
        # Get the raw HTML content
        if isinstance(soup, str):
            html_content = soup  # This should always be true if called correctly
            soup_obj = BeautifulSoup(soup, 'html.parser')
        else:
            # DANGER: If we get here, we've already lost data!
            html_content = str(soup)
            soup_obj = soup
            log_issue("ERROR: parse_conviction_charges received BeautifulSoup object - modified charges will be incomplete!")
        
        # IMPORTANT: BeautifulSoup truncates the table due to malformed HTML
        # We need to extract the table using regex instead
        
        # Find the charges table in the raw HTML
        table_pattern = r'<table[^>]*id="cphBody_Datagrid1"[^>]*>.*?(?=<table[^>]*id="cphBody_DatagridModCharge"|$)'
        table_match = re.search(table_pattern, html_content, re.DOTALL)
        
        if table_match:
            # Find the actual end of the table (there should be a </table> before the modified charges)
            table_html_full = table_match.group(0)
            # Find the last </table> in this section
            last_table_close = table_html_full.rfind('</table>')
            if last_table_close != -1:
                table_html_full = table_html_full[:last_table_close + 8]  # Include </table>
            
            print(f"DEBUG: Extracted full table HTML length: {len(table_html_full)}")
            
            # Parse the charges from the FULL table
            charges = self._parse_charges_from_malformed_table(table_html_full)
            print(f"DEBUG: Found {len(charges)} charges in main table")
        else:
            # Fallback to BeautifulSoup if regex fails
            print("DEBUG: Regex table extraction failed, falling back to BeautifulSoup")
            charges_table = soup_obj.find('table', id='cphBody_Datagrid1')
            if charges_table:
                charges = self._parse_charges_from_malformed_table(str(charges_table))
        
        # Parse modified charges using REGEX (NOT BeautifulSoup due to malformed HTML)
        # CRITICAL: This MUST use regex on raw HTML to avoid data loss
        modified_data = self._parse_modified_charges_regex(html_content)
        
        # Merge modified data into existing charges
        if modified_data:
            print(f"DEBUG: Found {len(modified_data)} entries in modified charges table")
            
            # Track matches to ensure we don't double-match
            matched_indices = set()
            
            # For each modified charge, update the corresponding original charge
            for mod_idx, mod in enumerate(modified_data):
                mod_statute = mod.get('statute', '').strip()
                mod_desc = mod.get('description', '').strip()
                
                print(f"\nDEBUG: Processing modified charge {mod_idx + 1}: {mod_statute} - {mod_desc}")
                
                # Find matching charge
                matched = False
                for charge_idx, charge in enumerate(charges):
                    # Skip if already matched (in case of duplicates)
                    if charge_idx in matched_indices:
                        continue
                        
                    charge_statute = charge.get('Statute', '').strip()
                    charge_desc = charge.get('Description', '').strip()
                    
                    # Debug comparison
                    print(f"  Comparing with charge {charge_idx + 1}: '{charge_statute}' - '{charge_desc}'")
                    
                    if charge_statute == mod_statute and charge_desc == mod_desc:
                        # Update the charge with modified data
                        charge['is_modified'] = True
                        charge['modified_sentence_finding'] = mod.get('verdict_finding', '')
                        charge['modified_sentence_date'] = mod.get('verdict_date', '')
                        charge['modified_fine'] = mod.get('fine', '')
                        charge['modified_fees'] = mod.get('fees', '')
                        
                        matched_indices.add(charge_idx)
                        print(f"  ✓ MATCHED! Updated charge {charge_idx + 1} with modified data:")
                        print(f"    - Modified finding: {charge['modified_sentence_finding']}")
                        print(f"    - Modified date: {charge['modified_sentence_date']}")
                        matched = True
                        break
                
                if not matched:
                    print(f"  ✗ WARNING: Could not find matching charge for modified: {mod_statute} - {mod_desc}")
        
        # Ensure all charges have required fields
        for i, charge in enumerate(charges):
            charge['charge_index'] = i
            charge.setdefault('Class', '')
            charge.setdefault('Type', '')
            charge.setdefault('Occ', '1')
            charge.setdefault('Offense Date', '')
            charge.setdefault('Plea', '')
            charge.setdefault('Verdict Finding', '')
            charge.setdefault('Verdict Date', '')
            charge.setdefault('Fine', '$0.00')
            charge.setdefault('Fee(s)', '$0.00')
            charge.setdefault('sentence_text', None)
            charge.setdefault('is_modified', False)
            charge.setdefault('modified_sentence_finding', '')
            charge.setdefault('modified_sentence_date', '')
            charge.setdefault('modified_fine', '')
            charge.setdefault('modified_fees', '')
        
        # Final debug output
        print(f"\nDEBUG: Final charge status:")
        for i, charge in enumerate(charges):
            print(f"  Charge {i + 1} ({charge.get('Statute', 'Unknown')}): is_modified={charge.get('is_modified', False)}, modified_finding='{charge.get('modified_sentence_finding', '')}'")
        
        return charges

    def _parse_charges_from_malformed_table(self, table_html):
        """Parse charges from the main charges table handling malformed HTML"""
        charges = []
        
        # Debug: Let's see what we're working with
        print(f"DEBUG: Table HTML length: {len(table_html)}")
        
        # The statutes have trailing spaces, so we need to account for that
        # Also, BeautifulSoup is truncating the table, so we need to use regex on the full HTML
        
        # First, let's get the FULL table content, not just what BeautifulSoup gives us
        # Find where the table really ends (before the modified charges table)
        table_start = table_html.find('<table')
        if table_start == -1:
            table_start = 0
            
        # The table might be truncated by BeautifulSoup, so let's be more aggressive
        # Look for statutes with spaces
        statute_pattern = r'<td[^>]*>\s*(\d+[a-z]?-\d+[a-z]?)\s*</td>'
        statute_matches = list(re.finditer(statute_pattern, table_html))
        
        print(f"DEBUG: Found {len(statute_matches)} statutes in provided table HTML")
        
        # If we didn't find all statutes, the table was truncated
        # Let's try a different approach - find statutes by their exact text with spaces
        if len(statute_matches) < 3:
            print("DEBUG: Table appears truncated, trying alternative approach")
            
            # Look for statute cells with the exact format including spaces
            statute_cell_pattern = r'<td[^>]*>(\d+[a-z]?-\d+[a-z]?\s*)</td>'
            statute_matches = list(re.finditer(statute_cell_pattern, table_html))
            print(f"DEBUG: Alternative search found {len(statute_matches)} statutes")
        
        # Process each statute found
        for i, statute_match in enumerate(statute_matches):
            statute = statute_match.group(1).strip()  # Remove trailing spaces
            start_pos = statute_match.start()
            
            # Find the <tr> tag that contains this statute
            tr_start = table_html.rfind('<tr', 0, start_pos)
            if tr_start == -1:
                continue
            
            # Find the end of this row
            # The HTML is malformed with </td></tr></span>
            # So we need to look for various possible endings
            tr_end = -1
            
            # Try different end patterns
            for end_pattern in [r'</tr></span>', r'</tr>', r'<tr\s+class=']:
                end_match = re.search(end_pattern, table_html[tr_start:])
                if end_match:
                    if end_pattern == r'<tr\s+class=':
                        tr_end = tr_start + end_match.start()
                    else:
                        tr_end = tr_start + end_match.end()
                    break
            
            if tr_end == -1:
                # If we can't find the end, use the next statute position
                if i + 1 < len(statute_matches):
                    next_tr_start = table_html.rfind('<tr', 0, statute_matches[i + 1].start())
                    tr_end = next_tr_start if next_tr_start != -1 else len(table_html)
                else:
                    tr_end = len(table_html)
            
            row_html = table_html[tr_start:tr_end]
            
            # Extract all cells from this row
            cell_pattern = r'<td[^>]*>(.*?)</td>'
            cell_matches = re.findall(cell_pattern, row_html, re.DOTALL)
            
            cells = []
            for cell_content in cell_matches:
                # Clean the cell content
                # Handle the malformed last cell that might contain </tr></span>
                if '</tr></span>' in cell_content:
                    # Extract content before the malformed part
                    cell_content = cell_content.split('</tr></span>')[0]
                
                # Handle spans
                if '<span' in cell_content:
                    span_match = re.search(r'<span[^>]*>([^<]*)</span>', cell_content)
                    if span_match:
                        cell_content = span_match.group(1)
                
                # Remove all HTML tags
                cell_content = re.sub(r'<[^>]+>', '', cell_content)
                cell_content = cell_content.replace('&nbsp;', ' ').strip()
                cells.append(cell_content)
                
                # We need at most 11 cells
                if len(cells) >= 11:
                    break
            
            # Ensure we have enough cells by padding with empty strings
            while len(cells) < 11:
                cells.append('')
            
            # Create charge record
            if cells[0]:  # If we have a statute
                charge = {
                    'Statute': cells[0].strip(),
                    'Description': cells[1].strip(),
                    'Class': cells[2].strip(),
                    'Type': cells[3].strip(),
                    'Occ': cells[4].strip() or '1',
                    'Offense Date': cells[5].strip(),
                    'Plea': cells[6].strip(),
                    'Verdict Finding': cells[7].strip(),
                    'Verdict Date': cells[8].strip(),
                    'Fine': cells[9].strip() or '$0.00',
                    'Fee(s)': cells[10].strip() or '$0.00'
                }
                
                # Look for sentence text
                sentence_match = re.search(r'<B>Sentenced:\s*</B>([^<]+)', row_html)
                if sentence_match:
                    charge['sentence_text'] = sentence_match.group(1).strip()
                
                charges.append(charge)
                print(f"DEBUG: Parsed charge {len(charges)}: {charge['Statute']} - {charge['Description']}")
        
        # If we still only found 1 charge, the table was definitely truncated
        # We need to go back to the original HTML
        if len(charges) <= 1:
            print("DEBUG: Only found 1 charge, table was truncated by BeautifulSoup")
            print("DEBUG: This is a known issue with malformed HTML - the table contains </span> tags that confuse BeautifulSoup")
        
        return charges
    
    def _parse_modified_charges_regex(self, html_content):
        """
        Parse modified charges using REGEX due to malformed HTML.
        
        CRITICAL - DO NOT USE BEAUTIFULSOUP HERE:
        =========================================
        The Connecticut Judicial website's modified charges table contains
        malformed HTML that BeautifulSoup cannot handle correctly.
        
        MALFORMED PATTERN:
        <td><span></td></tr><tr class=grdRow><td colspan=11> </td></tr></span></td>
        
        BeautifulSoup sees the </td></tr> inside the span and truncates the table,
        losing subsequent charges. This is why we MUST use regex.
        
        KNOWN ISSUE EXAMPLE:
        Docket K10K-CR17-0338221-S has two charges that should both show
        "Probation Terminated" in the modified charges table:
        - 53a-116 Criminal Mischief 2nd Deg
        - 53a-125a Larceny 5Th Deg
        
        If BeautifulSoup processes this HTML:
        - Raw table: 2507 chars, 2 charges → BeautifulSoup: 1387 chars, 1 charge
        - The larceny charge loses its "Probation Terminated" status
        
        REGEX APPROACH:
        1. Find the modified charges table by searching for the table ID
        2. Extract the complete table HTML including all malformed content
        3. Parse rows using patterns that handle the malformed structure
        4. Extract data from cells while handling the nested tag issues
        
        DO NOT:
        - Use BeautifulSoup to parse this table
        - Try to "clean" the HTML before parsing
        - Assume the HTML structure is valid
        
        TEST: After parsing K10K-CR17-0338221-S, this method should return
        2 modified charges, both with verdict_finding = "Probation Terminated"
        """
        modified_charges = []
        
        # Find the Modified Sentence Information table in the raw HTML
        # FIXED: The previous pattern was too restrictive and may have been cutting off early
        # Look for the table ID and capture everything until the closing </table> tag
        
        # First, find where the modified charges section starts
        mod_section_start = html_content.find('Modified Sentence Information')
        if mod_section_start == -1:
            print("DEBUG: No modified charges section found")
            return modified_charges
        
        # Find the table start after the section header
        table_start = html_content.find('<table', mod_section_start)
        if table_start == -1:
            print("DEBUG: No table found after Modified Sentence Information")
            return modified_charges
        
        # Find the matching </table> tag
        # We need to count nested tables if any
        table_depth = 1
        pos = html_content.find('>', table_start) + 1  # Start after the opening tag
        table_end = -1
        
        while pos < len(html_content) and table_depth > 0:
            # Look for next table tag
            next_open = html_content.find('<table', pos)
            next_close = html_content.find('</table>', pos)
            
            if next_close == -1:
                break
                
            if next_open != -1 and next_open < next_close:
                # Found opening tag first
                table_depth += 1
                pos = next_open + 1
            else:
                # Found closing tag first
                table_depth -= 1
                if table_depth == 0:
                    table_end = next_close + 8  # Include </table>
                pos = next_close + 1
        
        if table_end == -1:
            print("DEBUG: Could not find end of modified charges table")
            return modified_charges
        
        mod_table_html = html_content[table_start:table_end]
        print(f"DEBUG: Modified table HTML length: {len(mod_table_html)}")
        
        # Now parse the table with the improved row detection
        # Find all data rows (skip header)
        row_pattern = r'<tr\s+class=["\']?(grdRow|grdRowAlt)["\']?[^>]*>(.*?)(?=<tr\s+class=["\']?(?:grdRow|grdRowAlt)["\']?|</table>)'
        row_matches = list(re.finditer(row_pattern, mod_table_html, re.DOTALL))
        
        print(f"DEBUG: Found {len(row_matches)} data rows in modified charges table")
        
        for row_num, row_match in enumerate(row_matches):
            row_html = row_match.group(0)
            print(f"\nDEBUG: Processing row {row_num + 1}")
            
            # Extract all cell contents more carefully
            cells = []
            
            # Split by <td tags to get cell boundaries
            td_splits = re.split(r'<td[^>]*>', row_html)
            
            # First split is before first <td>, so skip it
            for i, cell_content in enumerate(td_splits[1:]):
                # Find where this cell ends
                if '</td>' in cell_content:
                    cell_content = cell_content.split('</td>')[0]
                
                # Handle spans
                text = cell_content
                if '<span' in text:
                    span_match = re.search(r'<span[^>]*>([^<]*)</span>', text)
                    if span_match:
                        text = span_match.group(1)
                
                # Remove any remaining HTML tags
                text = re.sub(r'<[^>]+>', '', text)
                text = text.replace('&nbsp;', ' ').strip()
                
                cells.append(text)
                
                # Stop after we have enough cells (11 columns expected)
                if len(cells) >= 11:
                    break
            
            print(f"  Extracted {len(cells)} cells")
            
            # Ensure we have enough cells
            while len(cells) < 11:
                cells.append('')
            
            # Debug first few cells
            if len(cells) >= 2:
                print(f"  Statute: '{cells[0]}'")
                print(f"  Description: '{cells[1]}'")
                if len(cells) >= 9:
                    print(f"  Verdict Finding: '{cells[7]}'")
                    print(f"  Verdict Date: '{cells[8]}'")
            
            # Create modified charge record if we have valid data
            if cells[0] and cells[0].strip() and cells[1] and cells[1].strip():
                # This is a real charge row (has both statute and description)
                modified_charge = {
                    'statute': cells[0].strip(),
                    'description': cells[1].strip(),
                    'verdict_finding': cells[7].strip() if len(cells) > 7 else '',
                    'verdict_date': cells[8].strip() if len(cells) > 8 else '',
                    'fine': cells[9].strip() if len(cells) > 9 and cells[9].strip() else '$0.00',
                    'fees': cells[10].strip() if len(cells) > 10 and cells[10].strip() else '$0.00'
                }
                
                modified_charges.append(modified_charge)
                print(f"  ✓ Created modified charge: {modified_charge['statute']} - {modified_charge['description']} - Finding: {modified_charge['verdict_finding']}")
            else:
                print(f"  Skipping row - appears to be empty or sentence text row")
        
        print(f"\nDEBUG: Total modified charges extracted: {len(modified_charges)}")
        return modified_charges

    def parse_conviction_record(self, soup):
        """
        Parse conviction record HTML to extract case details and charges.
        
        CRITICAL - MALFORMED HTML HANDLING:
        ===================================
        The input parameter 'soup' is INTENTIONALLY MISNAMED for backward compatibility.
        It should actually be a RAW HTML STRING, not a BeautifulSoup object.
        
        WHY: The Connecticut Judicial website has malformed HTML in the modified
        charges table with invalid nesting like <span></td></tr></span>. BeautifulSoup
        "fixes" this by truncating the table, causing data loss.
        
        FLOW:
        1. This method receives RAW HTML STRING from docket.py
        2. We create a BeautifulSoup object for parsing normal fields
        3. We pass the RAW HTML to parse_conviction_charges()
        4. parse_conviction_charges() uses REGEX (not BeautifulSoup) for modified charges
        
        DO NOT:
        - Change the parameter name (would break compatibility)
        - Pass a BeautifulSoup object from docket.py
        - Use BeautifulSoup to parse the modified charges table
        
        TEST: Check docket K10K-CR17-0338221-S - both charges should have
        modified_sentence_finding = "Probation Terminated"
        """
        # CRITICAL: Store raw HTML before any BeautifulSoup processing
        if isinstance(soup, str):
            html_content = soup  # This is raw HTML - good!
            soup = BeautifulSoup(soup, 'html.parser')
        else:
            # This should not happen with our fix, but handle it just in case
            html_content = str(soup)
            log_issue("WARNING: parse_conviction_record received BeautifulSoup object instead of raw HTML - modified charges may be lost!")
        
        result = {
            'docket_number': None,
            'defendant_name': None,
            'defendant_attorney': None,
            'birth_year': None,
            'case_status': None,
            'file_date': None,
            'offense_date': None,
            'arrest_date': None,
            'arresting_agency': None,
            'plea_date': None,
            'disposition_date': None,
            'court': None,
            'overall_sentence': None,
            'total_cost': None,
            'amount_paid': None,
            'payment_status': None,
            'charges': []
        }
        
        # Parse all the case details
        all_spans = soup.find_all('span', id=True)
        
        for span in all_spans:
            span_id = span.get('id', '')
            span_text = span.text.strip()
            
            # More specific ID matching
            if span_id.endswith('lblDocketNo') and 'Label' not in span_id:
                result['docket_number'] = span_text
            elif span_id.endswith('lblDefendant') and 'Attorney' not in span_id and 'BirthDate' not in span_id:
                result['defendant_name'] = span_text
            elif span_id.endswith('lblDefendantAttorney'):
                result['defendant_attorney'] = span_text
            elif span_id.endswith('lblDefendantBirthDate'):
                result['birth_year'] = span_text
            elif span_id.endswith('lblCaseStatus'):
                result['case_status'] = span_text
            elif span_id.endswith('lblFileDate'):
                result['file_date'] = span_text
            elif span_id.endswith('lblArrestDate'):
                result['arrest_date'] = span_text
            elif span_id.endswith('lblArrestingAgency'):
                result['arresting_agency'] = span_text
            elif span_id.endswith('lblPleaDate'):
                result['plea_date'] = span_text
            elif span_id.endswith('lblSentDate'):
                result['disposition_date'] = span_text
            elif span_id.endswith('lblCourt'):
                result['court'] = span_text
            elif span_id.endswith('lblCost'):
                result['total_cost'] = span_text
            elif span_id.endswith('Label4'):  # This is the paid amount
                result['amount_paid'] = span_text
        
        # Debug print to see what we found
        print(f"DEBUG: Parsed defendant info:")
        print(f"  Name: {result['defendant_name']}")
        print(f"  Attorney: {result['defendant_attorney']}")
        print(f"  Birth Year: {result['birth_year']}")
        
        # Calculate payment status
        if result.get('total_cost') and result.get('amount_paid'):
            try:
                cost_str = result['total_cost'].replace('$', '').replace(',', '').strip()
                paid_str = result['amount_paid'].replace('$', '').replace(',', '').strip()
                
                # Handle empty strings
                if cost_str and paid_str:
                    cost = float(cost_str)
                    paid = float(paid_str)
                    
                    if cost == 0:
                        result['payment_status'] = 'NO COST'
                    elif paid >= cost:
                        result['payment_status'] = 'PAID IN FULL'
                    elif paid > 0:
                        result['payment_status'] = 'PARTIALLY PAID'
                    else:
                        result['payment_status'] = 'UNPAID'
                else:
                    result['payment_status'] = 'UNKNOWN'
            except (ValueError, AttributeError):
                result['payment_status'] = 'UNKNOWN'
        
        # Parse overall sentence
        overall_sentence = self.parse_conviction_sentences(soup)
        if overall_sentence and overall_sentence.get('overall'):
            result['overall_sentence'] = overall_sentence['overall']
        
        # CRITICAL: Parse charges using the RAW HTML, not the soup object
        # This ensures modified charges aren't lost due to BeautifulSoup's HTML "fixing"
        result['charges'] = self.parse_conviction_charges(html_content)  # Pass raw HTML
        
        # Standardize the charge keys to lowercase
        standardized_charges = []
        for charge in result['charges']:
            standardized_charge = {
                'statute': charge.get('Statute', ''),
                'description': charge.get('Description', ''),
                'class': charge.get('Class', ''),
                'type': charge.get('Type', ''),
                'offense_date': charge.get('Offense Date', ''),
                'verdict_date': charge.get('Verdict Date', ''),
                'occ': charge.get('Occ', '1'),
                'plea': charge.get('Plea', ''),
                'verdict_finding': charge.get('Verdict Finding', ''),
                'fine': charge.get('Fine', '$0.00'),
                'fees': charge.get('Fee(s)', '$0.00'),
                'charge_index': charge.get('charge_index', 0),
                'is_modified': charge.get('is_modified', False),
                'modified_sentence_finding': charge.get('modified_sentence_finding', ''),
                'modified_sentence_date': charge.get('modified_sentence_date', ''),
                'modified_fine': charge.get('modified_fine', ''),
                'modified_fees': charge.get('modified_fees', ''),
                'sentence_text': charge.get('sentence_text', '')
            }
            standardized_charges.append(standardized_charge)
        
        result['charges'] = standardized_charges
        
        # If offense_date not found above, try to get it from first charge
        if not result['offense_date'] and standardized_charges:
            result['offense_date'] = standardized_charges[0].get('offense_date')
        
        return result

    def parse_pending_record(self, soup):
        """Parse a pending case record"""
        case_details = {}
        span_tags = soup.find_all('span', id=True)
        
        pending_spans = [
            'cphBody_lblDocketNo', 'cphBody_lblDefendant', 
            'cphBody_lblDefendantAttorney', 'cphBody_lblDefendantBirthDate',
            'cphBody_lblTimesInCourt', 'cphBody_lblArrestingAgency', 
            'cphBody_lblArrestDate', 'cphBody_lblCompanionDocketNo',
            'cphBody_lblDocketType', 'cphBody_lblCourt', 
            'cphBody_lblBondAmount', 'cphBody_lblBondTypeDesc',
            'cphBody_lblBondTypeDescHelp', 'cphBody_lblSidebarFlag', 
            'cphBody_lblPurposeDesc', 'cphBody_lblHearingDate', 
            'cphBody_lblReasonDesc'
        ]
        
        for span in span_tags:
            if span['id'] in pending_spans:
                case_details[span['id']] = span.get_text(strip=True)
        
        charges = self.parse_pending_charges(soup)
        
        return {
            'case_details': case_details,
            'charges': charges
        }
    
    def parse_pending_charges(self, soup):
        """Parse charges for pending cases"""
        charges = []
        
        charges_table = soup.find('table', class_='grdBorder', id=lambda x: x and ('Datagrid1' in x or 'grdCharges' in x))
        if not charges_table:
            return charges
        
        header_row = charges_table.find('tr', class_='grdHeader')
        if not header_row:
            return charges
        
        headers = []
        header_cells = header_row.find_all(['td', 'th'])
        for cell in header_cells:
            text = cell.get_text(strip=True)
            if text:
                headers.append(text)
        
        charge_rows = charges_table.find_all('tr', class_=['grdRow', 'grdRowAlt'])
        
        for i, row in enumerate(charge_rows):
            cells = row.find_all('td')
            cell_values = [cell.get_text(strip=True) for cell in cells]
            
            if len(cell_values) >= 2 and cell_values[0]:
                while cell_values and cell_values[-1] == '':
                    cell_values.pop()
                
                charge = dict(zip(headers, cell_values))
                charge['charge_index'] = i
                
                charge.setdefault('Statute', '')
                charge.setdefault('Description', '')
                charge.setdefault('Class', '')
                charge.setdefault('Type', '')
                charge.setdefault('Occ', '1')
                charge.setdefault('Offense Date', '')
                charge.setdefault('Plea', '')
                charge.setdefault('Verdict Finding', '')
                
                charges.append(charge)
        
        return charges
