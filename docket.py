from bs4 import BeautifulSoup
from color_print import *
from datetime import datetime
from storage import Judicial  # Changed from importing ConvictionStorage
from log import log_issue, log_action
import requests, sys, os, json, io
import socket, ssl, urllib3, time
from record_parser import RecordParser
from checkpoint import CheckpointManager

server_host = 'www.jud2.ct.gov'
server_url  = f'https://{server_host}'

# ========== CONFIGURATION VARIABLES ==========
DEBUGGING = False           # Set to True to enable debug mode
DEBUG_LIMIT = 30           # Number of records to process in debug mode
LIMIT_RECORDS = True      # Set to True to limit records even when not debugging
RECORD_LIMIT = 50         # Number of records to process when LIMIT_RECORDS is True
# ============================================

def ensure_connection(f, url, header, payload = None):
    delay = 60
    while True:
        try:
            if payload:
                resp = f(url, headers = header, data = payload)
            else:
                resp = f(url, headers = header)
            return resp
        except Exception as e:
            cprint(e, Fore.RED)
            cprint(f'Waiting {delay} seconds before re-attempt...', Fore.YELLOW)
            
            time.sleep(delay)

class UpdateCert:
    def __init__(self):
        self.host = server_host
        self.port = 443

    def get_certs_pem(self):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.set_ciphers("AES256-SHA256")
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        certs_pem = io.StringIO()
        with socket.create_connection((self.host, self.port)) as sock:
            with context.wrap_socket(sock, server_hostname = self.host) as ssock:
                cert_chain = ssock.get_verified_chain()
                
                for cert in cert_chain:
                    certs_pem.write(ssl.DER_cert_to_PEM_cert(cert))

        return certs_pem.read()

class Page:
    def __init__(self):
        crdockets_url = f'{server_url}/crdockets'

        self.url = {'docket':     f'{crdockets_url}/SearchByCourt.aspx',
                    'pending':    f'{crdockets_url}/parm1.aspx',
                    'conviction': f'{crdockets_url}/SearchByDefDisp.aspx'}

        self.next_url = {'pending':    f'{crdockets_url}/SearchResultsPending.aspx',
                         'conviction': f'{crdockets_url}/SearchResultsDisp.aspx'}
        
        shared_header = {'accept':                    'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                         'accept-encoding':           'gzip, deflate, br, zstd',
                         'accept-language':           'en-US,en;q=0.9',
                         'cache-control':             'no-cache',
                         'connection':                'keep-alive',
                         'host':                      server_host,
                         'pragma':                    'no-cache',
                         'sec-ch-ua':                 '"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                         'sec-ch-ua-mobile':          '?0',
                         'sec-ch-ua-platform':        '"Windows"',
                         'sec-fetch-dest':            'document',
                         'sec-fetch-mode':            'navigate',
                         'sec-fetch-user':            '?1',
                         'upgrade-insecure-requests': '1',
                         'user-agent':                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0'}

        self.results_header = shared_header | {'content-type':   'application/x-www-form-urlencoded',
                                               'origin':         server_url,
                                               'sec-fetch-site': 'same-origin'}
        self.search_header  = shared_header | {'sec-fetch-site': 'none'}
        
        self.record_header = shared_header | {'sec-fetch-site': 'same-origin'}

        self.payload = {'docket': {'_ctl0:cphBody:ddlCourts': None},
                        'search': {'_ctl0:cphBody:txtDefendantFullName': '_',
                                    '_ctl0:cphBody:txtFirstNameInitial':  '',
                                    '_ctl0:cphBody:txtBirthYear':         '',
                                    '_ctl0:cphBody:txtBirthYearRange':    '',
                                    '_ctl0:cphBody:ddlCourts':            '',
                                    '_ctl0:cphBody:ddlCaseType':          ''}}

    def load_init_page(self, session, page_name):
        try:
            resp = session.get(self.url[page_name], headers = self.search_header)
        except Exception as e:
            cinput(e, Fore.RED)
            sys.exit()

        return resp.text
                       
    def get_payload(self, page, text):
        fields = ['__EVENTTARGET', '__EVENTARGUMENT', '__VIEWSTATE',
                  '__VIEWSTATEGENERATOR', '__EVENTVALIDATION']

        payload = {'_ctl0:cphBody:btnSearch': 'Search'}
        payload |= self.payload[page]
        payload |= {field:'' for field in fields}

        for field in fields:
            text = text[text.find(field):]
            text = text[text.find('value="') + len('value="'):]

            payload[field] = text[:text.find('"')]

        return payload

class Search:
    def __init__(self, session, record, page):
        self.session = session
        self.record  = record
        self.page    = page
        self.page_count = 0  # Initialize page_count

    def get_records(self, payload, search_type):
        """Modified to handle new conviction data structure"""
        page = 1
        results = []
        cph_body_id = '_ctl0$cphBody$grdDockets$_ctl54$'
        self.sealed_count = 0
        
        # Add limit for records
        LIMIT_RECORDS = True  # From your configuration
        MAX_RECORDS = 200
        
        header = self.page.results_header | {'referer': self.page.url[search_type]}
        
        resp = ensure_connection(self.session.post, self.page.url[search_type],
                                 header, payload)
        
        count = 0
        while f'<span>{page}</span>' in resp.text:
            # Check limit outside of DEBUGGING block
            if LIMIT_RECORDS and count >= MAX_RECORDS:
                self.page_count = page
                return results
                
            soup = BeautifulSoup(resp.text, 'html.parser')
            grd_header = soup.find('tr', class_=['grdHeader'])
            header_vals = [a.get_text(strip=True) for a in grd_header.find_all('a')]
            
            rows = soup.find_all('tr', class_=['grdRow', 'grdRowAlt'])
            for row in rows:
                # Check limit here too
                if LIMIT_RECORDS and count >= MAX_RECORDS:
                    break
                    
                a_tag = row.find('a', id=True)
                if a_tag:
                    a_href = a_tag.get('href')
                    a_title = a_tag.get('title')
                
                if 'Sealed' not in a_title:
                    # Get the record data
                    record_data = self.record.get(a_href)
                    
                    # For convictions, ensure we have the new structure
                    if search_type == 'conviction' and isinstance(record_data, dict):
                        if 'case_details' in record_data:
                            # New structure
                            results.append(record_data)
                        else:
                            # Old structure - convert
                            results.append({
                                'case_details': record_data[0] if isinstance(record_data, tuple) else record_data,
                                'sentences': record_data[1] if isinstance(record_data, tuple) and len(record_data) > 1 else {},
                                'charges': []
                            })
                    else:
                        results.append(record_data)
                    
                    count += 1
                    print(f'Added data for href {a_href} ({count}/{MAX_RECORDS})')
                else:
                    self.sealed_count += 1

            # Break outer loop if limit reached
            if LIMIT_RECORDS and count >= MAX_RECORDS:
                break

            payload = self.page.get_payload('search', resp.text)

            span_tag = f'<span>{page}</span>'
            
            text = resp.text
            text = text[text.find(span_tag) + len(span_tag):]
            text = text[text.find(cph_body_id) + len(cph_body_id):]
            ctl_id    = text[:text.find('&')]
            next_page = f'{cph_body_id}{ctl_id}'

            resp = ensure_connection(self.session.post, self.page.next_url[search_type],
                                     header | {'referer': self.page.next_url[search_type]},
                                     payload | {'__EVENTTARGET': next_page})
            self.last_resp = resp

            page += 1
            self.page_count = page

        return results

class Docket:
    def __init__(self, session, page):
        self.session = session
        self.page = page

    def get_court_codes(self, text):
        courts = {}

        while 'option value="' in text:
            text = text[text.find('option value="') + len('option value="'):]
            code = text[:text.find('"')]

            if code:
                name = text[text.find('>') + 1:text.find('<')]

                courts[code] = name

        return courts

    def get_daily(self, payload, court_code):
        def get_record_count(text):
            text = text[text.find('cphBody_lblRecordCount') + len('cphBody_lblRecordCount'):]
            text = text[text.find('>') + 1:]

            return text[:text.find('<')]

        def get_docket_list(text, count, court_code):
            docket = []

            soup = BeautifulSoup(text, 'html.parser')

            header = soup.find('tr', class_=['grdHeader'])

            header_vals = [a.get_text(strip = True) for a in header.find_all('a')]

            rows = soup.find_all('tr', class_=['grdRow', 'grdRowAlt'])

            for row in rows:
                a_tag = row.find('a', id = True)
                if a_tag:
                    a_id = a_tag['id']
                    a_href = a_tag.get('href')
                    a_title = a_tag.get('title')

                td_vals = [td.get_text(strip = True) for td in row.find_all('td')]

                record = dict(zip(header_vals, td_vals))
                record['Page']   = a_href
                record['Sealed'] = 'Sealed' in a_title
                
                docket.append(record)
            return docket

        payload['_ctl0:cphBody:ddlCourts'] = court_code

        resp = ensure_connection(self.session.post, self.page.url['docket'],
                                 self.page.results_header, payload)
        text = resp.text

        record_count = int(get_record_count(text))
        
        return get_docket_list(text, record_count, court_code)

class Record:
    def __init__(self, session, page):
        self.session = session
        self.page = page
        self.url = f'{server_url}/crdockets/'
        self.parser = RecordParser()  # Add the parser

        # cphBody_Label4 is for Paid field
        shared_spans = ['cphBody_lblDefendant', 'cphBody_lblDefendantAttorney', 'cphBody_lblDefendantBirthDate',
                        'cphBody_lblDocketNo', 'cphBody_lblCourt']
        less_shared_spans = ['cphBody_lblTimesInCourt', 'cphBody_lblArrestingAgency', 'cphBody_lblCompanionDocketNo', 'cphBody_lblDocketType',
                             'cphBody_lblArrestDate', 'cphBody_lblCourt', 'cphBody_lblBondAmount', 'cphBody_lblBondTypeDesc', 'cphBody_lblSidebarFlag',
                             'cphBody_lblBondTypeDescHelp', 'cphBody_lblPurposeDesc', 'cphBody_lblHearingDate', 'cphBody_lblReasonDesc']
        self.needed_span = {'docket':     shared_spans + less_shared_spans, # cphBody_lblReasonDesc actually gets stored here
                            'pending':    shared_spans + less_shared_spans, # cphBody_lblReasonDesc is skipped because its value is added to PurposeDesc
                            'conviction': shared_spans + ['cphBody_lblDocketNo', 'cphBody_lblArrestingAgency', 'cphBody_lblCourt', 'cphBody_lblCost',
                                                          'cphBody_Label4', 'cphBody_lblArrestDate', 'cphBody_lblSentDate']}

    def get(self, href):
        """Modified get method with improved conviction and error handling"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                resp = ensure_connection(self.session.get, self.url + href,
                                         self.page.record_header | {'referer': f'{self.url}SearchByCourt.aspx'})
                
                if resp.status_code != 200:
                    log_issue(f"HTTP {resp.status_code} for {href}")
                    retry_count += 1
                    time.sleep(5)  # Wait before retry
                    continue
                    
                text = resp.text
                
                # Check if we got a valid page
                if 'Session Timeout' in text or 'Error' in text or len(text) < 100:
                    log_issue(f"Invalid response for {href}, retrying...")
                    retry_count += 1
                    time.sleep(5)
                    continue
                
                case_details = {}
                sentence = []
                offenses = []
                
                soup = BeautifulSoup(text, 'html.parser')
                soup._raw_html = text
                
                title_tags = soup.find_all('title')
                if not title_tags:
                    log_issue(f"No title found for {href}")
                    return case_details, offenses
                    
                title = title_tags[-1].get_text(strip=True).lower()
                
                conviction = False
                record_type = None
                for title_part, record_name in zip(('pending', 'conviction', 'case detail'),
                                                   ('pending', 'conviction', 'docket')):
                    if title_part in title:
                        record_type = record_name
                        break
                
                if record_type == 'conviction':
                    # Use parser for convictions
                    try:
                        # Store raw HTML in soup for the parser
                        soup._raw_html = text
                        
                        # Parse conviction record (this already calls parse_conviction_sentences_raw internally)
                        parsed_data = self.parser.parse_conviction_record(soup)
                        
                        # Extract modified charges for separate storage
                        modified_charges = []
                        if parsed_data and isinstance(parsed_data, dict) and 'sentences' in parsed_data:
                            sentences_data = parsed_data.get('sentences', {})
                            if isinstance(sentences_data, dict) and 'modified' in sentences_data:
                                # Convert the modified sentence format to what store_conviction_modified_charges expects
                                for mod in sentences_data['modified']:
                                    modified_charges.append({
                                        'statute': mod.get('statute', ''),
                                        'description': mod.get('description', ''),
                                        'class': mod.get('class', ''),
                                        'type': mod.get('type', ''),
                                        'occurrence': mod.get('occurrence', ''),
                                        'offense_date': mod.get('offense_date', ''),
                                        'plea': mod.get('plea', ''),
                                        'verdict_finding': mod.get('verdict_finding', ''),
                                        'verdict_date': mod.get('verdict_date', ''),
                                        'fine': mod.get('fine', ''),
                                        'fees': mod.get('fees', '')
                                    })
                        
                        # Debug output for test case
                        if 'K10K-CR17-0338221-S' in href:
                            print(f"\n{'='*60}")
                            print(f"MODIFIED CHARGES DEBUG FOR {href}")
                            print(f"{'='*60}")
                            if modified_charges:
                                print(f"Found {len(modified_charges)} modified charges:")
                                for i, charge in enumerate(modified_charges, 1):
                                    print(f"  {i}. {charge.get('statute')} - {charge.get('description')} - {charge.get('modification')}")
                            else:
                                print("WARNING: No modified charges found!")
                        
                        # Ensure parsed_data has the expected structure
                        if parsed_data and 'case_details' not in parsed_data:
                            # parse_conviction_record should already return the correct structure
                            # but just in case...
                            parsed_data = {
                                'case_details': parsed_data,
                                'sentences': {},
                                'charges': []
                            }
                        
                        # Add modified charges for later storage
                        if parsed_data:
                            parsed_data['modified_charges'] = modified_charges
                        
                        return parsed_data
                        
                    except Exception as e:
                        log_issue(f'Error parsing conviction {href}: {str(e)}')
                        # Fall back to original logic if needed
                        conviction = True

                if not record_type:
                    log_issue(f'{href} isnt a known record type. Must investigate')
                    return case_details, offenses

                # Original parsing logic for non-conviction records
                span_tags = soup.find_all('span', id = True)
                for span in span_tags:
                    if span['id'] in self.needed_span[record_type]:
                        span_id = span['id']
                        text = span.get_text(strip = True)
                        if record_type == 'pending' and span_id == 'cphBody_lblReasonDesc':
                            if 'cphBody_lblPurposeDesc' in case_details:
                                case_details['cphBody_lblPurposeDesc'] += f' {text}'
                            else:
                                case_details['cphBody_lblPurposeDesc'] = text
                        else:
                            case_details[span_id] = text

                # Rest of original logic for non-conviction records
                overall_sentence = []
                modified_sentence = []
                if conviction:
                    # This is the fallback conviction logic
                    for table_id, sentence_lst in zip(('cphBody_Datagrid2', 'cphBody_DatagridModCharge'),
                                                      (overall_sentence, modified_sentence)):
                        border = soup.find('table', class_='grdBorder', id = table_id)
                        if border:
                            rows = border.find_all('tr', class_=['grdRow', 'grdRowAlt'])
                            
                            for row in rows:
                                td_vals = [td.get_text(strip = True) for td in row.find_all('td')]

                                if any(td_vals):
                                    if len(td_vals) == 1:
                                        sentence_lst.append(td_vals[0])
                                    elif td_vals:
                                        sentence_lst.append(td_vals)

                # Find charges table with error handling
                charges_table = soup.find('table', class_='grdBorder', id=['cphBody_Datagrid1', 'cphBody_grdCharges'])
                
                if not charges_table:
                    log_issue(f"No charges table found for {href}")
                    # Return what we have so far
                    if conviction:
                        return case_details, [{'Overall Sentence': '\t'.join(overall_sentence),
                                              'Modified Sentence': '\n'.join(['\t'.join(ls) for ls in modified_sentence])},
                                              {'Charges': []}]
                    else:
                        return case_details, []
                
                charges_header = charges_table.find('tr', class_='grdHeader')
                if not charges_header:
                    log_issue(f"No charges header found for {href}")
                    if conviction:
                        return case_details, sentence
                    else:
                        return case_details, offenses

                td_elements = charges_header.find_all('td')
                if td_elements:
                    header_vals = [td.get_text(strip = True) for td in td_elements]
                    header_vals = list(filter(bool, header_vals))
                    if conviction and 'Sentenced' not in header_vals:
                        header_vals.append('Sentenced')
                else:
                    log_issue(f'{href} is missing td elements')
                    if conviction:
                        return case_details, sentence
                    else:
                        return case_details, offenses
                
                rows = charges_table.find_all('tr', class_=['grdRow', 'grdRowAlt'])
                all_offense_rows = []

                for i, row in enumerate(rows):
                    # For pending cases, the charges table has id 'cphBody_grdCharges' and might not have span IDs
                    # So we need to check if this is a pending case and handle it differently
                    if record_type == 'pending':
                        # For pending cases, just add all rows that have data
                        td_values = [td.get_text(strip=True) for td in row.find_all('td')]
                        if any(td_values):  # If row has any content
                            all_offense_rows.append(row)
                    else:
                        # For conviction/docket cases, use the original logic
                        if not any(span.get('id', '').startswith('cphBody_Datagrid1') for td in row.find_all('td') for span in td.find_all('span', id=True)):
                            continue
                        all_offense_rows.append(row)

                charge_rows = []
                seen_sentences = set()

                for row in all_offense_rows:
                    # Extract td values from the row
                    td_values = [td.get_text(strip=True) for td in row.find_all('td')]
                    
                    if td_values and td_values[-1] == '':
                        td_values = td_values[:-1]
                    
                    # For conviction cases, check for sentencing info in next row
                    if record_type != 'pending':
                        next_row = row.find_next_sibling('tr')
                        if next_row and next_row.find('td', colspan='11'):
                            sentencing_text = next_row.get_text(strip=True)
                            td_values.append(sentencing_text)
                    
                    # Handle duplicate sentences for convictions
                    if conviction:
                        offense_sentence = next((val for val in td_values if isinstance(val, str) and val.startswith('Sentenced')), None)
                        if offense_sentence and offense_sentence in seen_sentences:
                            continue
                        if offense_sentence:
                            seen_sentences.add(offense_sentence)
                    
                    # Clean up sentenced text
                    if td_values and isinstance(td_values[-1], str) and td_values[-1].startswith('Sentenced:'):
                        td_values[-1] = td_values[-1].replace('Sentenced:', '')
                    
                    charge_rows.append(td_values)

                if conviction:
                    return case_details, [{'Overall Sentence': '\t'.join(overall_sentence),
                                          'Modified Sentence': '\n'.join(['\t'.join(ls) for ls in modified_sentence])},
                                          {'Charges': [dict(zip(header_vals, charge)) for charge in charge_rows]}]
                else:
                    return case_details, [dict(zip(header_vals, charge)) for charge in charge_rows]
                    
            except Exception as e:
                log_issue(f"Error getting record {href}: {str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(5)
                    continue
                else:
                    # Return empty data after all retries failed
                    return {}, []
        
        # If all retries failed
        log_issue(f"Failed to get record {href} after {max_retries} attempts")
        return {}, []

class CustomSSLContextHTTPAdapter(requests.adapters.HTTPAdapter):
    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections, maxsize=maxsize,
            block=block, ssl_context=self.ssl_context)


if __name__ == "__main__":
    # Print configuration status
    cprint('=== DOCKET CONFIGURATION ===', Fore.CYAN)
    if DEBUGGING:
        cprint(f'DEBUG MODE ACTIVE: Limited to {DEBUG_LIMIT} records', Fore.YELLOW)
    elif LIMIT_RECORDS:
        cprint(f'LIMITED MODE: Processing up to {RECORD_LIMIT} records', Fore.YELLOW)
    else:
        cprint('FULL MODE: Processing all available records', Fore.GREEN)
    cprint('=' * 30 + '\n', Fore.CYAN)
    
    log_action(f'Starting docket.py - Debug: {DEBUGGING}, Limit: {LIMIT_RECORDS}, Max: {RECORD_LIMIT if LIMIT_RECORDS else "unlimited"}')
    
    # SSL Certificate setup
    update_cert = UpdateCert()
    
    cprint('Retrieving SSL certificate...', Fore.YELLOW)
    ssl_context = ssl.create_default_context(cadata=update_cert.get_certs_pem())
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
    ssl_context.set_ciphers('AES256-SHA256')
    
    # Session setup
    session = requests.session()
    session.adapters.pop("https://", None)
    session.mount("https://", CustomSSLContextHTTPAdapter(ssl_context))

    checkpoint_manager = CheckpointManager()
    
    # Initialize page and record handlers
    page = Page()
    record = Record(session, page)
    
    cprint('Loading docket page...', Fore.YELLOW)
    docket = Docket(session, page)
    
    docket_text = page.load_init_page(session, 'docket')
    
    cprint('Retrieving court codes...', Fore.YELLOW)
    courts = docket.get_court_codes(docket_text)
    payload = page.get_payload('docket', docket_text)
    
    search = Search(session, record, page)
    search_records = {}
    
    # Initialize database
    judicial = Judicial()
    judicial.init_db()
    
    # Process pending and conviction cases
    for search_type in ('pending', 'conviction'):
        if checkpoint_manager.should_skip_search(search_type):
            log_action(f"Skipping {search_type} - already completed")
            continue
        
        start_time = int(time.time())
        cprint(f'Loading {search_type} cases page...', Fore.YELLOW)
        log_action(f'Loading {search_type} cases page...', False)
        
        search_text = page.load_init_page(session, search_type)
        search_payload = page.get_payload('search', search_text)
        
        search_records[search_type] = search.get_records(search_payload, search_type)
        
        log_action(f'{int(time.time()) - start_time} seconds to finish')
        log_action(f'reached page {search.page_count}')
        log_action(f'record count: {len(search_records[search_type])}')
        log_action(f'sealed count: {search.sealed_count}')
        
        print('\n')
        log_action(f'Attempting to store {search_type} data in SQL...')
        
        if search_type == 'conviction':
            # Use new storage method for convictions
            stored_count = 0
            error_count = 0
            modified_charges_count = 0
            
            for conviction_record in search_records[search_type]:
                try:
                    # Check if we have the new structure
                    if isinstance(conviction_record, dict) and 'case_details' in conviction_record:
                        # Store the conviction
                        judicial.conviction_storage.store_conviction_with_sentences(conviction_record)
                        stored_count += 1
                        
                        if 'modified_charges' in conviction_record and conviction_record['modified_charges']:
                            modified_charges = conviction_record['modified_charges']
                            
                            # Get the docket NUMBER (not ID) from case_details
                            docket_no = None
                            if 'docket' in conviction_record['case_details']:
                                docket_no = conviction_record['case_details']['docket'].get('docket_number')
                            elif 'cphBody_lblDocketNo' in conviction_record['case_details']:
                                docket_no = conviction_record['case_details']['cphBody_lblDocketNo']
                            
                            if docket_no:
                                # Get the actual case_id from the database
                                judicial.conviction_storage.cursor.execute(
                                    'SELECT id FROM conviction WHERE docket_no = ?', 
                                    (docket_no,)
                                )
                                result = judicial.conviction_storage.cursor.fetchone()
                                
                                if result:
                                    case_id = result[0]  # This is the INTEGER id we need
                                    print(f"\nStoring {len(modified_charges)} modified charges for {docket_no} (case_id: {case_id})")
                                    judicial.conviction_storage.store_conviction_modified_charges(case_id, modified_charges)
                                    modified_charges_count += len(modified_charges)
                                else:
                                    log_issue(f'Could not find case_id for docket {docket_no}')
                            else:
                                log_issue(f'No docket number found for conviction with modified charges')
                                
                    else:
                        # Handle old structure if needed
                        log_issue(f'Unexpected conviction record structure: {type(conviction_record)}')
                        error_count += 1
                except Exception as e:
                    log_issue(f'Error storing conviction: {str(e)}')
                    error_count += 1
                    if DEBUGGING:
                        import traceback
                        traceback.print_exc()
            
            log_action(f'Stored {stored_count} conviction records, {modified_charges_count} modified charges, {error_count} errors')
        else:
            # Use existing storage for pending cases
            judicial.store_case(search_records[search_type], search_type)
        
        log_action(f'Finished storing {search_type} data in SQL\n')
    
    log_action('Done with pending and conviction cases')
    
    # Process daily dockets by court
    docket_by_court = {}
    case_by_court = {}
    sealed_dockets = []
    
    for code, name in courts.items():
        if DEBUGGING and code != 'N07M': 
            continue
        
        cprint(f'\nRetrieving daily docket for {name}...', Fore.YELLOW)
        
        daily = docket.get_daily(payload, code)
        docket_by_court[code] = daily
        
        cprint(f'Found {len(daily)} records\n', Fore.GREEN)
        
        case_by_court.setdefault(code, [])
        
        for listing in daily:
            if not listing['Sealed']:
                cprint(f'Reading docket {listing["Docket No."]} for {listing["Defendant (Last, First)"]}...', Fore.YELLOW)
                
                try:
                    # Get record data
                    record_data = record.get(listing['Page'])
                    
                    # Check if this is a conviction record with new structure
                    if isinstance(record_data, dict) and 'case_details' in record_data:
                        # This is a conviction with new structure - store it separately
                        judicial.conviction_storage.store_conviction_with_sentences(record_data)
                        
                        if 'modified_charges' in record_data and record_data['modified_charges']:
                            modified_charges = record_data['modified_charges']
                            docket_no = listing["Docket No."]
                            
                            # Get the actual case_id from the database
                            judicial.conviction_storage.cursor.execute(
                                'SELECT id FROM conviction WHERE docket_no = ?', 
                                (docket_no,)
                            )
                            result = judicial.conviction_storage.cursor.fetchone()
                            
                            if result:
                                case_id = result[0]  # This is the INTEGER id we need
                                print(f"Storing {len(modified_charges)} modified charges for {docket_no} (case_id: {case_id})")
                                judicial.conviction_storage.store_conviction_modified_charges(case_id, modified_charges)
                            else:
                                log_issue(f'Could not find case_id for docket {docket_no}')
        
                        cprint(f'Stored conviction record {listing["Docket No."]}', Fore.GREEN)
                    else:
                        # Regular docket case - add to case_by_court for batch processing
                        case_details, offenses = record_data
                        case_by_court[code].append((case_details, offenses))
                        
                except Exception as e:
                    log_issue(f'Error processing docket {listing["Docket No."]}: {str(e)}')
                    if DEBUGGING:
                        import traceback
                        traceback.print_exc()
            else:
                cprint(f'{listing["Docket No."]} is sealed and will be noted as such', Fore.WHITE)
                sealed_dockets.append(listing)
    
    print('\n')
    
    # Store non-conviction daily docket cases
    if any(case_by_court.values()):
        cprint('Storing daily docket cases...', Fore.YELLOW)
        judicial.store_docket(case_by_court)
        cprint('Finished storing daily docket cases', Fore.GREEN)
    
    # Close database connection
    judicial.close()
    
    # Final summary
    log_action('\n=== Processing Complete ===')
    log_action(f'Total sealed dockets: {len(sealed_dockets)}')
    log_action(f'Total records processed: {sum(len(records) for records in search_records.values())}')
    
    if DEBUGGING:
        cprint('\nDEBUGGING MODE: Limited processing', Fore.YELLOW)
    elif LIMIT_RECORDS:
        cprint(f'\nLIMITED MODE: Processed up to {RECORD_LIMIT} records', Fore.YELLOW)
    else:
        cprint('\nFULL MODE: Processed all available records', Fore.GREEN)
