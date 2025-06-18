# docket.py - Updated for simplified two-table schema
"""
Main docket processing script with simplified storage
Maintains SSL handling and original functionality
"""
from bs4 import BeautifulSoup
from color_print import *
from datetime import datetime
from simplified_storage import SimplifiedStorage
from log import log_issue, log_action
import requests, sys, os, json, io
import socket, ssl, urllib3, time
from record_parser import RecordParser
from checkpoint import CheckpointManager

server_host = 'www.jud2.ct.gov'
server_url  = f'https://{server_host}'

DEBUGGING = False
LIMIT_RECORDS = True  # Set to False to process all records
RECORD_LIMIT = 50    # Number of records to process when LIMIT_RECORDS is True
DEBUG_LIMIT = 20      # Number of records in debug mode

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

        self.url = {'pending':    f'{crdockets_url}/parm1.aspx',
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

        self.payload = {'search': {'_ctl0:cphBody:txtDefendantFullName': '_',
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
        self.page_count = 0
        self.sealed_count = 0

    def get_records(self, payload, search_type):
        """Modified to handle new conviction data structure"""
        page = 1
        results = []
        cph_body_id = '_ctl0$cphBody$grdDockets$_ctl54$'
        self.sealed_count = 0
        
        header = self.page.results_header | {'referer': self.page.url[search_type]}
        
        resp = ensure_connection(self.session.post, self.page.url[search_type],
                                 header, payload)
        
        count = 0
        # Apply record limit if configured
        max_records = DEBUG_LIMIT if DEBUGGING else (RECORD_LIMIT if LIMIT_RECORDS else float('inf'))
        
        while f'<span>{page}</span>' in resp.text and count < max_records:
            soup = BeautifulSoup(resp.text, 'html.parser')
            grd_header = soup.find('tr', class_=['grdHeader'])
            header_vals = [a.get_text(strip=True) for a in grd_header.find_all('a')]
            
            rows = soup.find_all('tr', class_=['grdRow', 'grdRowAlt'])
            for row in rows:
                if count >= max_records:
                    break
                    
                a_tag = row.find('a', id=True)
                if a_tag:
                    a_href = a_tag.get('href')
                    a_title = a_tag.get('title')
                
                if 'Sealed' not in a_title:
                    # Get the record data
                    record_data = self.record.get(a_href)
                    
                    # For convictions, the parser returns the full structure we need
                    if search_type == 'conviction' and isinstance(record_data, dict):
                        # The parser already returns the correct structure
                        # We need to wrap it properly for the storage layer
                        wrapped_data = {
                            'case_details': {
                                'docket_number': record_data.get('docket_number'),
                                'defendant_name': record_data.get('defendant_name'),
                                'defendant_attorney': record_data.get('defendant_attorney'),  # if available
                                'birth_year': record_data.get('birth_year'),
                                'arresting_agency': record_data.get('arresting_agency'),
                                'arrest_date': record_data.get('arrest_date'),
                                'sentenced_date': record_data.get('disposition_date'),  # or sentenced_date
                                'court': record_data.get('court'),
                                'total_cost': record_data.get('total_cost', ''),
                                'amount_paid': record_data.get('amount_paid', ''),
                                'payment_status': record_data.get('payment_status', ''),
                                'overall_sentence': record_data.get('overall_sentence', ''),
                                'total_fines_amount': 0.0,  # calculate from charges if needed
                                'total_fees_amount': 0.0,   # calculate from charges if needed
                                'is_sealed': False,
                                'source_url': record_data.get('href', a_href)
                            },
                            'sentences': {},  # Add if you parse sentences
                            'charges': record_data.get('charges', [])
                        }
                        results.append(wrapped_data)
                    else:  # pending
                        results.append(record_data)
                    
                    print(f'Added data for href {a_href} ({count}/{max_records})')
                    count += 1
                else:
                    self.sealed_count += 1

            if count >= max_records:
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
        self.parser = RecordParser()

        shared_spans = ['cphBody_lblDefendant', 'cphBody_lblDefendantAttorney', 'cphBody_lblDefendantBirthDate',
                        'cphBody_lblDocketNo', 'cphBody_lblCourt']
        less_shared_spans = ['cphBody_lblTimesInCourt', 'cphBody_lblArrestingAgency', 'cphBody_lblCompanionDocketNo', 'cphBody_lblDocketType',
                             'cphBody_lblArrestDate', 'cphBody_lblCourt', 'cphBody_lblBondAmount', 'cphBody_lblBondTypeDesc', 'cphBody_lblSidebarFlag',
                             'cphBody_lblBondTypeDescHelp', 'cphBody_lblPurposeDesc', 'cphBody_lblHearingDate', 'cphBody_lblReasonDesc']
        self.needed_span = {'pending':    shared_spans + less_shared_spans,
                            'conviction': shared_spans + ['cphBody_lblDocketNo', 'cphBody_lblArrestingAgency', 'cphBody_lblCourt', 'cphBody_lblCost',
                                                          'cphBody_Label4', 'cphBody_lblArrestDate', 'cphBody_lblSentDate']}

    def get(self, href):
        """
        Fetch and parse court record HTML from the given href.
        
        CRITICAL FOR CONVICTION RECORDS - MALFORMED HTML ISSUE:
        =====================================================
        The Connecticut Judicial website has MALFORMED HTML in conviction records,
        specifically in the "Modified Sentence Information" table. The HTML contains
        invalid nested tags like: <span></td></tr><tr><td></tr></span>
        
        This causes BeautifulSoup to "fix" the HTML by truncating it, which results
        in losing modified charge data (e.g., "Probation Terminated" statuses).
        
        SOLUTION:
        - For conviction records, we MUST pass the RAW HTML STRING (resp.text) to
          the parser, NOT a BeautifulSoup object
        - The parser will use regex on the raw HTML to extract modified charges
        - DO NOT let BeautifulSoup process the HTML before passing to the parser
        
        EXAMPLE OF DATA LOSS:
        - Raw HTML modified table: 2507 characters, 2 charge rows
        - After BeautifulSoup: 1387 characters, only 1 charge row
        - Second charge loses its "Probation Terminated" status
        
        TEST CASE: Docket K10K-CR17-0338221-S should show "Probation Terminated"
        for BOTH charges (53a-116 and 53a-125a) in the modified_sentence_finding
        column. If only one shows it, this parsing is broken.
        """
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                resp = ensure_connection(self.session.get, self.url + href,
                                         self.page.record_header | {'referer': f'{self.url}SearchByCourt.aspx'})
                
                if resp.status_code != 200:
                    log_issue(f"HTTP {resp.status_code} for {href}")
                    retry_count += 1
                    time.sleep(5)
                    continue
                    
                # CRITICAL: Store the raw text BEFORE any BeautifulSoup processing
                # This preserves the malformed HTML that contains all the data
                text = resp.text
                
                # Check if we got a valid page
                if 'Session Timeout' in text or 'Error' in text or len(text) < 100:
                    log_issue(f"Invalid response for {href}, retrying...")
                    retry_count += 1
                    time.sleep(5)
                    continue
                
                soup = BeautifulSoup(text, 'html.parser')
                
                title_tags = soup.find_all('title')
                if not title_tags:
                    log_issue(f"No title found for {href}")
                    return {}
                    
                title = title_tags[-1].get_text(strip=True).lower()
                
                # Determine record type
                record_type = None
                for title_part, record_name in zip(('pending', 'conviction'),
                                                   ('pending', 'conviction')):
                    if title_part in title:
                        record_type = record_name
                        break
                
                if not record_type:
                    log_issue(f'{href} isnt a known record type. Must investigate')
                    return {}
                
                # Use parser for convictions
                if record_type == 'conviction':
                    try:
                        # CRITICAL CHANGE: Pass raw HTML text, NOT the soup object
                        # The parser needs the malformed HTML intact to extract all charges
                        parsed_data = self.parser.parse_conviction_record(text)  # <-- Pass raw HTML
                        # Add href for reference
                        parsed_data['href'] = self.url + href
                        return parsed_data
                    except Exception as e:
                        log_issue(f'Error parsing conviction {href}: {str(e)}')
                        return {}
                
                # Use parser for pending cases
                elif record_type == 'pending':
                    try:
                        # For pending, we can still use soup (no malformed HTML issue)
                        parsed_data = self.parser.parse_pending_record(soup)
                        # Add href for reference
                        parsed_data['href'] = self.url + href
                        return parsed_data
                    except Exception as e:
                        log_issue(f'Error parsing pending {href}: {str(e)}')
                        return {}
                        
            except Exception as e:
                log_issue(f"Error getting record {href}: {str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(5)
                    continue
                else:
                    return {}
        
        # If all retries failed
        log_issue(f"Failed to get record {href} after {max_retries} attempts")
        return {}

class CustomSSLContextHTTPAdapter(requests.adapters.HTTPAdapter):
    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections, maxsize=maxsize,
            block=block, ssl_context=self.ssl_context)


if __name__ == "__main__":
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
    
    search = Search(session, record, page)
    search_records = {}
    
    # Initialize simplified storage
    storage = SimplifiedStorage()
    
    # Show configuration
    cprint("\n=== Connecticut Court Docket Processor ===", Fore.CYAN)
    cprint(f"Configuration:", Fore.WHITE)
    cprint(f"  LIMIT_RECORDS: {LIMIT_RECORDS}", Fore.YELLOW if LIMIT_RECORDS else Fore.GREEN)
    if LIMIT_RECORDS:
        cprint(f"  Record limit: {DEBUG_LIMIT if DEBUGGING else RECORD_LIMIT}", Fore.YELLOW)
    cprint(f"  Debug mode: {DEBUGGING}", Fore.YELLOW if DEBUGGING else Fore.GREEN)
    
    # Process pending and conviction cases
    for search_type in ('pending', 'conviction'):
        if checkpoint_manager.should_skip_search(search_type):
            log_action(f"Skipping {search_type} - already completed")
            continue
        
        start_time = int(time.time())
        cprint(f'\nLoading {search_type} cases page...', Fore.YELLOW)
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
        
        stored_count = 0
        error_count = 0
        
        for record_data in search_records[search_type]:
            try:
                if search_type == 'conviction':
                    if isinstance(record_data, dict) and 'case_details' in record_data:
                        source_url = record_data.get('href', '')
                        storage.store_conviction(record_data, source_url)
                        stored_count += 1
                    else:
                        log_issue(f'Unexpected conviction record structure: {type(record_data)}')
                        error_count += 1
                else:  # pending
                    if isinstance(record_data, dict) and 'case_details' in record_data:
                        source_url = record_data.get('href', '')
                        storage.store_pending(record_data, source_url)
                        stored_count += 1
                    else:
                        log_issue(f'Unexpected pending record structure: {type(record_data)}')
                        error_count += 1
            except Exception as e:
                log_issue(f'Error storing {search_type}: {str(e)}')
                error_count += 1
                if DEBUGGING:
                    import traceback
                    traceback.print_exc()
        
        log_action(f'Stored {stored_count} {search_type} records, {error_count} errors')
        log_action(f'Finished storing {search_type} data in SQL\n')
    
    log_action('Done with pending and conviction cases')
    
    # Show statistics
    cprint("\n=== Database Statistics ===", Fore.CYAN)
    
    # Conviction stats
    storage.cursor.execute(
        "SELECT COUNT(DISTINCT docket_no), COUNT(*) FROM convictions"
    )
    unique_convictions, total_conviction_records = storage.cursor.fetchone()
    
    # Pending stats
    storage.cursor.execute(
        "SELECT COUNT(DISTINCT docket_no), COUNT(*) FROM pending"
    )
    unique_pending, total_pending_records = storage.cursor.fetchone()
    
    # Financial summary
    financial = storage.get_financial_summary()
    
    cprint(f"\nConviction Cases:", Fore.WHITE)
    cprint(f"  Unique cases: {unique_convictions}", Fore.GREEN)
    cprint(f"  Total records (with versions): {total_conviction_records}", Fore.GREEN)
    
    cprint(f"\nPending Cases:", Fore.WHITE)
    cprint(f"  Unique cases: {unique_pending}", Fore.GREEN)
    cprint(f"  Total records (with versions): {total_pending_records}", Fore.GREEN)
    
    if financial:
        cprint(f"\nFinancial Summary:", Fore.WHITE)
        cprint(f"  Total cases with costs: {financial[0]}", Fore.GREEN)
        cprint(f"  Total owed: ${financial[1]:,.2f}" if financial[1] else "  Total owed: $0.00", Fore.YELLOW)
        cprint(f"  Total paid: ${financial[2]:,.2f}" if financial[2] else "  Total paid: $0.00", Fore.GREEN)
        cprint(f"  Paid in full: {financial[3]}", Fore.GREEN)
        cprint(f"  Partially paid: {financial[4]}", Fore.YELLOW)
        cprint(f"  Unpaid: {financial[5]}", Fore.RED)
    
    # Close storage
    storage.close()
    
    # Final summary
    log_action('\n=== Processing Complete ===')
    
    if DEBUGGING:
        cprint('\nDEBUGGING MODE: Limited processing', Fore.YELLOW)
