# storage.py - Fixed version with better error handling
from color_print import *
from log import log_issue, log_action
from sql_modules import *
from conviction_storage import ConvictionStorage
from date_utils import parse_date
import os, sys, sqlite3

INSERT = {'docket':     INSERT_DOCKET,
          'pending':    INSERT_PENDING,
          'conviction': INSERT_CONVICTION}
          # Removed 'sentence': INSERT_SENTENCE

INSERT_CHARGE = {'docket':     INSERT_DOCKET_CHARGE,
                 'pending':    INSERT_PENDING_CHARGE,
                 'conviction': INSERT_CONVICTION_CHARGE}

CASE_ORDER = {'docket':     DOCKET_CASE_ORDER,
              'pending':    PENDING_CASE_ORDER,
              'conviction': CONVICTION_CASE_ORDER}

CHARGE_ORDER = {'docket':    DOCKET_CHARGE_ORDER,
               'pending':    PENDING_CHARGE_ORDER,
               'conviction': CONVICTION_CHARGE_ORDER}

TABLE_TYPES = [DOCKET_TABLES, PENDING_TABLES, CONVICTION_TABLES]
# Removed SENTENCE_TABLES

class Judicial:
    def __init__(self):
        self.conn = sqlite3.connect('records.db')
        self.cursor = self.conn.cursor()
        self.conviction_storage = ConvictionStorage(self.conn)

    def init_db(self):
        for table_type in TABLE_TYPES:
            for table in table_type:
                self.cursor.execute(table)
        self.conn.commit()

    def close(self):
        self.conn.close()

    def set_table_names(self, case_table, charges_table, sentence_table = None):
        self.case_table = case_table
        self.charges_table = charges_table

        if sentence_table:
            self.sentence_table = sentence_table

    def set_table_order(self, table, table_order):
        ls = [''] * len(table_order)
        for k, v in table.items():
            if k in table_order:
                try:
                    # Parse dates for specific fields
                    if k in ['cphBody_lblArrestDate', 'cphBody_lblHearingDate', 'cphBody_lblSentDate']:
                        v = parse_date(v) if v else v
                    elif k == 'Offense Date' or k == 'Verdict Date':
                        v = parse_date(v) if v else v
                        
                    ls[table_order[k]] = v
                except Exception as e:
                    print(e)
                    print(table, '\n\n', table_order)
                    input('paused')
            else:
                log_issue(f'{k} is an unrecognized field. Will not proceed until investigated')
                cinput('Cannot proceed based on type of issue', Fore.RED)
                sys.exit()
        return ls

    def get_version(self, docket_no):
        self.cursor.execute(f'SELECT MAX(version) FROM {self.case_table} WHERE docket_no = ?', (docket_no,))
        result = self.cursor.fetchone()
        return (result[0] or 0) if result else 0

    def get_case_id(self, docket_no, version):
        self.cursor.execute(f'SELECT id FROM {self.case_table} WHERE docket_no = ? and version = ?',
                            (docket_no, version,))
        result = self.cursor.fetchone()
        if result:
            return result[0]
        else:
            return None

    def get_existing_case_details(self, docket_no, version):
        self.cursor.execute(f'SELECT * FROM {self.case_table} WHERE docket_no = ? and version = ?',
                            (docket_no, version,))
        return self.cursor.fetchone()

    def get_existing_charges(self, case_id):
        self.cursor.execute(f'SELECT * FROM {self.charges_table} WHERE case_id = ?', (case_id,))
        return self.cursor.fetchall()

    def get_existing_sentence(self, case_id):
        # This method is only used for the old sentence table, which we're not using anymore
        # Keeping it for compatibility but it won't be called
        if hasattr(self, 'sentence_table'):
            self.cursor.execute(f'SELECT * FROM {self.sentence_table} WHERE case_id = ?', (case_id,))
            return self.cursor.fetchall()
        return []

    def case_compare(self, existing_details, new_details):
        return [v for i, v in enumerate(existing_details) if i not in [0, 2]] != new_details

    def charges_compare(self, existing, new):
        existing_set = [t[2:] for t in existing]
        new_set = [tuple(d.values()) for d in new]
        return sorted(existing_set) != sorted(new_set)

    def sentence_compare(self, existing, new):
        # This method is now implemented in ConvictionStorage
        pass

    def store_case(self, cases, case_type):
        """Store cases with better format handling for pending charges."""
        
        if case_type == 'conviction':
            log_issue('Conviction cases should be handled by ConvictionStorage, not store_case')
            return
        
        stored_cases = 0
        stored_charges = 0
        error_count = 0
        
        print(f"\nStoring {len(cases)} {case_type} cases...")
        
        for case in cases:
            try:
                # Handle different input formats
                charges = []
                case_details = None
                
                # Debug what we're receiving
                if stored_cases < 3:  # Debug first 3 cases
                    print(f"  Case type: {type(case)}")
                
                if isinstance(case, tuple):
                    if len(case) == 2:
                        case_details, charges = case
                    elif len(case) == 1:
                        case_details = case[0]
                        charges = []
                elif isinstance(case, dict):
                    # If it's just a dict, assume it's case_details
                    case_details = case
                    charges = []
                elif isinstance(case, list):
                    # If it's a list, first item is case_details, rest are charges
                    if len(case) > 0:
                        case_details = case[0]
                        charges = case[1:] if len(case) > 1 else []
                
                if not case_details:
                    log_issue(f'No case details found in {case_type} case')
                    error_count += 1
                    continue
                
                # Ensure charges is a list
                if not isinstance(charges, list):
                    charges = [charges] if charges else []
                
                docket_no = case_details.get('cphBody_lblDocketNo', '')
                if not docket_no:
                    log_issue(f'Missing docket number in case details: {case_details}')
                    error_count += 1
                    continue

                self.set_table_names(case_type, f'{case_type}_charges')
                
                case_details_list = self.set_table_order(case_details, CASE_ORDER[case_type])

                version = self.get_version(docket_no)
                case_id = self.get_case_id(docket_no, version) if version else None

                case_details_changed = False
                charges_changed = False
                
                if case_id:
                    existing_case_details = self.get_existing_case_details(docket_no, version)
                    existing_charges = self.get_existing_charges(case_id)

                    case_details_changed = self.case_compare(existing_case_details, case_details_list)
                    charges_changed = self.charges_compare(existing_charges, charges)

                if any((case_details_changed, charges_changed, case_id is None)):
                    new_version = version + 1
                    case_details_list.insert(1, new_version)

                    # Use INSERT OR REPLACE instead of INSERT OR IGNORE
                    insert_sql = INSERT[case_type].replace('INSERT OR IGNORE', 'INSERT OR REPLACE')
                    
                    try:
                        self.cursor.execute(insert_sql, case_details_list)
                        self.conn.commit()
                        
                        if new_version > 1:
                            print(f'  Inserted new case details for {docket_no} with ver {new_version}')
                        
                        # Get the case ID after insertion
                        case_id = self.get_case_id(docket_no, new_version)
                        
                        if not case_id:
                            # If still no case_id, try using lastrowid
                            case_id = self.cursor.lastrowid
                            if not case_id:
                                log_issue(f'Failed to get case_id for {docket_no} version {new_version}')
                                error_count += 1
                                continue

                        # Insert charges - THIS IS THE KEY FIX
                        if charges:  # Only try to store if there are charges
                            charge_count = 0
                            for charge in charges:
                                if isinstance(charge, dict) and any(charge.values()):  # Ensure charge has data
                                    try:
                                        charge_order = self.set_table_order(charge, CHARGE_ORDER[case_type])
                                        self.cursor.execute(INSERT_CHARGE[case_type], [case_id, *charge_order])
                                        charge_count += 1
                                    except Exception as e:
                                        log_issue(f'Error inserting charge for {docket_no}: {e}')
                            
                            if charge_count > 0:
                                stored_charges += charge_count
                                if stored_cases < 3:  # Debug first 3 cases
                                    print(f'  Stored {charge_count} charges for {docket_no}')

                        self.conn.commit()
                        stored_cases += 1
                        
                    except sqlite3.IntegrityError as e:
                        log_issue(f'Integrity error for {docket_no}: {e}')
                        self.conn.rollback()
                        error_count += 1
                    except Exception as e:
                        log_issue(f'Error inserting case {docket_no}: {e}')
                        self.conn.rollback()
                        error_count += 1
                        
            except Exception as e:
                log_issue(f'Error processing case: {e}')
                error_count += 1
                import traceback
                traceback.print_exc()

        self.conn.commit()
        print(f"\nSummary for {case_type}:")
        print(f"  - Stored {stored_cases} cases")
        print(f"  - Stored {stored_charges} charges")
        print(f"  - Errors: {error_count}")
    
    def store_docket(self, case_by_court):
        for code in case_by_court:
            cprint(f'Storing {code} to db...', Fore.YELLOW)

            for case in case_by_court[code]:
                try:
                    case_details, charges = case

                    docket_no = case_details.get('cphBody_lblDocketNo', '')
                    if not docket_no:
                        log_issue(f'Missing docket number in case details: {case_details}')
                        continue

                    self.set_table_names('docket', 'docket_charges')
                    case_details_list = self.set_table_order(case_details, CASE_ORDER['docket'])
                    
                    version = self.get_version(docket_no)
                    case_id = self.get_case_id(docket_no, version) if version else None

                    case_details_changed = False
                    charges_changed = False

                    if case_id:
                        existing_case_details = self.get_existing_case_details(docket_no, version)
                        existing_charges = self.get_existing_charges(case_id)

                        case_details_changed = self.case_compare(existing_case_details, case_details_list)
                        charges_changed = self.charges_compare(existing_charges, charges)

                    if case_details_changed or charges_changed or case_id is None:
                        new_version = version + 1
                        case_details_list.insert(1, new_version)

                        # Use INSERT OR REPLACE
                        insert_sql = INSERT['docket'].replace('INSERT OR IGNORE', 'INSERT OR REPLACE')
                        
                        try:
                            self.cursor.execute(insert_sql, case_details_list)
                            
                            if new_version > 1:
                                print(f'Inserted new case details for {docket_no} with ver {new_version}')
                            
                            # Get case ID
                            case_id = self.get_case_id(docket_no, new_version)
                            if not case_id:
                                case_id = self.cursor.lastrowid
                            
                            if case_id:
                                for charge in charges:
                                    charge_list = self.set_table_order(charge, CHARGE_ORDER['docket'])
                                    self.cursor.execute(INSERT_CHARGE['docket'], [case_id, *charge_list])
                            else:
                                log_issue(f'Failed to get case_id for docket {docket_no}')
                                
                        except sqlite3.IntegrityError as e:
                            log_issue(f'Integrity error for docket {docket_no}: {e}')
                            self.conn.rollback()
                        except Exception as e:
                            log_issue(f'Error inserting docket {docket_no}: {e}')
                            self.conn.rollback()
                            
                except Exception as e:
                    log_issue(f'Error processing docket case: {e}')
                    import traceback
                    traceback.print_exc()

        self.conn.commit()
