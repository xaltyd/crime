# conviction_storage.py - Updated version with sentence-to-charge linking
"""
Storage module for conviction records with proper sentence linking
"""

import sqlite3
from datetime import datetime
from log import log_issue, log_action

class ConvictionStorage:
    def __init__(self, conn):
        """Initialize with an existing database connection"""
        self.conn = conn
        self.cursor = self.conn.cursor()
        self.init_tables()
    
    def init_tables(self):
        """Create or update conviction-related tables"""
        
        # Main conviction table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS conviction (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                docket_no TEXT UNIQUE,
                version INTEGER DEFAULT 1,
                last_first_name TEXT,
                represented_by TEXT,
                birth_year TEXT,
                arresting_agency TEXT,
                arrest_date TEXT,
                sentenced_date TEXT,
                court TEXT,
                cost TEXT,
                paid TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Charges table with sentence information
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS conviction_charges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id INTEGER,
                statute TEXT,
                description TEXT,
                class TEXT,
                type TEXT,
                occ TEXT,
                offense_date TEXT,
                plea TEXT,
                verdict_finding TEXT,
                verdict_date TEXT,
                fine TEXT,
                fees TEXT,
                charge_specific_sentence TEXT,
                modified_verdict_finding TEXT,
                modified_verdict_date TEXT,
                FOREIGN KEY (case_id) REFERENCES conviction(id)
            )
        ''')
        
        # Overall sentences table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS conviction_sentences (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id INTEGER,
                sentence_type TEXT,
                sentence_text TEXT,
                sentence_date TEXT,
                is_active BOOLEAN DEFAULT 1,
                FOREIGN KEY (case_id) REFERENCES conviction(id)
            )
        ''')
        
        # Modified charges table for structured modified sentence data
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS conviction_modified_charges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id INTEGER,
                statute TEXT,
                description TEXT,
                class TEXT,
                type TEXT,
                occurrence TEXT,
                offense_date TEXT,
                plea TEXT,
                verdict_finding TEXT,
                verdict_date TEXT,
                fine TEXT,
                fees TEXT,
                FOREIGN KEY (case_id) REFERENCES conviction(id)
            )
        ''')
        
        # Charge-to-sentence linking table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS conviction_charge_sentences (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                charge_id INTEGER,
                sentence_id INTEGER,
                sentence_text TEXT,
                is_modified BOOLEAN DEFAULT 0,
                FOREIGN KEY (charge_id) REFERENCES conviction_charges(id),
                FOREIGN KEY (sentence_id) REFERENCES conviction_sentences(id)
            )
        ''')
        
        self.conn.commit()
    
    def store_conviction_with_sentences(self, conviction_data):
        """Store conviction data with proper sentence linking"""
        try:
            # Extract data from the new structure
            case_details = conviction_data.get('case_details', {})
            sentences = conviction_data.get('sentences', {})
            charges = conviction_data.get('charges', [])
            
            # Debug: Print what we received
            print(f"\nDEBUG: Storing conviction data:")
            print(f"  Number of charges: {len(charges)}")
            for i, charge in enumerate(charges):
                print(f"  Charge {i}: {charge.get('Statute')} - {charge.get('Description')}")
                print(f"    Charge-specific sentence: {charge.get('charge_specific_sentence')}")
                print(f"    Modified sentence: {charge.get('modified_sentence')}")
            print(f"  Modified charges in sentences: {len(sentences.get('modified_charges', []))}")
            for mod in sentences.get('modified_charges', []):
                print(f"    Modified: {mod.get('statute')} - {mod.get('verdict_finding')}")
            
            # Get docket number
            docket_no = None
            for key in ['cphBody_lblDocketNo', 'docket_number', 'docket']:
                if key in case_details:
                    if isinstance(case_details[key], dict):
                        docket_no = case_details[key].get('docket_number')
                    else:
                        docket_no = case_details[key]
                    if docket_no:
                        break
            
            if not docket_no:
                log_issue(f"No docket number found in conviction data")
                return
            
            # Check if conviction already exists
            self.cursor.execute('SELECT id FROM conviction WHERE docket_no = ?', (docket_no,))
            existing = self.cursor.fetchone()
            
            if existing:
                case_id = existing[0]
                # Update existing record
                self.cursor.execute('''
                    UPDATE conviction 
                    SET version = version + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (case_id,))
                
                # Clear existing related data
                self.cursor.execute('DELETE FROM conviction_charges WHERE case_id = ?', (case_id,))
                self.cursor.execute('DELETE FROM conviction_sentences WHERE case_id = ?', (case_id,))
                self.cursor.execute('DELETE FROM conviction_modified_charges WHERE case_id = ?', (case_id,))
            else:
                # Insert new conviction
                self.cursor.execute('''
                    INSERT INTO conviction (
                        docket_no, last_first_name, represented_by, birth_year,
                        arresting_agency, arrest_date, sentenced_date, court, cost, paid
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    docket_no,
                    case_details.get('cphBody_lblDefendant', ''),
                    case_details.get('cphBody_lblDefendantAttorney', ''),
                    case_details.get('cphBody_lblDefendantBirthDate', ''),
                    case_details.get('cphBody_lblArrestingAgency', ''),
                    case_details.get('cphBody_lblArrestDate', ''),
                    case_details.get('cphBody_lblSentDate', ''),
                    case_details.get('cphBody_lblCourt', ''),
                    case_details.get('cphBody_lblCost', ''),
                    case_details.get('cphBody_Label4', '')
                ))
                case_id = self.cursor.lastrowid
            
            # Store overall sentence
            if sentences.get('overall'):
                self.cursor.execute('''
                    INSERT INTO conviction_sentences (case_id, sentence_type, sentence_text, sentence_date)
                    VALUES (?, ?, ?, ?)
                ''', (case_id, 'OVERALL', sentences['overall'], case_details.get('cphBody_lblSentDate', '')))
            
            # Store charges with their specific sentences
            charge_sentence_map = {}  # Track unique sentences
            
            for charge in charges:
                # Insert charge
                self.cursor.execute('''
                    INSERT INTO conviction_charges (
                        case_id, statute, description, class, type, occ,
                        offense_date, plea, verdict_finding, verdict_date,
                        fine, fees, charge_specific_sentence,
                        modified_verdict_finding, modified_verdict_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    case_id,
                    charge.get('Statute', ''),
                    charge.get('Description', ''),
                    charge.get('Class', ''),
                    charge.get('Type', ''),
                    charge.get('Occ', ''),
                    charge.get('Offense Date', ''),
                    charge.get('Plea', ''),
                    charge.get('Verdict Finding', ''),
                    charge.get('Verdict Date', ''),
                    charge.get('Fine', ''),
                    charge.get('Fee(s)', ''),
                    charge.get('charge_specific_sentence', ''),
                    charge.get('modified_sentence', {}).get('verdict_finding', '') if isinstance(charge.get('modified_sentence'), dict) else '',
                    charge.get('modified_sentence', {}).get('verdict_date', '') if isinstance(charge.get('modified_sentence'), dict) else ''
                ))
                charge_id = self.cursor.lastrowid
                
                # Handle charge-specific sentences
                charge_sentence = charge.get('charge_specific_sentence')
                if charge_sentence:
                    # Check if we've seen this sentence before
                    if charge_sentence not in charge_sentence_map:
                        # Create new sentence record
                        self.cursor.execute('''
                            INSERT INTO conviction_sentences (case_id, sentence_type, sentence_text, sentence_date)
                            VALUES (?, ?, ?, ?)
                        ''', (case_id, 'CHARGE_SPECIFIC', charge_sentence, charge.get('Verdict Date', '')))
                        sentence_id = self.cursor.lastrowid
                        charge_sentence_map[charge_sentence] = sentence_id
                    else:
                        sentence_id = charge_sentence_map[charge_sentence]
                    
                    # Link charge to sentence
                    self.cursor.execute('''
                        INSERT INTO conviction_charge_sentences (charge_id, sentence_id, sentence_text)
                        VALUES (?, ?, ?)
                    ''', (charge_id, sentence_id, charge_sentence))
            
            # Store modified charges data
            print(f"\nDEBUG: About to store {len(sentences.get('modified_charges', []))} modified charges")
            for idx, mod_charge in enumerate(sentences.get('modified_charges', [])):
                print(f"  Modified charge {idx}: {mod_charge.get('statute')} - {mod_charge.get('verdict_finding')}")
                self.cursor.execute('''
                    INSERT INTO conviction_modified_charges (
                        case_id, statute, description, class, type, occurrence,
                        offense_date, plea, verdict_finding, verdict_date, fine, fees)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    case_id,
                    mod_charge.get('statute', ''),
                    mod_charge.get('description', ''),
                    mod_charge.get('class', ''),
                    mod_charge.get('type', ''),
                    mod_charge.get('occurrence', ''),
                    mod_charge.get('offense_date', ''),
                    mod_charge.get('plea', ''),
                    mod_charge.get('verdict_finding', ''),
                    mod_charge.get('verdict_date', ''),
                    mod_charge.get('fine', ''),
                    mod_charge.get('fees', '')
                ))
            
            modified_verdicts = set()
            modified_dates = set()
            for mod_charge in sentences.get('modified_charges', []):
                verdict = mod_charge.get('verdict_finding', '')
                if verdict:
                    modified_verdicts.add(verdict)
                date = mod_charge.get('verdict_date', '')
                if date:
                    modified_dates.add(date)

            # If all modified charges have the same verdict, apply it to all charges
            if len(modified_verdicts) == 1 and modified_verdicts:
                common_verdict = modified_verdicts.pop()
                common_date = modified_dates.pop() if len(modified_dates) == 1 else ''
                
                # Apply to ALL charges in this case
                self.cursor.execute('''
                    UPDATE conviction_charges 
                    SET modified_verdict_finding = ?, 
                        modified_verdict_date = ?
                    WHERE case_id = ?
                ''', (common_verdict, common_date, case_id))
                
                print(f"Applied common modified verdict '{common_verdict}' to all charges for case_id {case_id}")
            else:
                # Different verdicts for different charges - match by statute
                for charge in charges:
                    if charge.get('modified_sentence'):
                        # This charge has modified sentence data from the parser
                        mod_data = charge['modified_sentence']
                        # The parser already updated the charge with modified data
                    else:
                        # Check if this charge appears in the modified_charges list
                        statute = charge.get('Statute', '')
                        for mod_charge in sentences.get('modified_charges', []):
                            if mod_charge.get('statute', '') == statute:
                                # Update this specific charge
                                self.cursor.execute('''
                                    UPDATE conviction_charges 
                                    SET modified_verdict_finding = ?, 
                                        modified_verdict_date = ?
                                    WHERE case_id = ? AND statute = ?
                                ''', (
                                    mod_charge.get('verdict_finding', ''),
                                    mod_charge.get('verdict_date', ''),
                                    case_id,
                                    statute
                                ))
                                break
            
            self.conn.commit()
            log_action(f"Stored conviction {docket_no} with {len(charges)} charges")
            
        except Exception as e:
            self.conn.rollback()
            log_issue(f"Error storing conviction: {str(e)}")
            raise
    
    def get_conviction_with_sentences(self, docket_no):
        """Retrieve conviction with all related sentence data"""
        self.cursor.execute('SELECT * FROM conviction WHERE docket_no = ?', (docket_no,))
        conviction = self.cursor.fetchone()
        
        if not conviction:
            return None
        
        case_id = conviction[0]
        
        # Get charges
        self.cursor.execute('''
            SELECT * FROM conviction_charges WHERE case_id = ?
        ''', (case_id,))
        charges = self.cursor.fetchall()
        
        # Get sentences
        self.cursor.execute('''
            SELECT * FROM conviction_sentences WHERE case_id = ?
        ''', (case_id,))
        sentences = self.cursor.fetchall()
        
        # Get charge-sentence links
        self.cursor.execute('''
            SELECT ccs.*, cc.statute, cc.description 
            FROM conviction_charge_sentences ccs
            JOIN conviction_charges cc ON ccs.charge_id = cc.id
            WHERE cc.case_id = ?
        ''', (case_id,))
        charge_sentences = self.cursor.fetchall()
        
        # Get modified charges
        self.cursor.execute('''
            SELECT * FROM conviction_modified_charges WHERE case_id = ?
        ''', (case_id,))
        modified_charges = self.cursor.fetchall()
        
        return {
            'conviction': conviction,
            'charges': charges,
            'sentences': sentences,
            'charge_sentences': charge_sentences,
            'modified_charges': modified_charges
        }
    
    def close(self):
        """Close database connection"""
        # Don't close the connection since it's managed externally
        pass

    def store_conviction_modified_charges(self, case_id, modified_charges):
        """
        Store modified charges from parsed conviction data.
        
        Args:
            case_id: The integer case_id from conviction table
            modified_charges: List of dictionaries containing parsed charge data
        """
        if not modified_charges:
            print(f"No modified charges to store for case {case_id}")
            return
        
        stored_count = 0
        
        print(f"\nDEBUG: store_conviction_modified_charges called for case_id {case_id}")
        print(f"DEBUG: Attempting to store {len(modified_charges)} charges")
        
        for i, charge in enumerate(modified_charges, 1):
            try:
                # Debug output for each charge
                print(f"DEBUG: Charge {i}:")
                print(f"  Statute: {charge.get('statute')} - {charge.get('description')}")
                print(f"  Class: {charge.get('class')} - Type: {charge.get('type')}")
                print(f"  Verdict: {charge.get('verdict_finding')} - Date: {charge.get('verdict_date')}")
                print(f"  Plea: {charge.get('plea')}")
                
                # Insert into database with ALL fields
                self.cursor.execute('''
                    INSERT INTO conviction_modified_charges 
                    (case_id, statute, description, class, type, occurrence, 
                     offense_date, plea, verdict_finding, verdict_date, fine, fees)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    case_id,
                    charge.get('statute', ''),
                    charge.get('description', ''),
                    charge.get('class', ''),
                    charge.get('type', ''),
                    charge.get('occurrence', ''),
                    charge.get('offense_date', ''),
                    charge.get('plea', ''),
                    charge.get('verdict_finding', ''),
                    charge.get('verdict_date', ''),
                    charge.get('fine', ''),
                    charge.get('fees', '')
                ))
                
                stored_count += 1
                print(f"DEBUG: Successfully inserted charge {i}")
                
            except Exception as e:
                print(f"ERROR storing modified charge {i} for case {case_id}: {e}")
                print(f"Charge data: {charge}")
                continue
        
        # Commit the transaction
        try:
            self.conn.commit()
            print(f"DEBUG: Transaction committed. Successfully stored {stored_count} modified charges for case {case_id}")
        except Exception as e:
            print(f"ERROR committing modified charges for case {case_id}: {e}")
            self.conn.rollback()
            raise
