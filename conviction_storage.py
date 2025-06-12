# conviction_storage.py - Simplified version without redundant columns
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
        
        # Charges table WITHOUT redundant modified verdict columns
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
        
        # Modified charges table - THE source of truth for modified verdicts
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
    
    def migrate_remove_redundant_columns(self):
        """Remove redundant modified verdict columns from conviction_charges if they exist"""
        try:
            # Check if columns exist
            self.cursor.execute("PRAGMA table_info(conviction_charges)")
            columns = [col[1] for col in self.cursor.fetchall()]
            
            if 'modified_verdict_finding' in columns or 'modified_verdict_date' in columns:
                print("Migrating conviction_charges table to remove redundant columns...")
                
                # Create new table without redundant columns
                self.cursor.execute('''
                    CREATE TABLE conviction_charges_new (
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
                        FOREIGN KEY (case_id) REFERENCES conviction(id)
                    )
                ''')
                
                # Copy data (excluding redundant columns)
                self.cursor.execute('''
                    INSERT INTO conviction_charges_new 
                    (id, case_id, statute, description, class, type, occ, 
                     offense_date, plea, verdict_finding, verdict_date, 
                     fine, fees, charge_specific_sentence)
                    SELECT id, case_id, statute, description, class, type, occ, 
                           offense_date, plea, verdict_finding, verdict_date, 
                           fine, fees, charge_specific_sentence
                    FROM conviction_charges
                ''')
                
                # Drop old table and rename new one
                self.cursor.execute('DROP TABLE conviction_charges')
                self.cursor.execute('ALTER TABLE conviction_charges_new RENAME TO conviction_charges')
                
                self.conn.commit()
                print("Migration completed successfully")
                
        except Exception as e:
            print(f"Migration error (may be ignorable if columns don't exist): {e}")
    
    def store_conviction_with_sentences(self, conviction_data):
        """Store conviction data with proper sentence linking"""
        try:
            # Extract data from the new structure
            case_details = conviction_data.get('case_details', {})
            sentences = conviction_data.get('sentences', {})
            charges = conviction_data.get('charges', [])
            
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
            
            # Store charges with their specific sentences (NO modified verdict columns)
            charge_sentence_map = {}  # Track unique sentences
            
            for charge in charges:
                # Insert charge WITHOUT modified verdict columns
                self.cursor.execute('''
                    INSERT INTO conviction_charges (
                        case_id, statute, description, class, type, occ,
                        offense_date, plea, verdict_finding, verdict_date,
                        fine, fees, charge_specific_sentence
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    charge.get('charge_specific_sentence', '')
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
            
            # NOTE: Modified charges are stored separately via store_conviction_modified_charges()
            # to avoid duplication. The 'modified' data in sentences is handled by docket.py
            
            self.conn.commit()
            log_action(f"Stored conviction {docket_no} with {len(charges)} charges")
            
        except Exception as e:
            self.conn.rollback()
            log_issue(f"Error storing conviction: {str(e)}")
            raise
    
    def store_conviction_modified_charges(self, case_id, modified_charges):
        """
        Store modified charges from parsed conviction data.
        This is THE authoritative source for modified verdict information.
        
        Args:
            case_id: The integer case_id from conviction table
            modified_charges: List of dictionaries containing parsed charge data
        """
        if not modified_charges:
            print(f"No modified charges to store for case {case_id}")
            return
        
        # Check if we already have modified charges for this case
        self.cursor.execute('SELECT COUNT(*) FROM conviction_modified_charges WHERE case_id = ?', (case_id,))
        existing_count = self.cursor.fetchone()[0]
        
        if existing_count > 0:
            log_action(f"Modified charges already exist for case {case_id}, skipping to avoid duplicates")
            return
        
        stored_count = 0
        
        for i, charge in enumerate(modified_charges, 1):
            try:
                # Check if this specific charge already exists
                self.cursor.execute('''
                    SELECT id FROM conviction_modified_charges 
                    WHERE case_id = ? AND statute = ? AND verdict_finding = ?
                ''', (case_id, charge.get('statute', ''), charge.get('verdict_finding', '')))
                
                if self.cursor.fetchone():
                    log_action(f"Modified charge {charge.get('statute')} already exists for case {case_id}, skipping")
                    continue
                
                # Insert into database
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
                
            except Exception as e:
                log_issue(f"Error storing modified charge {i} for case {case_id}: {e}")
                continue
        
        # Commit the transaction
        try:
            self.conn.commit()
            if stored_count > 0:
                log_action(f"Successfully stored {stored_count} modified charges for case {case_id}")
        except Exception as e:
            log_issue(f"Error committing modified charges for case {case_id}: {e}")
            self.conn.rollback()
            raise
    
    def get_conviction_with_sentences(self, docket_no):
        """Retrieve conviction with all related sentence data including modified charges"""
        self.cursor.execute('SELECT * FROM conviction WHERE docket_no = ?', (docket_no,))
        conviction = self.cursor.fetchone()
        
        if not conviction:
            return None
        
        case_id = conviction[0]
        
        # Get original charges
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
        
        # Get modified charges - THE authoritative source for modified verdicts
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
    
    def get_modified_verdict_for_charge(self, case_id, statute):
        """
        Get the modified verdict for a specific charge by matching statute.
        This queries the conviction_modified_charges table which is the source of truth.
        """
        self.cursor.execute('''
            SELECT verdict_finding, verdict_date 
            FROM conviction_modified_charges 
            WHERE case_id = ? AND statute = ?
        ''', (case_id, statute))
        
        result = self.cursor.fetchone()
        if result:
            return {
                'verdict_finding': result[0],
                'verdict_date': result[1]
            }
        return None
    
    def close(self):
        """Close database connection"""
        # Don't close the connection since it's managed externally
        pass
