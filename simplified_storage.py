# simplified_storage.py
"""
Simplified storage handler with separate charge tables
"""
import sqlite3
import json
from datetime import datetime
from color_print import *
from log import log_issue, log_action
from simplified_sql import *

class SimplifiedStorage:
    """Storage handler for simplified schema with separate charge tables"""
    
    def __init__(self, db_path='records.db'):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.create_tables()
    
    def create_tables(self):
        """Create all tables and indexes"""
        for create_statement in ALL_TABLES:
            self.cursor.execute(create_statement)
        self.conn.commit()
        cprint("Database tables created successfully", Fore.GREEN)
    
    def get_current_version(self, docket_no, table_name):
        """Get the current version number for a docket"""
        query = f'SELECT MAX(version) FROM {table_name} WHERE docket_no = ?'
        self.cursor.execute(query, (docket_no,))
        result = self.cursor.fetchone()
        return (result[0] or 0) if result else 0
    
    def store_conviction(self, parsed_data, source_url=None):
        """
        Store conviction record with charges in the database.
        
        MODIFIED CHARGES DATA:
        =====================
        The parsed_data should contain charges with these modified fields populated
        when applicable:
        - is_modified: Boolean indicating if charge has modified sentence
        - modified_sentence_finding: e.g., "Probation Terminated"
        - modified_sentence_date: Date of modification
        - modified_fine: Modified fine amount
        - modified_fees: Modified fees amount
        
        VERIFICATION:
        For docket K10K-CR17-0338221-S, the conviction_charges table should have:
        - Row 1 (53a-116): is_modified=1, modified_sentence_finding="Probation Terminated"
        - Row 2 (53a-125a): is_modified=1, modified_sentence_finding="Probation Terminated"
        - Row 3 (53a-32): is_modified=0, modified_sentence_finding=NULL
        
        If only one charge shows modified data, the parsing pipeline is broken.
        """
        try:
            # Prepare the data
            conviction_data, charge_rows = prepare_conviction_data(parsed_data, source_url)
            
            # Check if record already exists
            self.cursor.execute(
                "SELECT id, version FROM convictions WHERE docket_no = ? ORDER BY version DESC LIMIT 1",
                (conviction_data['docket_no'],)
            )
            existing = self.cursor.fetchone()
            
            if existing:
                # Compare with existing record
                existing_id, existing_version = existing
                
                # For now, we'll just update if different (you can add more sophisticated comparison)
                conviction_data['version'] = existing_version + 1
            else:
                conviction_data['version'] = 1
            
            # Prepare values for insertion
            conviction_values = (
                conviction_data['docket_no'],
                conviction_data['version'],
                conviction_data['defendant_name'],
                conviction_data.get('defendant_attorney', ''),
                conviction_data.get('birth_year', ''),
                conviction_data.get('arresting_agency', ''),
                conviction_data.get('arrest_date', ''),
                conviction_data.get('sentenced_date', ''),
                conviction_data.get('court', ''),
                conviction_data.get('total_cost', ''),
                conviction_data.get('amount_paid', ''),
                conviction_data.get('payment_status', ''),
                conviction_data.get('overall_sentence', ''),
                conviction_data.get('is_sealed', False),
                conviction_data.get('data_source_url', source_url or ''),
                conviction_data['record_updated'].isoformat() if hasattr(conviction_data['record_updated'], 'isoformat') else conviction_data['record_updated']
            )
            
            # Insert conviction
            self.cursor.execute(INSERT_CONVICTION, conviction_values)
            case_id = self.cursor.lastrowid
            
            # Insert charges if any
            if charge_rows:
                for charge_data in charge_rows:
                    try:
                        # charge_data is a tuple with docket_no as first element
                        # We need case_id instead, so skip the first element
                        charge_values = (case_id,) + charge_data[1:]
                        self.cursor.execute(INSERT_CONVICTION_CHARGE, charge_values)
                    except Exception as e:
                        log_issue(f"Error inserting charge for case {case_id}: {e}")
            
            # Commit the transaction
            self.conn.commit()
            print(f"Stored new conviction {conviction_data['docket_no']}")
            
        except Exception as e:
            self.conn.rollback()
            log_issue(f"Error storing conviction {parsed_data.get('docket_number', 'unknown')}: {e}")
            raise
    
    def store_pending(self, parsed_data, source_url=None):
        """
        Store a pending case record with charges
        Automatically handles versioning
        """
        try:
            # Prepare the data
            pending_data, charges = prepare_pending_data(parsed_data, source_url)
            
            # Get current version
            docket_no = pending_data['docket_no']
            current_version = self.get_current_version(docket_no, 'pending')
            
            # Check if update is needed
            needs_update = False
            
            if current_version > 0:
                # Check if data has changed
                self.cursor.execute('''
                    SELECT next_hearing_date, bond_amount, bond_type
                    FROM pending 
                    WHERE docket_no = ? AND version = ?
                ''', (docket_no, current_version))
                
                existing = self.cursor.fetchone()
                if existing:
                    existing_hearing = existing[0] or ''
                    existing_bond = existing[1] or ''
                    existing_bond_type = existing[2] or ''
                    
                    new_hearing = pending_data['next_hearing_date'] or ''
                    new_bond = pending_data['bond_amount'] or ''
                    new_bond_type = pending_data['bond_type'] or ''
                    
                    if (existing_hearing != new_hearing or 
                        existing_bond != new_bond or
                        existing_bond_type != new_bond_type):
                        needs_update = True
                        log_action(f'Changes detected for pending case {docket_no}')
                else:
                    needs_update = True
                
                # Also check if charges changed
                if not needs_update:
                    self.cursor.execute('''
                        SELECT COUNT(*) FROM pending_charges pc
                        JOIN pending p ON pc.case_id = p.id
                        WHERE p.docket_no = ? AND p.version = ?
                    ''', (docket_no, current_version))
                    existing_charge_count = self.cursor.fetchone()[0]
                    if existing_charge_count != len(charges):
                        needs_update = True
                        log_action(f'Charge count changed for pending case {docket_no}')
            else:
                needs_update = True
            
            if needs_update:
                new_version = current_version + 1
                
                # Insert new version
                values = (
                    docket_no, new_version,
                    pending_data['defendant_name'],
                    pending_data['defendant_attorney'],
                    pending_data['birth_year'],
                    pending_data['arresting_agency'],
                    pending_data['arrest_date'],
                    pending_data['court'],
                    pending_data['times_on_docket'],
                    pending_data['companion_cases'],
                    pending_data['docket_type'],
                    pending_data['bond_amount'],
                    pending_data['bond_type'],
                    pending_data['custody_status'],
                    pending_data['next_hearing_date'],
                    pending_data['hearing_purpose'],
                    pending_data['hearing_reason'],
                    False,  # is_sealed
                    pending_data['data_source_url'],
                    pending_data['misc_notes'],
                    pending_data['record_updated']
                )
                
                self.cursor.execute(INSERT_PENDING, values)
                case_id = self.cursor.lastrowid
                
                # Insert charges
                for i, charge in enumerate(charges):
                    charge_values = (
                        case_id,
                        i,  # charge_sequence
                        charge.get('Statute', ''),
                        charge.get('Description', ''),
                        charge.get('Class', ''),
                        charge.get('Type', ''),
                        charge.get('Occ', ''),
                        charge.get('Offense Date', ''),
                        charge.get('Plea', ''),
                        charge.get('Verdict Finding', '')
                    )
                    self.cursor.execute(INSERT_PENDING_CHARGE, charge_values)
                
                self.conn.commit()
                
                if new_version > 1:
                    cprint(f'Updated pending case {docket_no} to version {new_version}', Fore.GREEN)
                else:
                    cprint(f'Stored new pending case {docket_no}', Fore.CYAN)
                
                return True
            else:
                return False
                
        except Exception as e:
            log_issue(f'Error storing pending case {docket_no}: {str(e)}')
            self.conn.rollback()
            raise
    
    def mark_sealed(self, docket_no, table_name):
        """Mark a case as sealed"""
        query = f'UPDATE {table_name} SET is_sealed = TRUE WHERE docket_no = ?'
        self.cursor.execute(query, (docket_no,))
        self.conn.commit()
    
    def get_financial_summary(self):
        """Get financial summary across all convictions"""
        query = '''
        SELECT 
            COUNT(DISTINCT docket_no) as total_cases,
            SUM(CAST(total_cost AS REAL)) as total_owed,
            SUM(CAST(amount_paid AS REAL)) as total_paid,
            SUM(CASE WHEN payment_status = 'PAID IN FULL' THEN 1 ELSE 0 END) as paid_in_full,
            SUM(CASE WHEN payment_status = 'PARTIALLY PAID' THEN 1 ELSE 0 END) as partially_paid,
            SUM(CASE WHEN payment_status = 'UNPAID' THEN 1 ELSE 0 END) as unpaid
        FROM (
            SELECT DISTINCT ON (docket_no) * 
            FROM convictions 
            ORDER BY docket_no, version DESC
        ) latest_convictions
        '''
        
        # SQLite doesn't support DISTINCT ON, so we need a different approach
        query = '''
        SELECT 
            COUNT(DISTINCT docket_no) as total_cases,
            SUM(CAST(total_cost AS REAL)) as total_owed,
            SUM(CAST(amount_paid AS REAL)) as total_paid,
            SUM(CASE WHEN payment_status = 'PAID IN FULL' THEN 1 ELSE 0 END) as paid_in_full,
            SUM(CASE WHEN payment_status = 'PARTIALLY PAID' THEN 1 ELSE 0 END) as partially_paid,
            SUM(CASE WHEN payment_status = 'UNPAID' THEN 1 ELSE 0 END) as unpaid
        FROM convictions c1
        WHERE version = (
            SELECT MAX(version) 
            FROM convictions c2 
            WHERE c2.docket_no = c1.docket_no
        )
        '''
        
        self.cursor.execute(query)
        return self.cursor.fetchone()
    
    def close(self):
        """Close database connection"""
        self.conn.close()
    
    # LLM-friendly query methods
    def get_charges_for_case(self, docket_no, case_type='conviction'):
        """Get all charges for a specific case"""
        if case_type == 'conviction':
            query = '''
            SELECT cc.* 
            FROM conviction_charges cc
            JOIN convictions c ON cc.case_id = c.id
            WHERE c.docket_no = ? 
            AND c.version = (SELECT MAX(version) FROM convictions WHERE docket_no = ?)
            ORDER BY cc.charge_sequence
            '''
        else:
            query = '''
            SELECT pc.* 
            FROM pending_charges pc
            JOIN pending p ON pc.case_id = p.id
            WHERE p.docket_no = ? 
            AND p.version = (SELECT MAX(version) FROM pending WHERE docket_no = ?)
            ORDER BY pc.charge_sequence
            '''
        
        self.cursor.execute(query, (docket_no, docket_no))
        return self.cursor.fetchall()
    
    def search_by_statute(self, statute_code, case_type='conviction'):
        """Search cases by statute code"""
        if case_type == 'conviction':
            query = '''
            SELECT DISTINCT c.docket_no, c.defendant_name, cc.statute, cc.description
            FROM convictions c
            JOIN conviction_charges cc ON c.id = cc.case_id
            WHERE cc.statute LIKE ?
            AND c.version = (SELECT MAX(version) FROM convictions c2 WHERE c2.docket_no = c.docket_no)
            '''
        else:
            query = '''
            SELECT DISTINCT p.docket_no, p.defendant_name, pc.statute, pc.description
            FROM pending p
            JOIN pending_charges pc ON p.id = pc.case_id
            WHERE pc.statute LIKE ?
            AND p.version = (SELECT MAX(version) FROM pending p2 WHERE p2.docket_no = p.docket_no)
            '''
        
        self.cursor.execute(query, (f'%{statute_code}%',))
        return self.cursor.fetchall()
