"""
Storage module for conviction records with merged sentence data in conviction_charges
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
                sentence TEXT,
                sentence_date TEXT,
                count INTEGER,
                modified_sentence_finding TEXT,
                modified_sentence_date TEXT,
                modified_sentence_fine TEXT,
                modified_sentence_fees TEXT,
                FOREIGN KEY (case_id) REFERENCES conviction(id)
            )
        ''')
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
        self.conn.commit()

    def store_conviction_with_sentences(self, conviction_data):
        try:
            case_details = conviction_data.get('case_details', {})
            sentences = conviction_data.get('sentences', {})
            charges = conviction_data.get('charges', [])

            docket_no = case_details.get('cphBody_lblDocketNo', '')
            if not docket_no:
                log_issue("No docket number found in conviction data")
                return

            self.cursor.execute('SELECT id FROM conviction WHERE docket_no = ?', (docket_no,))
            existing = self.cursor.fetchone()
            if existing:
                case_id = existing[0]
                self.cursor.execute(
                    '''
                    UPDATE conviction
                    SET version = version + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    ''', (case_id,)
                )
                self.cursor.execute('DELETE FROM conviction_charges WHERE case_id = ?', (case_id,))
                self.cursor.execute('DELETE FROM conviction_sentences WHERE case_id = ?', (case_id,))
            else:
                self.cursor.execute(
                    '''
                    INSERT INTO conviction (
                        docket_no, last_first_name, represented_by,
                        birth_year, arresting_agency, arrest_date,
                        sentenced_date, court, cost, paid
                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                    ''', (
                        docket_no,
                        case_details.get('cphBody_lblDefendant',''),
                        case_details.get('cphBody_lblDefendantAttorney',''),
                        case_details.get('cphBody_lblDefendantBirthDate',''),
                        case_details.get('cphBody_lblArrestingAgency',''),
                        case_details.get('cphBody_lblArrestDate',''),
                        case_details.get('cphBody_lblSentDate',''),
                        case_details.get('cphBody_lblCourt',''),
                        case_details.get('cphBody_lblCost',''),
                        case_details.get('cphBody_Label4','')
                    )
                )
                case_id = self.cursor.lastrowid

            overall = sentences.get('overall')
            if overall:
                self.cursor.execute(
                    '''
                    INSERT INTO conviction_sentences (case_id, sentence_type, sentence_text, sentence_date)
                    VALUES (?, 'OVERALL', ?, ?)
                    ''',
                    (case_id, overall, case_details.get('cphBody_lblSentDate',''))
                )

            raw_mods = sentences.get('modified_charges', [])
            mod_map = {}
            _mcount = {}
            for mod in raw_mods:
                key = (mod.get('statute',''), mod.get('description',''))
                _mcount[key] = _mcount.get(key, 0) + 1
                entry = {
                    'count': _mcount[key],
                    'verdict_finding': mod.get('verdict_finding',''),
                    'verdict_date': mod.get('verdict_date',''),
                    'fine': mod.get('fine',''),
                    'fees': mod.get('fees',''),
                }
                mod_map.setdefault(key, []).append(entry)

            _ccount = {}
            for ch in charges:
                stat = ch.get('Statute','')
                desc = ch.get('Description','')
                key = (stat, desc)
                _ccount[key] = _ccount.get(key, 0) + 1
                this_count = _ccount[key]
                mod_entries = mod_map.get(key, [])
                match = next((m for m in mod_entries if m['count'] == this_count), {})

                self.cursor.execute(
                    '''
                    INSERT INTO conviction_charges (
                        case_id, statute, description, class, type,
                        occ, offense_date, plea,
                        verdict_finding, verdict_date, fine, fees,
                        sentence, sentence_date, count,
                        modified_sentence_finding, modified_sentence_date,
                        modified_sentence_fine, modified_sentence_fees
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ''', (
                        case_id,
                        stat,
                        desc,
                        ch.get('Class',''),
                        ch.get('Type',''),
                        ch.get('Occ',''),
                        ch.get('Offense Date',''),
                        ch.get('Plea',''),
                        ch.get('Verdict Finding',''),
                        ch.get('Verdict Date',''),
                        ch.get('Fine',''),
                        ch.get('Fee(s)',''),
                        ch.get('charge_specific_sentence',''),
                        ch.get('Verdict Date',''),
                        this_count,
                        match.get('verdict_finding',''),
                        match.get('verdict_date',''),
                        match.get('fine',''),
                        match.get('fees','')
                    )
                )
            self.conn.commit()
            log_action(f"Stored conviction {docket_no} with {len(charges)} charges")

        except Exception as e:
            self.conn.rollback()
            log_issue(f"Error storing conviction: {e}")
            raise

    def close(self):
        """Close database connection"""
        self.conn.close()
