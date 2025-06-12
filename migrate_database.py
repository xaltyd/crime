# migrate_database.py
# Run this script to update your existing database with the new schema

import sqlite3
from color_print import *

def migrate_database():
    """Add missing columns and tables to existing database"""
    
    conn = sqlite3.connect('records.db')
    cursor = conn.cursor()
    
    try:
        # Check if charge_sequence column exists in conviction_charges
        cursor.execute("PRAGMA table_info(conviction_charges)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'charge_sequence' not in columns:
            cprint("Adding charge_sequence column to conviction_charges table...", Fore.YELLOW)
            cursor.execute("""
                ALTER TABLE conviction_charges 
                ADD COLUMN charge_sequence INTEGER DEFAULT 0
            """)
            conn.commit()
            cprint("Added charge_sequence column", Fore.GREEN)
        else:
            cprint("charge_sequence column already exists", Fore.GREEN)
        
        # Check if conviction_sentences table exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='conviction_sentences'
        """)
        
        if not cursor.fetchone():
            cprint("Creating conviction_sentences table...", Fore.YELLOW)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS conviction_sentences (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    case_id INTEGER REFERENCES conviction(id) ON DELETE CASCADE,
                    sentence_type TEXT,
                    sentence_text TEXT,
                    sentence_date TEXT,
                    is_active BOOLEAN
                )
            """)
            conn.commit()
            cprint("Created conviction_sentences table", Fore.GREEN)
        else:
            cprint("conviction_sentences table already exists", Fore.GREEN)
        
        # Check if charge_sentence_link table exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='charge_sentence_link'
        """)
        
        if not cursor.fetchone():
            cprint("Creating charge_sentence_link table...", Fore.YELLOW)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS charge_sentence_link (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    charge_id INTEGER REFERENCES conviction_charges(id),
                    sentence_id INTEGER REFERENCES conviction_sentences(id),
                    sentence_details TEXT,
                    UNIQUE(charge_id, sentence_id)
                )
            """)
            conn.commit()
            cprint("Created charge_sentence_link table", Fore.GREEN)
        else:
            cprint("charge_sentence_link table already exists", Fore.GREEN)
        
        # Check if conviction_summary view exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='view' AND name='conviction_summary'
        """)
        
        if cursor.fetchone():
            cprint("Dropping old conviction_summary view...", Fore.YELLOW)
            cursor.execute("DROP VIEW IF EXISTS conviction_summary")
        
        cprint("Creating conviction_summary view...", Fore.YELLOW)
        cursor.execute("""
            CREATE VIEW IF NOT EXISTS conviction_summary AS
            SELECT 
                c.docket_no,
                c.version,
                c.last_first_name as defendant_name,
                c.represented_by as attorney,
                c.birth_year,
                c.arrest_date,
                c.sentenced_date,
                c.court,
                c.cost as total_cost,
                c.paid as amount_paid,
                CASE 
                    WHEN c.paid = c.cost THEN 'PAID IN FULL'
                    WHEN c.paid > '0' THEN 'PARTIALLY PAID'
                    ELSE 'UNPAID'
                END as payment_status,
                GROUP_CONCAT(DISTINCT 
                    CASE WHEN cs.sentence_type = 'OVERALL' AND cs.is_active 
                    THEN cs.sentence_text END) as current_sentence,
                GROUP_CONCAT(DISTINCT 
                    CASE WHEN cs.sentence_type = 'MODIFIED' 
                    THEN cs.sentence_text END) as modified_sentences,
                COUNT(DISTINCT ch.id) as total_charges,
                GROUP_CONCAT(DISTINCT ch.statute || ' - ' || ch.description, ', ') as all_charges,
                SUM(CAST(ch.fine AS DECIMAL(10,2))) as total_fines,
                SUM(CAST(ch.fees AS DECIMAL(10,2))) as total_fees
            FROM conviction c
            LEFT JOIN conviction_sentences cs ON c.id = cs.case_id
            LEFT JOIN conviction_charges ch ON c.id = ch.case_id
            WHERE c.version = (SELECT MAX(version) FROM conviction WHERE docket_no = c.docket_no)
            GROUP BY c.id
        """)
        conn.commit()
        cprint("Created conviction_summary view", Fore.GREEN)
        
        # Migrate existing sentence data if any
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='conviction_sentence'
        """)
        
        if cursor.fetchone():
            cprint("Migrating data from old conviction_sentence table...", Fore.YELLOW)
            cursor.execute("""
                INSERT INTO conviction_sentences (case_id, sentence_type, sentence_text, is_active)
                SELECT case_id, 'OVERALL', overall, 1
                FROM conviction_sentence
                WHERE overall IS NOT NULL AND overall != ''
            """)
            
            cursor.execute("""
                INSERT INTO conviction_sentences (case_id, sentence_type, sentence_text, is_active)
                SELECT case_id, 'MODIFIED', modified, 0
                FROM conviction_sentence
                WHERE modified IS NOT NULL AND modified != ''
            """)
            
            conn.commit()
            cprint("Migrated sentence data", Fore.GREEN)
        
        cprint("\nDatabase migration completed successfully!", Fore.GREEN)
        
    except Exception as e:
        cprint(f"Error during migration: {e}", Fore.RED)
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_database()
