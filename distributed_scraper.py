# distributed_coordinator.py
# Central coordination system for distributed scraping across multiple computers

import sqlite3
import json
import time
from datetime import datetime, timedelta
from color_print import *
import os
import argparse

class ScrapingCoordinator:
    """
    Central coordinator that tracks which pages are assigned to which workers.
    This should be accessible by all workers (shared drive, network location, or cloud storage).
    """
    
    def __init__(self, coordinator_db='scraping_coordinator.db'):
        self.db_path = coordinator_db
        self.init_database()
    
    def init_database(self):
        """Initialize the coordinator database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create pages table to track all pages
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                search_type TEXT NOT NULL,
                page_number INTEGER NOT NULL,
                status TEXT DEFAULT 'pending',  -- pending, assigned, completed, failed
                worker_id TEXT,
                assigned_at TIMESTAMP,
                completed_at TIMESTAMP,
                retry_count INTEGER DEFAULT 0,
                UNIQUE(search_type, page_number)
            )
        """)
        
        # Create workers table to track active workers
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS workers (
                worker_id TEXT PRIMARY KEY,
                hostname TEXT,
                last_heartbeat TIMESTAMP,
                pages_completed INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active'  -- active, idle, dead
            )
        """)
        
        conn.commit()
        conn.close()
    
    def initialize_pages(self, search_type, total_pages):
        """Initialize all pages for a search type"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if pages already exist
        cursor.execute("""
            SELECT COUNT(*) FROM pages WHERE search_type = ?
        """, (search_type,))
        
        if cursor.fetchone()[0] == 0:
            # Insert all pages
            pages_data = [(search_type, i) for i in range(1, total_pages + 1)]
            cursor.executemany("""
                INSERT OR IGNORE INTO pages (search_type, page_number)
                VALUES (?, ?)
            """, pages_data)
            conn.commit()
            cprint(f"Initialized {total_pages} pages for {search_type}", Fore.GREEN)
        else:
            cprint(f"Pages already initialized for {search_type}", Fore.YELLOW)
        
        conn.close()
    
    def register_worker(self, worker_id, hostname):
        """Register a worker"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT OR REPLACE INTO workers (worker_id, hostname, last_heartbeat, status)
            VALUES (?, ?, datetime('now'), 'active')
        """, (worker_id, hostname))
        
        conn.commit()
        conn.close()
    
    def get_next_page(self, search_type, worker_id):
        """Get next available page for a worker"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Start transaction to ensure atomic operation
        cursor.execute("BEGIN EXCLUSIVE")
        
        try:
            # First, check for abandoned pages (assigned but not completed after 30 minutes)
            cursor.execute("""
                UPDATE pages
                SET status = 'pending', worker_id = NULL, retry_count = retry_count + 1
                WHERE search_type = ?
                  AND status = 'assigned'
                  AND datetime(assigned_at, '+30 minutes') < datetime('now')
                  AND retry_count < 3
            """, (search_type,))
            
            # Get next pending page
            cursor.execute("""
                SELECT page_number
                FROM pages
                WHERE search_type = ?
                  AND status = 'pending'
                ORDER BY page_number
                LIMIT 1
            """, (search_type,))
            
            result = cursor.fetchone()
            
            if result:
                page_number = result[0]
                
                # Assign the page to this worker
                cursor.execute("""
                    UPDATE pages
                    SET status = 'assigned',
                        worker_id = ?,
                        assigned_at = datetime('now')
                    WHERE search_type = ?
                      AND page_number = ?
                """, (worker_id, search_type, page_number))
                
                conn.commit()
                return page_number
            else:
                conn.rollback()
                return None
                
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def mark_page_complete(self, search_type, page_number, worker_id):
        """Mark a page as completed"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE pages
            SET status = 'completed',
                completed_at = datetime('now')
            WHERE search_type = ?
              AND page_number = ?
              AND worker_id = ?
        """, (search_type, page_number, worker_id))
        
        # Update worker stats
        cursor.execute("""
            UPDATE workers
            SET pages_completed = pages_completed + 1,
                last_heartbeat = datetime('now')
            WHERE worker_id = ?
        """, (worker_id,))
        
        conn.commit()
        conn.close()
    
    def get_progress(self, search_type):
        """Get progress statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status = 'assigned' THEN 1 ELSE 0 END) as assigned,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
            FROM pages
            WHERE search_type = ?
        """, (search_type,))
        
        result = cursor.fetchone()
        conn.close()
        
        return {
            'total': result[0],
            'completed': result[1],
            'assigned': result[2],
            'pending': result[3],
            'failed': result[4]
        }
    
    def get_worker_stats(self):
        """Get statistics for all workers"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                worker_id,
                hostname,
                pages_completed,
                last_heartbeat,
                status
            FROM workers
            ORDER BY pages_completed DESC
        """)
        
        results = cursor.fetchall()
        conn.close()
        
        return results


# Modified worker script that uses the coordinator
def run_coordinated_worker(worker_id, search_type, coordinator_path):
    """Worker that gets pages from coordinator"""
    import socket
    from parallel_scraper import DistributedScraper
    
    # Get hostname
    hostname = socket.gethostname()
    
    # Create coordinator instance
    coordinator = ScrapingCoordinator(coordinator_path)
    coordinator.register_worker(worker_id, hostname)
    
    # Create scraper instance
    scraper = DistributedScraper(search_type, worker_id, 1)  # total_workers=1 since coordinator handles distribution
    scraper.setup_session()
    scraper.setup_database()
    
    cprint(f"Worker {worker_id} on {hostname} started for {search_type}", Fore.GREEN)
    
    # Get initial search payload
    search_text = scraper.page.load_init_page(scraper.session, search_type)
    search_payload = scraper.page.get_payload('search', search_text)
    
    pages_processed = 0
    
    while True:
        # Get next page from coordinator
        page_number = coordinator.get_next_page(search_type, worker_id)
        
        if page_number is None:
            cprint(f"Worker {worker_id}: No more pages to process", Fore.YELLOW)
            break
        
        cprint(f"Worker {worker_id}: Processing page {page_number}", Fore.CYAN)
        
        try:
            # Navigate to the page
            if page_number > 1:
                resp = scraper.navigate_to_page(page_number, search_payload)
            else:
                header = scraper.page.results_header | {'referer': scraper.page.url[search_type]}
                resp = ensure_connection(scraper.session.post, scraper.page.url[search_type],
                                        header, search_payload)
            
            # Process the page
            results = scraper.process_page(page_number, resp.text)
            
            # Store results
            for record_data in results:
                if search_type == 'conviction':
                    scraper.judicial.conviction_storage.store_conviction_with_sentences(record_data)
                else:
                    scraper.judicial.store_case([record_data], search_type)
            
            scraper.judicial.conn.commit()
            
            # Mark page as complete
            coordinator.mark_page_complete(search_type, page_number, worker_id)
            pages_processed += 1
            
            cprint(f"Worker {worker_id}: Completed page {page_number} (Total: {pages_processed})", Fore.GREEN)
            
        except Exception as e:
            cprint(f"Worker {worker_id}: Error on page {page_number}: {e}", Fore.RED)
            # Could mark as failed in coordinator
            
        # Small delay to be nice to the server
        time.sleep(0.5)
    
    scraper.judicial.close()
    cprint(f"Worker {worker_id} completed! Processed {pages_processed} pages", Fore.GREEN)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Coordinated distributed scraping')
    parser.add_argument('--init', action='store_true', help='Initialize coordinator database')
    parser.add_argument('--type', choices=['pending', 'conviction'], help='Type of records')
    parser.add_argument('--pages', type=int, help='Total number of pages (for init)')
    parser.add_argument('--worker', type=str, help='Worker ID (can be any unique string)')
    parser.add_argument('--coordinator', default='scraping_coordinator.db', help='Path to coordinator DB')
    parser.add_argument('--status', action='store_true', help='Show status')
    
    args = parser.parse_args()
    
    if args.init:
        if not args.type or not args.pages:
            print("Please specify --type and --pages for initialization")
            sys.exit(1)
            
        coordinator = ScrapingCoordinator(args.coordinator)
        coordinator.initialize_pages(args.type, args.pages)
        
    elif args.status:
        coordinator = ScrapingCoordinator(args.coordinator)
        
        if args.type:
            progress = coordinator.get_progress(args.type)
            print(f"\n{args.type.upper()} Progress:")
            print(f"  Total pages: {progress['total']}")
            print(f"  Completed: {progress['completed']} ({progress['completed']/progress['total']*100:.1f}%)")
            print(f"  Assigned: {progress['assigned']}")
            print(f"  Pending: {progress['pending']}")
            print(f"  Failed: {progress['failed']}")
        
        print("\nWorker Statistics:")
        workers = coordinator.get_worker_stats()
        for worker in workers:
            print(f"  {worker[0]} ({worker[1]}): {worker[2]} pages, last seen: {worker[3]}")
            
    elif args.worker:
        if not args.type:
            print("Please specify --type")
            sys.exit(1)
            
        run_coordinated_worker(args.worker, args.type, args.coordinator)
