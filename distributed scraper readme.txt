DISTRIBUTED SCRAPING INSTRUCTIONS
=================================

This system allows you to scrape conviction and pending records across multiple computers
without overlaps or duplicate work.

INITIAL SETUP
-------------

1. Choose a coordinator database location that all computers can access:
   - Network share: //server/share/scraping_coordinator.db
   - Cloud sync folder: Dropbox/Google Drive/OneDrive
   - USB drive (if moving between computers)

2. Initialize the coordinator database (run once from any computer):

   For conviction records (approximately 1500 pages):
   python distributed_coordinator.py --init --type conviction --pages 1500 --coordinator /path/to/scraping_coordinator.db

   For pending records (approximately 800 pages):
   python distributed_coordinator.py --init --type pending --pages 800 --coordinator /path/to/scraping_coordinator.db


RUNNING WORKERS ON EACH COMPUTER
--------------------------------

On each computer/device, run a worker with a unique ID:

Computer 1:
python distributed_coordinator.py --worker PC1 --type conviction --coordinator /path/to/scraping_coordinator.db

Computer 2:
python distributed_coordinator.py --worker PC2 --type conviction --coordinator /path/to/scraping_coordinator.db

Computer 3:
python distributed_coordinator.py --worker LAPTOP1 --type conviction --coordinator /path/to/scraping_coordinator.db

Computer 4:
python distributed_coordinator.py --worker OFFICE_PC --type conviction --coordinator /path/to/scraping_coordinator.db

Note: 
- Worker IDs can be any unique string (PC1, LAPTOP_JOHN, DESKTOP_OFFICE, etc.)
- Replace /path/to/ with the actual path to your coordinator database
- For pending records, change --type conviction to --type pending


MONITORING PROGRESS
-------------------

Check progress from any computer:
python distributed_coordinator.py --status --type conviction --coordinator /path/to/scraping_coordinator.db

This shows:
- Total pages and completion percentage
- How many pages are assigned/pending/completed
- Which workers are active and their progress


AFTER SCRAPING COMPLETES
------------------------

1. Each worker creates its own database file:
   - records_conviction_worker_PC1.db
   - records_conviction_worker_PC2.db
   - etc.

2. Copy all worker databases to one computer

3. Merge them into the main database:
   python parallel_scraper.py --type conviction --merge --total-workers 4

   (Replace 4 with the actual number of workers you used)


IMPORTANT NOTES
---------------

- Workers automatically handle page distribution - no manual page ranges needed
- If a worker crashes, restart it with the same worker ID to continue
- Pages abandoned for 30+ minutes are automatically reassigned
- Each worker processes pages at its own speed (faster computers do more)
- Add a small delay between requests to avoid overwhelming the server
- The coordinator.db file must be accessible to all workers during scraping


TROUBLESHOOTING
---------------

If a worker gets stuck:
1. Stop the worker (Ctrl+C)
2. Check the status to see which pages are assigned to it
3. Restart the worker with the same ID
4. Abandoned pages will be reassigned after 30 minutes

If the coordinator database gets corrupted:
1. Make a backup of all worker databases
2. Re-initialize the coordinator
3. Check worker databases to see which pages were completed
4. Manually update the coordinator or re-scrape missing pages


EXAMPLE FULL WORKFLOW
---------------------

1. Set up shared folder on network: \\SERVER\Scraping\coordinator.db

2. Initialize coordinator for both types:
   python distributed_coordinator.py --init --type conviction --pages 1500 --coordinator \\SERVER\Scraping\coordinator.db
   python distributed_coordinator.py --init --type pending --pages 800 --coordinator \\SERVER\Scraping\coordinator.db

3. Start workers on 4 computers for convictions:
   PC1: python distributed_coordinator.py --worker PC1 --type conviction --coordinator \\SERVER\Scraping\coordinator.db
   PC2: python distributed_coordinator.py --worker PC2 --type conviction --coordinator \\SERVER\Scraping\coordinator.db
   PC3: python distributed_coordinator.py --worker PC3 --type conviction --coordinator \\SERVER\Scraping\coordinator.db
   PC4: python distributed_coordinator.py --worker PC4 --type conviction --coordinator \\SERVER\Scraping\coordinator.db

4. Monitor progress periodically:
   python distributed_coordinator.py --status --type conviction --coordinator \\SERVER\Scraping\coordinator.db

5. When complete, gather all worker databases and merge:
   python parallel_scraper.py --type conviction --merge --total-workers 4

6. Repeat steps 3-5 for pending records