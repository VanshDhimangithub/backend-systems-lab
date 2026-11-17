"""SQLite transactional outbox exercise. No external delivery or credentials."""
import json
import sqlite3
from contextlib import contextmanager, closing

class Outbox:
    def __init__(self, path):
        self.path = str(path)
        with closing(self.connect()) as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS campaigns (
                    id TEXT PRIMARY KEY, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY, campaign_id TEXT UNIQUE NOT NULL,
                    state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
                    available_at INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(campaign_id) REFERENCES campaigns(id));
            ''')

    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.execute('PRAGMA foreign_keys=ON')
        db.row_factory = sqlite3.Row
        return db

    @contextmanager
    def transaction(self):
        db = self.connect()
        try:
            db.execute('BEGIN IMMEDIATE')
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def enqueue(self, key, payload):
        if not isinstance(key, str) or not key.strip() or len(key) > 128:
            raise ValueError('Invalid idempotency key')
        encoded = json.dumps(payload, sort_keys=True, allow_nan=False)
        with self.transaction() as db:
            prior = db.execute('SELECT payload FROM campaigns WHERE id=?', (key,)).fetchone()
            if prior:
                if prior['payload'] != encoded:
                    raise ValueError('Key reused for a different payload')
                return db.execute('SELECT id FROM jobs WHERE campaign_id=?', (key,)).fetchone()['id']
            db.execute('INSERT INTO campaigns VALUES (?, ?)', (key, encoded))
            return db.execute('INSERT INTO jobs(campaign_id) VALUES (?)', (key,)).lastrowid

    def process_one(self, deliver, now, max_attempts=3):
        """Single active worker. Delivery occurs before commit; retries may duplicate it."""
        if max_attempts < 1:
            raise ValueError('max_attempts must be positive')
        with self.transaction() as db:
            row = db.execute('''SELECT j.*, c.payload FROM jobs j
                JOIN campaigns c ON c.id=j.campaign_id
                WHERE state='pending' AND available_at<=? ORDER BY j.id LIMIT 1''', (now,)).fetchone()
            if row is None:
                return None
            attempts = row['attempts'] + 1
            try:
                # Pass a stable delivery key so a real downstream consumer can deduplicate.
                deliver(row['campaign_id'], json.loads(row['payload']))
            except Exception:
                state = 'dead' if attempts >= max_attempts else 'pending'
                db.execute('UPDATE jobs SET state=?, attempts=?, available_at=? WHERE id=?',
                           (state, attempts, now + 2 ** (attempts-1), row['id']))
            else:
                state = 'sent'
                db.execute('UPDATE jobs SET state=?, attempts=? WHERE id=?', (state, attempts, row['id']))
            return state

    def jobs(self):
        with closing(self.connect()) as db:
            return [dict(row) for row in db.execute('SELECT * FROM jobs ORDER BY id')]
