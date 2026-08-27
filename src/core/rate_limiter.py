import time
import threading
import psycopg2
from psycopg2.extras import RealDictCursor
from src.core.config import settings

class RateLimiter:
    def __init__(self):
        self.configs = {}
        self.tokens = {}
        self.last_check = {}
        self.token_lock = threading.Lock()
        
        self.db_conn_str = f"host={settings.DB_HOST} port={settings.DB_PORT} dbname={settings.DB_NAME} user={settings.DB_USER} password={settings.DB_PASSWORD}"
        self.last_updated_at = None
        
        self.refresh_configs(initial=True)
        threading.Thread(target=self._monitor_db_changes, daemon=True).start()

    def _monitor_db_changes(self):
        while True:
            try:
                with psycopg2.connect(self.db_conn_str) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT MAX(updated_at) FROM rate_limit_config")
                        latest_update = cursor.fetchone()[0]
                        if latest_update and latest_update != self.last_updated_at:
                            self.refresh_configs()
                            self.last_updated_at = latest_update
            except Exception: pass
            time.sleep(5) 

    def refresh_configs(self, initial=False):
        try:
            with psycopg2.connect(self.db_conn_str, cursor_factory=RealDictCursor) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT * FROM rate_limit_config")
                    rows = cursor.fetchall()
            with self.token_lock:
                for row in rows:
                    channel = row['channel'].lower()
                    self.configs[channel] = dict(row)
                    if channel not in self.tokens:
                        self.tokens[channel] = float(row['max_requests'])
                        self.last_check[channel] = time.time()
            if not initial: print("[RATE LIMITER] Cache RAM berhasil diperbarui tanpa latensi!", flush=True)
        except Exception as e:
            print(f"[RATE LIMITER ERROR] {e}", flush=True)

    def acquire(self, channel: str) -> bool:
        with self.token_lock:
            config = self.configs.get(channel.lower())
            if not config or not config['is_enabled']: return True

            if config['window_unit'] == 'message':
                delay_sec = config['throttle_ms'] / 1000.0
                if delay_sec > 0 and config['queue_behavior'] in ['queue', 'delay']: time.sleep(delay_sec)
                elif config['queue_behavior'] == 'drop': return False
                return True

            now = time.time()
            time_passed = now - self.last_check[channel]
            window_sec = 60.0 if config['window_unit'] == 'minute' else (3600.0 if config['window_unit'] == 'hour' else 1.0)
            refill_rate = config['max_requests'] / window_sec
            
            self.tokens[channel] = min(config['max_requests'], self.tokens[channel] + time_passed * refill_rate)
            self.last_check[channel] = now
            
            if self.tokens[channel] >= 1.0:
                self.tokens[channel] -= 1.0
                return True
            else:
                if config['queue_behavior'] == 'drop': return False
                time.sleep((1.0 - self.tokens[channel]) / refill_rate)
                self.tokens[channel] = 0.0
                self.last_check[channel] = time.time()
                return True

global_rate_limiter = RateLimiter()