import time
import threading
import psycopg2
from psycopg2.extras import RealDictCursor
from src.core.config import settings

class RateLimiter:
    def __init__(self):
        self.throttle = {}  # channel -> throttle_ms
        self.lock = threading.Lock()

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
                    cursor.execute("SELECT channel, throttle_ms FROM rate_limit_config")
                    rows = cursor.fetchall()
            with self.lock:
                self.throttle = {row['channel'].lower(): row['throttle_ms'] for row in rows}
            if not initial: print("[RATE LIMITER] Cache RAM berhasil diperbarui tanpa latensi!", flush=True)
        except Exception as e:
            print(f"[RATE LIMITER ERROR] {e}", flush=True)

    def acquire(self, channel: str):
        with self.lock:
            throttle_ms = self.throttle.get(channel.lower(), 0)
        if throttle_ms > 0:
            time.sleep(throttle_ms / 1000.0)

global_rate_limiter = RateLimiter()
