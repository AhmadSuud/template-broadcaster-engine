import time
import threading
import psycopg2
from psycopg2.extras import RealDictCursor
from src.core.config import settings

class ChannelManager:
    def __init__(self):
        self.accounts = {}
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
                        cursor.execute("SELECT MAX(updated_at) FROM channel_accounts")
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
                    cursor.execute("SELECT channel, config FROM channel_accounts WHERE enabled = true AND is_default = true")
                    rows = cursor.fetchall()
            with self.lock:
                for row in rows:
                    self.accounts[row['channel'].lower()] = row['config']
            if not initial: print("[CHANNEL MANAGER] Kredensial Pengiriman (DB) diperbarui!", flush=True)
        except Exception as e:
            print(f"[CHANNEL MANAGER ERROR] {e}", flush=True)

    def get_account(self, channel: str) -> dict:
        with self.lock: return self.accounts.get(channel.lower())

global_channel_manager = ChannelManager()