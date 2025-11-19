#!/usr/bin/env python3
"""
Real-time Polymarket price display for Bitcoin Up or Down markets.
Fetches UP and DOWN token prices and displays them in real-time.
"""
import sys
import requests
import re
import json
import asyncio
import websockets
from datetime import datetime
import pytz
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QLabel, QLineEdit, QPushButton)
from PyQt5.QtCore import QTimer, Qt, QThread, pyqtSignal, QObject
from PyQt5.QtGui import QFont

# WebSocket server URL
SERVER_URL = ""


class WebSocketThread(QThread):
    """Thread that handles WebSocket connection and emits signals for UI updates."""
    data_received = pyqtSignal(dict)
    connection_status = pyqtSignal(str, str)  # status, message
    
    def __init__(self, server_url):
        super().__init__()
        self.server_url = server_url
        self.running = True
        self.loop = None
        
    def run(self):
        """Runs the asyncio event loop in this thread."""
        # Create a new event loop for this thread
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self.connect_and_listen())
        finally:
            self.loop.close()
    
    async def connect_and_listen(self):
        """Connects to the WebSocket server and emits data signals."""
        while self.running:
            try:
                async with websockets.connect(self.server_url) as websocket:
                    self.connection_status.emit("connected", f"Connected to {self.server_url}")
                    
                    while self.running:
                        try:
                            # Use asyncio.wait_for to make recv() cancellable
                            message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                    try:
                        data = json.loads(message)
                                self.data_received.emit(data)
                    except json.JSONDecodeError:
                                self.connection_status.emit("warning", f"Non-JSON message: {message}")
                        except asyncio.TimeoutError:
                            # Timeout is fine, just check if we should continue
                            continue

        except websockets.exceptions.ConnectionClosed as e:
                if self.running:
                    self.connection_status.emit("disconnected", 
                        f"Connection lost: {e.code} - {e.reason}")
            await asyncio.sleep(5)
        except Exception as e:
                if self.running:
                    self.connection_status.emit("error", 
                        f"Error: {type(e).__name__} - {str(e)}")
            await asyncio.sleep(5)

    def stop(self):
        """Stop the WebSocket connection."""
        self.running = False
        if self.loop and self.loop.is_running():
            # Cancel all pending tasks
            for task in asyncio.all_tasks(self.loop):
                task.cancel()

# Import Polymarket client
try:
    from polymarket_client import PolymarketClient
    from config import config
    POLYMARKET_AVAILABLE = True
except ImportError:
    POLYMARKET_AVAILABLE = False
    print("Warning: Polymarket client not available.")


class PolymarketWorker(QObject):
    """Worker object for Polymarket operations in separate thread"""
    price_fetched = pyqtSignal(str, float)  # token_id, price
    error_occurred = pyqtSignal(str, str)  # operation, error
    
    def __init__(self, polymarket_client):
        super().__init__()
        self.client = polymarket_client
        self.running = True
        import queue
        self.request_queue = queue.Queue()
    
    def fetch_price(self, token_id: str):
        """Request price fetch for a token"""
        try:
            # Put request in queue (non-blocking)
            self.request_queue.put_nowait(('fetch_price', token_id))
        except:
            pass
    
    def process_requests(self):
        """Process requests in the worker thread"""
        while self.running:
            try:
                # Get request from queue (with timeout to allow checking self.running)
                try:
                    operation, token_id = self.request_queue.get(timeout=0.1)
                    if operation == 'fetch_price':
                        try:
                            price = self.client.get_market_price_display(token_id)
                            if price is None or price <= 0 or price >= 1:
                                price = self.client.get_midpoint_price(token_id)
                            if price and 0 < price < 1:
                                self.price_fetched.emit(token_id, price)
                        except Exception as e:
                            error_msg = f"{type(e).__name__}: {str(e)}"
                            self.error_occurred.emit('fetch_price', error_msg)
                            print(f"Error fetching price for {token_id[:20]}...: {error_msg}")
                except queue.Empty:
                    # Timeout or empty queue - continue loop
                    continue
            except Exception as e:
                self.error_occurred.emit('process', str(e))
    
    def stop(self):
        """Stop the worker"""
        self.running = False


class PolymarketThread(QThread):
    """Thread for Polymarket worker"""
    worker_ready = pyqtSignal(object)  # Signal when worker is created
    
    def __init__(self, polymarket_client):
        super().__init__()
        self.polymarket_client = polymarket_client
        self.worker = None
    
    def run(self):
        """Run the worker's process loop"""
        # Create worker in this thread (not in main thread)
        self.worker = PolymarketWorker(self.polymarket_client)
        # Emit signal that worker is ready
        self.worker_ready.emit(self.worker)
        self.worker.process_requests()
    
    def stop(self):
        """Stop the worker and thread"""
        if self.worker:
            self.worker.stop()
        self.quit()
        self.wait(5000)


class PriceDisplayWindow(QMainWindow):
    """Main window for displaying real-time Polymarket prices."""
    
    def __init__(self):
        super().__init__()
        self.token_id_up = None
        self.token_id_down = None
        self.polymarket_client = None
        self.polymarket_thread = None
        self.polymarket_worker = None
        self.market_url = None
        self.current_hour = None
        self.ws_thread = None
        self.last_ws_data = None  # Store last WebSocket data
        self.cached_prices = {}  # Cache for prices: {token_id: price}
        
        # Initialize Polymarket client if available
        if POLYMARKET_AVAILABLE and config.private_key:
            try:
                self.polymarket_client = PolymarketClient(
                    host=config.polymarket_host,
                    private_key=config.private_key,
                    chain_id=config.chain_id,
                    api_key=config.api_key,
                    api_secret=config.api_secret,
                    api_passphrase=config.api_passphrase,
                    simulation_mode=True
                )
                # Start Polymarket worker thread
                self.polymarket_thread = PolymarketThread(self.polymarket_client)
                # Connect signals when worker is ready (created in thread's run())
                self.polymarket_thread.worker_ready.connect(self._connect_polymarket_signals)
                self.polymarket_thread.start()
            except Exception as e:
                print(f"Warning: Could not initialize Polymarket client: {e}")
        
        self.init_ui()
        
        # Timer to update prices every 1 second
        self.price_timer = QTimer()
        self.price_timer.timeout.connect(self.update_prices)
        self.price_timer.start(1000)  # 1 second
        
        # Timer to check for hourly market changes every minute
        self.market_check_timer = QTimer()
        self.market_check_timer.timeout.connect(self.check_and_update_market)
        self.market_check_timer.start(60000)  # Check every 1 minute
        
        # Auto-fetch market on startup
        QTimer.singleShot(500, self.auto_fetch_market)
        
        # Start WebSocket connection
        self.start_websocket()
    
    def init_ui(self):
        """Initialize the user interface."""
        self.setWindowTitle("Polymarket Real-Time Price Display")
        self.setGeometry(100, 100, 600, 500)
        
        # Central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)
        layout.setSpacing(15)
        layout.setContentsMargins(20, 20, 20, 20)
        
        # Title
        title = QLabel("Polymarket Real-Time Prices")
        title_font = QFont()
        title_font.setPointSize(18)
        title_font.setBold(True)
        title.setFont(title_font)
        title.setAlignment(Qt.AlignCenter)
        layout.addWidget(title)
        
        # Market URL section
        url_layout = QVBoxLayout()
        url_label = QLabel("Market URL (Auto-Generated):")
        url_label.setStyleSheet("font-weight: bold;")
        url_layout.addWidget(url_label)
        
        url_input_layout = QHBoxLayout()
        self.url_input = QLineEdit()
        self.url_input.setPlaceholderText("Auto-generating market URL...")
        self.url_input.setReadOnly(True)
        self.url_input.setStyleSheet("background-color: #f0f0f0;")
        url_input_layout.addWidget(self.url_input)
        
        self.fetch_btn = QPushButton("Refresh Market")
        self.fetch_btn.clicked.connect(self.auto_fetch_market)
        url_input_layout.addWidget(self.fetch_btn)
        url_layout.addLayout(url_input_layout)
        layout.addLayout(url_layout)
        
        # Status
        self.status_label = QLabel("Enter market URL and click 'Fetch Token IDs'")
        self.status_label.setAlignment(Qt.AlignCenter)
        self.status_label.setStyleSheet("color: #666; font-size: 10px; padding: 5px;")
        layout.addWidget(self.status_label)
        
        # Separator
        separator = QLabel("─" * 50)
        separator.setAlignment(Qt.AlignCenter)
        separator.setStyleSheet("color: #ccc;")
        layout.addWidget(separator)
        
        # Token IDs display
        token_layout = QVBoxLayout()
        token_title = QLabel("Token IDs")
        token_title.setAlignment(Qt.AlignCenter)
        token_title.setStyleSheet("font-weight: bold; font-size: 12px;")
        token_layout.addWidget(token_title)
        
        self.token_up_label = QLabel("UP Token ID: Not set")
        self.token_up_label.setStyleSheet("color: #4CAF50; font-size: 10px; padding: 5px; background-color: #E8F5E9; border-radius: 5px;")
        self.token_up_label.setWordWrap(True)
        token_layout.addWidget(self.token_up_label)
        
        self.token_down_label = QLabel("DOWN Token ID: Not set")
        self.token_down_label.setStyleSheet("color: #F44336; font-size: 10px; padding: 5px; background-color: #FFEBEE; border-radius: 5px;")
        self.token_down_label.setWordWrap(True)
        token_layout.addWidget(self.token_down_label)
        layout.addLayout(token_layout)
        
        # Separator
        separator2 = QLabel("─" * 50)
        separator2.setAlignment(Qt.AlignCenter)
        separator2.setStyleSheet("color: #ccc;")
        layout.addWidget(separator2)
        
        # Price Comparison section
        comparison_title = QLabel("Price Comparison: Your Software vs Polymarket")
        comparison_title.setAlignment(Qt.AlignCenter)
        comparison_title.setStyleSheet("font-weight: bold; font-size: 14px; color: #333;")
        layout.addWidget(comparison_title)
        
        # UP price comparison
        up_comp_layout = QVBoxLayout()
        up_comp_title = QLabel("UP Price")
        up_comp_title.setAlignment(Qt.AlignCenter)
        up_comp_title.setStyleSheet("color: #666; font-size: 12px; font-weight: bold;")
        up_comp_layout.addWidget(up_comp_title)
        
        up_prices_layout = QHBoxLayout()
        
        # Your software UP price
        ws_up_layout = QVBoxLayout()
        ws_up_label_title = QLabel("Your Software")
        ws_up_label_title.setAlignment(Qt.AlignCenter)
        ws_up_label_title.setStyleSheet("color: #2196F3; font-size: 10px;")
        ws_up_layout.addWidget(ws_up_label_title)
        self.ws_price_up_label = QLabel("$0.0000")
        ws_up_font = QFont()
        ws_up_font.setPointSize(18)
        ws_up_font.setBold(True)
        self.ws_price_up_label.setFont(ws_up_font)
        self.ws_price_up_label.setAlignment(Qt.AlignCenter)
        self.ws_price_up_label.setStyleSheet("color: #2196F3; padding: 10px; background-color: #E3F2FD; border-radius: 8px;")
        ws_up_layout.addWidget(self.ws_price_up_label)
        up_prices_layout.addLayout(ws_up_layout)
        
        # Polymarket UP price
        poly_up_layout = QVBoxLayout()
        poly_up_label_title = QLabel("Polymarket")
        poly_up_label_title.setAlignment(Qt.AlignCenter)
        poly_up_label_title.setStyleSheet("color: #4CAF50; font-size: 10px;")
        poly_up_layout.addWidget(poly_up_label_title)
        self.price_up_label = QLabel("$0.0000")
        price_font = QFont()
        price_font.setPointSize(18)
        price_font.setBold(True)
        self.price_up_label.setFont(price_font)
        self.price_up_label.setAlignment(Qt.AlignCenter)
        self.price_up_label.setStyleSheet("color: #4CAF50; padding: 10px; background-color: #E8F5E9; border-radius: 8px;")
        poly_up_layout.addWidget(self.price_up_label)
        up_prices_layout.addLayout(poly_up_layout)
        
        # UP Difference
        diff_up_layout = QVBoxLayout()
        diff_up_label_title = QLabel("Difference")
        diff_up_label_title.setAlignment(Qt.AlignCenter)
        diff_up_label_title.setStyleSheet("color: #666; font-size: 10px;")
        diff_up_layout.addWidget(diff_up_label_title)
        self.diff_up_label = QLabel("$0.0000")
        self.diff_up_label.setFont(price_font)
        self.diff_up_label.setAlignment(Qt.AlignCenter)
        self.diff_up_label.setStyleSheet("color: #666; padding: 10px; background-color: #F5F5F5; border-radius: 8px;")
        diff_up_layout.addWidget(self.diff_up_label)
        up_prices_layout.addLayout(diff_up_layout)
        
        up_comp_layout.addLayout(up_prices_layout)
        layout.addLayout(up_comp_layout)
        
        # DOWN price comparison
        down_comp_layout = QVBoxLayout()
        down_comp_title = QLabel("DOWN Price")
        down_comp_title.setAlignment(Qt.AlignCenter)
        down_comp_title.setStyleSheet("color: #666; font-size: 12px; font-weight: bold;")
        down_comp_layout.addWidget(down_comp_title)
        
        down_prices_layout = QHBoxLayout()
        
        # Your software DOWN price
        ws_down_layout = QVBoxLayout()
        ws_down_label_title = QLabel("Your Software")
        ws_down_label_title.setAlignment(Qt.AlignCenter)
        ws_down_label_title.setStyleSheet("color: #2196F3; font-size: 10px;")
        ws_down_layout.addWidget(ws_down_label_title)
        self.ws_price_down_label = QLabel("$0.0000")
        self.ws_price_down_label.setFont(ws_up_font)
        self.ws_price_down_label.setAlignment(Qt.AlignCenter)
        self.ws_price_down_label.setStyleSheet("color: #2196F3; padding: 10px; background-color: #E3F2FD; border-radius: 8px;")
        ws_down_layout.addWidget(self.ws_price_down_label)
        down_prices_layout.addLayout(ws_down_layout)
        
        # Polymarket DOWN price
        poly_down_layout = QVBoxLayout()
        poly_down_label_title = QLabel("Polymarket")
        poly_down_label_title.setAlignment(Qt.AlignCenter)
        poly_down_label_title.setStyleSheet("color: #F44336; font-size: 10px;")
        poly_down_layout.addWidget(poly_down_label_title)
        self.price_down_label = QLabel("$0.0000")
        self.price_down_label.setFont(price_font)
        self.price_down_label.setAlignment(Qt.AlignCenter)
        self.price_down_label.setStyleSheet("color: #F44336; padding: 10px; background-color: #FFEBEE; border-radius: 8px;")
        poly_down_layout.addWidget(self.price_down_label)
        down_prices_layout.addLayout(poly_down_layout)
        
        # DOWN Difference
        diff_down_layout = QVBoxLayout()
        diff_down_label_title = QLabel("Difference")
        diff_down_label_title.setAlignment(Qt.AlignCenter)
        diff_down_label_title.setStyleSheet("color: #666; font-size: 10px;")
        diff_down_layout.addWidget(diff_down_label_title)
        self.diff_down_label = QLabel("$0.0000")
        self.diff_down_label.setFont(price_font)
        self.diff_down_label.setAlignment(Qt.AlignCenter)
        self.diff_down_label.setStyleSheet("color: #666; padding: 10px; background-color: #F5F5F5; border-radius: 8px;")
        diff_down_layout.addWidget(self.diff_down_label)
        down_prices_layout.addLayout(diff_down_layout)
        
        down_comp_layout.addLayout(down_prices_layout)
        layout.addLayout(down_comp_layout)
        
        # WebSocket connection status
        self.ws_status_label = QLabel("WebSocket: Connecting...")
        self.ws_status_label.setAlignment(Qt.AlignCenter)
        self.ws_status_label.setStyleSheet("color: #666; font-size: 10px; padding: 5px;")
        layout.addWidget(self.ws_status_label)
        
        # Last update
        self.last_update_label = QLabel("Last update: --")
        self.last_update_label.setAlignment(Qt.AlignCenter)
        self.last_update_label.setStyleSheet("color: #999; font-size: 10px;")
        layout.addWidget(self.last_update_label)
        
        layout.addStretch()
        
        # Set window style
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f5f5f5;
            }
            QPushButton {
                background-color: #2196F3;
                color: white;
                border: none;
                padding: 8px 15px;
                border-radius: 5px;
                font-size: 12px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #1976D2;
            }
            QPushButton:pressed {
                background-color: #0D47A1;
            }
        """)
    
    def get_et_time(self):
        """Get current time in Eastern Time (ET) timezone."""
        try:
            # Get ET timezone
            et_tz = pytz.timezone('US/Eastern')
            now_et = datetime.now(et_tz)
            return now_et
        except:
            # Fallback: assume UTC-5 for ET (EST) or UTC-4 for EDT
            # This is approximate, but better than local time
            from datetime import timedelta
            utc_now = datetime.utcnow()
            # Try to determine if DST (rough approximation: March-November)
            if 3 <= utc_now.month <= 11:
                et_offset = timedelta(hours=-4)  # EDT (UTC-4)
            else:
                et_offset = timedelta(hours=-5)  # EST (UTC-5)
            now_et = utc_now + et_offset
            return now_et
    
    def generate_market_url(self):
        """Generate market URL based on current date and time in ET."""
        now_et = self.get_et_time()
        
        # Format: bitcoin-up-or-down-november-18-11am-et
        month_name = now_et.strftime('%B').lower()  # november
        day = now_et.strftime('%d').lstrip('0')  # 18
        hour = now_et.hour
        
        # Convert hour to 12-hour format with am/pm
        if hour == 0:
            time_str = "12am"
        elif hour < 12:
            time_str = f"{hour}am"
        elif hour == 12:
            time_str = "12pm"
        else:
            time_str = f"{hour-12}pm"
        
        # Construct slug
        slug = f"bitcoin-up-or-down-{month_name}-{day}-{time_str}-et"
        url = f"https://polymarket.com/event/{slug}"
        
        return url, slug
    
    def auto_fetch_market(self):
        """Automatically find and fetch the current active market."""
        self.status_label.setText("🔄 Auto-generating market URL...")
        self.status_label.setStyleSheet("color: #FF9800; font-size: 10px;")
        
        try:
            # Generate URL based on current time
            url, slug = self.generate_market_url()
            self.url_input.setText(url)
            self.market_url = url
            
            # Update current hour (in ET)
            now_et = self.get_et_time()
            self.current_hour = now_et.hour
            
            # Try to find the market
            self.find_and_fetch_market(slug)
            
        except Exception as e:
            self.status_label.setText(f"❌ Error: {str(e)[:50]}")
            self.status_label.setStyleSheet("color: #F44336; font-size: 10px;")
    
    def check_and_update_market(self):
        """Check if market hour has changed and update if needed."""
        now_et = self.get_et_time()
        current_hour = now_et.hour
        if current_hour != self.current_hour:
            self.status_label.setText("🔄 Hour changed - updating market...")
            self.status_label.setStyleSheet("color: #FF9800; font-size: 10px;")
            self.auto_fetch_market()
    
    def find_and_fetch_market(self, slug):
        """Find market by slug and fetch token IDs."""
        self.status_label.setText("🔄 Searching for market...")
        self.status_label.setStyleSheet("color: #FF9800; font-size: 10px;")
        
        try:
            # Query Gamma Markets API
            gamma_url = "https://gamma-api.polymarket.com/markets"
            params = {"slug": slug}
            response = requests.get(gamma_url, params=params, timeout=10)
            
            if response.status_code != 200:
                # Try searching active markets
                self.search_active_markets()
                return
            
            data = response.json()
            
            # Parse response
            if isinstance(data, list) and len(data) > 0:
                market = data[0]
            elif isinstance(data, dict) and 'data' in data and len(data['data']) > 0:
                market = data['data'][0]
            elif isinstance(data, dict) and 'results' in data and len(data['results']) > 0:
                market = data['results'][0]
            else:
                market = data if isinstance(data, dict) else None
            
            if market:
                self.extract_token_ids_from_market(market)
            else:
                # Try searching active markets
                self.search_active_markets()
                
        except Exception as e:
            self.status_label.setText(f"⚠️ Error: {str(e)[:50]}, trying active markets...")
            self.search_active_markets()
    
    def search_active_markets(self):
        """Search active markets for Bitcoin Up or Down."""
        try:
            now_et = self.get_et_time()
            month_name = now_et.strftime('%B').lower()
            day = now_et.strftime('%d').lstrip('0')
            hour = now_et.hour
            
            if hour == 0:
                time_str = "12am"
            elif hour < 12:
                time_str = f"{hour}am"
            elif hour == 12:
                time_str = "12pm"
            else:
                time_str = f"{hour-12}pm"
            
            gamma_url = "https://gamma-api.polymarket.com/markets"
            params = {"active": "true", "limit": 50, "closed": "false"}
            response = requests.get(gamma_url, params=params, timeout=10)
            
            if response.status_code != 200:
                self.status_label.setText(f"❌ API Error: {response.status_code}")
                self.status_label.setStyleSheet("color: #F44336; font-size: 10px;")
                return
            
            markets = response.json()
            if not isinstance(markets, list):
                if isinstance(markets, dict) and 'data' in markets:
                    markets = markets['data']
                elif isinstance(markets, dict) and 'results' in markets:
                    markets = markets['results']
                else:
                    markets = []
            
            best_match = None
            best_score = 0
            
            for market in markets:
                question = market.get('question', '').lower()
                slug = market.get('slug', '')
                
                score = 0
                
                # Check if it's Bitcoin
                if 'bitcoin' in question or 'btc' in question:
                    score += 10
                
                # Check if it's "up or down"
                if 'up' in question and 'down' in question:
                    score += 10
                
                # Check time match
                if str(hour) in question or str(hour) in slug or time_str in slug:
                    score += 10
                
                # Check date match
                if month_name in question and day in question:
                    score += 10
                
                # Prefer markets with token IDs
                if market.get('clobTokenIds'):
                    score += 5
                
                if score > best_score:
                    best_score = score
                    best_match = market
            
            if best_match and best_score >= 15:
                slug = best_match.get('slug', '')
                if slug:
                    url = f"https://polymarket.com/event/{slug}"
                    self.url_input.setText(url)
                    self.market_url = url
                    self.extract_token_ids_from_market(best_match)
                else:
                    self.status_label.setText("⚠️ Market found but no slug")
                    self.status_label.setStyleSheet("color: #FF9800; font-size: 10px;")
            else:
                self.status_label.setText("⚠️ No active market found. Try manual URL.")
                self.status_label.setStyleSheet("color: #FF9800; font-size: 10px;")
                
        except Exception as e:
            self.status_label.setText(f"❌ Error searching markets: {str(e)[:50]}")
            self.status_label.setStyleSheet("color: #F44336; font-size: 10px;")
    
    def extract_token_ids_from_market(self, market):
        """Extract token IDs from market data."""
        try:
            # Extract token IDs
            clob_token_ids = market.get('clobTokenIds', '')
            outcomes_str = market.get('outcomes', '')
            
            # Parse token IDs
            token_ids_list = []
            if clob_token_ids:
                if isinstance(clob_token_ids, str):
                    try:
                        parsed = json.loads(clob_token_ids.strip())
                        if isinstance(parsed, list):
                            token_ids_list = [str(tid).strip() for tid in parsed if str(tid).strip().isdigit() and len(str(tid).strip()) > 20]
                    except:
                        parts = clob_token_ids.split(',')
                        for part in parts:
                            cleaned = part.strip().strip('[]').strip('"').strip("'")
                            if cleaned.isdigit() and len(cleaned) > 20:
                                token_ids_list.append(cleaned)
                elif isinstance(clob_token_ids, list):
                    token_ids_list = [str(tid).strip() for tid in clob_token_ids if str(tid).strip().isdigit() and len(str(tid).strip()) > 20]
            
            # Parse outcomes
            outcome_names = []
            if outcomes_str:
                try:
                    if isinstance(outcomes_str, str):
                        outcomes_data = json.loads(outcomes_str)
                    else:
                        outcomes_data = outcomes_str
                    if isinstance(outcomes_data, list):
                        outcome_names = [str(o) for o in outcomes_data]
                except:
                    if isinstance(outcomes_str, list):
                        outcome_names = [str(o) for o in outcomes_str]
                    else:
                        outcome_names = [o.strip() for o in str(outcomes_str).split(',') if o.strip()]
            
            # Default outcome names
            if not outcome_names:
                question = market.get('question', '').lower()
                if 'up' in question and 'down' in question:
                    outcome_names = ['Up', 'Down']
                elif 'yes' in question and 'no' in question:
                    outcome_names = ['Yes', 'No']
                else:
                    outcome_names = ['Outcome 1', 'Outcome 2']
            
            # Map token IDs to outcomes
            if len(token_ids_list) >= 2:
                # Determine which is UP and which is DOWN
                if 'up' in outcome_names[0].lower() or 'yes' in outcome_names[0].lower():
                    self.token_id_up = token_ids_list[0]
                    self.token_id_down = token_ids_list[1]
                else:
                    self.token_id_up = token_ids_list[1]
                    self.token_id_down = token_ids_list[0]
                
                self.token_up_label.setText(f"UP Token ID: {self.token_id_up}")
                self.token_down_label.setText(f"DOWN Token ID: {self.token_id_down}")
                
                # Clear cached prices when token IDs change
                self.cached_prices = {}
                
                # Immediately request prices
                if self.polymarket_worker:
                    self.polymarket_worker.fetch_price(self.token_id_up)
                    if self.token_id_down:
                        self.polymarket_worker.fetch_price(self.token_id_down)
                elif self.polymarket_client:
                    # Fallback: fetch directly
                    try:
                        price_up = self.polymarket_client.get_market_price_display(self.token_id_up)
                        if price_up and 0 < price_up < 1:
                            self.cached_prices[self.token_id_up] = price_up
                        if self.token_id_down:
                            price_down = self.polymarket_client.get_market_price_display(self.token_id_down)
                            if price_down and 0 < price_down < 1:
                                self.cached_prices[self.token_id_down] = price_down
                    except Exception as e:
                        print(f"Error fetching initial prices: {e}")
                
                self.status_label.setText("✅ Token IDs fetched! Prices updating...")
                self.status_label.setStyleSheet("color: #4CAF50; font-size: 10px;")
            else:
                self.status_label.setText("⚠️ Could not extract token IDs from market")
                self.status_label.setStyleSheet("color: #FF9800; font-size: 10px;")
                self.token_id_up = None
                self.token_id_down = None
                
        except Exception as e:
            self.status_label.setText(f"❌ Error: {str(e)[:50]}")
            self.status_label.setStyleSheet("color: #F44336; font-size: 10px;")
            self.token_id_up = None
            self.token_id_down = None
    
    def start_websocket(self):
        """Start the WebSocket connection in a separate thread."""
        self.ws_thread = WebSocketThread(SERVER_URL)
        self.ws_thread.data_received.connect(self.on_websocket_data)
        self.ws_thread.connection_status.connect(self.on_websocket_status)
        self.ws_thread.start()
    
    def on_websocket_data(self, data):
        """Handle data received from WebSocket."""
        self.last_ws_data = data
        # Prices will be updated by the timer
    
    def on_websocket_status(self, status, message):
        """Handle WebSocket connection status updates."""
        if status == "connected":
            self.ws_status_label.setText(f"✅ {message}")
            self.ws_status_label.setStyleSheet("color: #4CAF50; font-size: 10px; padding: 5px;")
        elif status == "disconnected":
            self.ws_status_label.setText(f"❌ {message}")
            self.ws_status_label.setStyleSheet("color: #F44336; font-size: 10px; padding: 5px;")
        elif status == "error":
            self.ws_status_label.setText(f"⚠️ {message}")
            self.ws_status_label.setStyleSheet("color: #FF9800; font-size: 10px; padding: 5px;")
        else:
            self.ws_status_label.setText(message)
            self.ws_status_label.setStyleSheet("color: #666; font-size: 10px; padding: 5px;")
    
    def on_price_fetched(self, token_id: str, price: float):
        """Handle price fetched from Polymarket worker"""
        if price and 0 < price < 1:
            self.cached_prices[token_id] = price
            # Force immediate UI update
            self.update_price_display()
    
    def on_polymarket_error(self, operation: str, error: str):
        """Handle errors from Polymarket worker"""
        # Log error to status label
        error_msg = f"⚠️ {operation}: {str(error)[:50]}"
        self.status_label.setText(error_msg)
        self.status_label.setStyleSheet("color: #FF9800; font-size: 10px;")
        print(f"Polymarket error: {operation} - {error}")
    
    def _connect_polymarket_signals(self, worker):
        """Connect signals after worker is created in thread"""
        self.polymarket_worker = worker
        self.polymarket_worker.price_fetched.connect(self.on_price_fetched)
        self.polymarket_worker.error_occurred.connect(self.on_polymarket_error)
    
    def update_prices(self):
        """Request price updates from Polymarket worker thread."""
        # Request prices from worker thread (non-blocking)
        if self.polymarket_worker and self.token_id_up:
            try:
                self.polymarket_worker.fetch_price(self.token_id_up)
            except Exception as e:
                print(f"Error requesting UP price: {e}")
        
        if self.polymarket_worker and self.token_id_down:
            try:
                self.polymarket_worker.fetch_price(self.token_id_down)
            except Exception as e:
                print(f"Error requesting DOWN price: {e}")
        
        # Fallback: fetch directly if worker not available or prices are stale
        # Check if we need to fetch directly (worker not ready or no cached prices for tokens)
        needs_direct_fetch = False
        if not self.polymarket_worker:
            needs_direct_fetch = True
        elif self.token_id_up and self.token_id_up not in self.cached_prices:
            needs_direct_fetch = True
        elif self.token_id_down and self.token_id_down not in self.cached_prices:
            needs_direct_fetch = True
        
        if needs_direct_fetch and self.polymarket_client:
            try:
                if self.token_id_up:
                    price_up = self.polymarket_client.get_market_price_display(self.token_id_up)
                    if price_up and 0 < price_up < 1:
                        self.cached_prices[self.token_id_up] = price_up
                        print(f"✅ Fetched UP price directly: ${price_up:.4f}")
                
                if self.token_id_down:
                    price_down = self.polymarket_client.get_market_price_display(self.token_id_down)
                    if price_down and 0 < price_down < 1:
                        self.cached_prices[self.token_id_down] = price_down
                        print(f"✅ Fetched DOWN price directly: ${price_down:.4f}")
            except Exception as e:
                print(f"Error fetching prices directly: {e}")
        
        # Update display with cached prices
        self.update_price_display()
    
    def update_price_display(self):
        """Update the price display with cached and WebSocket data"""
        try:
            # Get cached Polymarket prices
            price_up_poly = self.cached_prices.get(self.token_id_up) if self.token_id_up else None
            price_down_poly = self.cached_prices.get(self.token_id_down) if self.token_id_down else None
            
            # If DOWN price not available, calculate from UP
            if price_up_poly and not price_down_poly and 0 < price_up_poly < 1:
                price_down_poly = 1.0 - price_up_poly
            
            # Debug: print if no prices
            if not price_up_poly and self.token_id_up:
                print(f"Warning: No price for UP token {self.token_id_up[:20]}...")
            if not price_down_poly and self.token_id_down:
                print(f"Warning: No price for DOWN token {self.token_id_down[:20]}...")
            
            # Get WebSocket prices (from your custom software)
            price_up_ws = None
            price_down_ws = None
            
            if self.last_ws_data:
                prob_up = self.last_ws_data.get('prob_up', 0)
                prob_down = self.last_ws_data.get('prob_down', 0)
                # Convert probabilities to prices (prob_up is percentage, so divide by 100)
                if prob_up > 0:
                    price_up_ws = prob_up / 100.0
                if prob_down > 0:
                    price_down_ws = prob_down / 100.0
            
            # Update Polymarket prices display
            if price_up_poly and 0 < price_up_poly < 1:
                self.price_up_label.setText(f"${price_up_poly:.4f}")
            else:
                self.price_up_label.setText("N/A")
            
            if price_down_poly and 0 < price_down_poly < 1:
                self.price_down_label.setText(f"${price_down_poly:.4f}")
            else:
                self.price_down_label.setText("N/A")
            
            # Update WebSocket prices display
            if price_up_ws and 0 < price_up_ws < 1:
                self.ws_price_up_label.setText(f"${price_up_ws:.4f}")
            else:
                self.ws_price_up_label.setText("N/A")
            
            if price_down_ws and 0 < price_down_ws < 1:
                self.ws_price_down_label.setText(f"${price_down_ws:.4f}")
            else:
                self.ws_price_down_label.setText("N/A")
            
            # Calculate and display differences
            if price_up_ws and price_up_poly and 0 < price_up_ws < 1 and 0 < price_up_poly < 1:
                diff_up = price_up_ws - price_up_poly
                color = "#4CAF50" if abs(diff_up) < 0.01 else "#FF9800" if diff_up > 0 else "#F44336"
                sign = "+" if diff_up >= 0 else ""
                self.diff_up_label.setText(f"{sign}${diff_up:.4f}")
                self.diff_up_label.setStyleSheet(f"color: {color}; padding: 10px; background-color: #F5F5F5; border-radius: 8px; font-weight: bold;")
            else:
                self.diff_up_label.setText("N/A")
                self.diff_up_label.setStyleSheet("color: #666; padding: 10px; background-color: #F5F5F5; border-radius: 8px;")
            
            if price_down_ws and price_down_poly and 0 < price_down_ws < 1 and 0 < price_down_poly < 1:
                diff_down = price_down_ws - price_down_poly
                color = "#4CAF50" if abs(diff_down) < 0.01 else "#FF9800" if diff_down > 0 else "#F44336"
                sign = "+" if diff_down >= 0 else ""
                self.diff_down_label.setText(f"{sign}${diff_down:.4f}")
                self.diff_down_label.setStyleSheet(f"color: {color}; padding: 10px; background-color: #F5F5F5; border-radius: 8px; font-weight: bold;")
            else:
                self.diff_down_label.setText("N/A")
                self.diff_down_label.setStyleSheet("color: #666; padding: 10px; background-color: #F5F5F5; border-radius: 8px;")
            
            # Update timestamp
            self.last_update_label.setText(f"Last update: {datetime.now().strftime('%H:%M:%S')}")
            
        except Exception as e:
            self.status_label.setText(f"⚠️ Error fetching prices: {str(e)[:50]}")
            self.status_label.setStyleSheet("color: #FF9800; font-size: 10px;")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = PriceDisplayWindow()
    window.show()
    
    # Cleanup on exit
    import atexit
    def cleanup():
        if window.ws_thread:
            window.ws_thread.stop()
            window.ws_thread.wait()
    atexit.register(cleanup)
    
    sys.exit(app.exec_())

