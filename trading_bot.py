"""
Polymarket Trading Bot with UI
Compares external software probabilities with Polymarket prices and executes trades
"""
import sys
import asyncio
import json
import time
import websockets
import requests
import re
from datetime import datetime
from typing import Optional, Dict
from dataclasses import dataclass

from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QLabel, QPushButton, QTextEdit, 
                             QLineEdit, QCheckBox, QSpinBox, QDoubleSpinBox,
                             QGroupBox, QGridLayout, QTabWidget, QTableWidget,
                             QTableWidgetItem, QHeaderView)
from PyQt5.QtCore import QThread, pyqtSignal, Qt, QTimer, QObject
from PyQt5.QtGui import QFont
import queue

from config import BotConfig, config
from strategy import MarketData, EntryStrategy, ExitStrategy, TradingStrategy, Position
from polymarket_client import PolymarketClient
from trading_engine import TradingEngine


class WebSocketThread(QThread):
    """Thread that handles WebSocket connection for external software probabilities"""
    data_received = pyqtSignal(dict)
    connection_status = pyqtSignal(str, str)  # status, message
    
    def __init__(self, server_url: str):
        super().__init__()
        self.server_url = server_url
        self.running = True
    
    def run(self):
        """Runs the asyncio event loop in this thread"""
        asyncio.run(self.connect_and_listen())
    
    async def connect_and_listen(self):
        """Connects to WebSocket and emits data signals"""
        while self.running:
            try:
                async with websockets.connect(self.server_url) as websocket:
                    self.connection_status.emit("connected", f"Connected to {self.server_url}")
                    
                    while self.running:
                        message = await websocket.recv()
                        try:
                            data = json.loads(message)
                            self.data_received.emit(data)
                        except json.JSONDecodeError:
                            self.connection_status.emit("warning", f"Non-JSON message received")
                            
            except websockets.exceptions.ConnectionClosed as e:
                self.connection_status.emit("disconnected", 
                    f"Connection lost: {e.code} - {e.reason}")
                await asyncio.sleep(5)
            except Exception as e:
                self.connection_status.emit("error", f"Error: {type(e).__name__} - {str(e)}")
                await asyncio.sleep(5)
    
    def stop(self):
        """Stop the WebSocket connection"""
        self.running = False


class PolymarketWorker(QObject):
    """Worker object for Polymarket operations in separate thread"""
    price_fetched = pyqtSignal(str, float)  # token_id, price
    order_placed = pyqtSignal(dict)  # order response
    error_occurred = pyqtSignal(str, str)  # operation, error message
    
    def __init__(self, polymarket_client):
        super().__init__()
        self.client = polymarket_client
        self.request_queue = queue.Queue()
        self.running = True
    
    def fetch_price(self, token_id: str, method: str = "display"):
        """Request price fetch (non-blocking)"""
        self.request_queue.put(("fetch_price", token_id, method))
    
    def place_order(self, token_id: str, side: str, price: float, size: float):
        """Request order placement (non-blocking)"""
        self.request_queue.put(("place_order", token_id, side, price, size))
    
    def process_requests(self):
        """Process requests from queue (runs in worker thread)"""
        while self.running:
            try:
                # Get request with timeout to allow checking self.running
                try:
                    request = self.request_queue.get(timeout=0.1)
                except queue.Empty:
                    continue
                
                operation = request[0]
                
                if operation == "fetch_price":
                    token_id, method = request[1], request[2]
                    try:
                        if method == "display":
                            price = self.client.get_market_price_display(token_id)
                        elif method == "midpoint":
                            price = self.client.get_midpoint_price(token_id)
                        else:
                            price = self.client.get_market_price(token_id, "BUY")
                        
                        if price and 0 < price < 1:
                            self.price_fetched.emit(token_id, price)
                    except Exception as e:
                        self.error_occurred.emit(f"fetch_price_{token_id}", str(e))
                
                elif operation == "place_order":
                    token_id, side, price, size = request[1], request[2], request[3], request[4]
                    try:
                        response = self.client.place_limit_order(
                            token_id=token_id,
                            side=side,
                            price=price,
                            size=size
                        )
                        self.order_placed.emit({
                            "token_id": token_id,
                            "side": side,
                            "price": price,
                            "size": size,
                            "response": response
                        })
                    except Exception as e:
                        self.error_occurred.emit(f"place_order_{token_id}", str(e))
                
                self.request_queue.task_done()
            except Exception as e:
                self.error_occurred.emit("process_requests", str(e))
    
    def stop(self):
        """Stop processing requests"""
        self.running = False


class PolymarketThread(QThread):
    """Thread for Polymarket operations"""
    
    def __init__(self, polymarket_client):
        super().__init__()
        self.worker = PolymarketWorker(polymarket_client)
        self.worker.moveToThread(self)
    
    def run(self):
        """Run worker in this thread"""
        self.worker.process_requests()
    
    def stop(self):
        """Stop the worker"""
        self.worker.stop()
        self.quit()
        self.wait(5000)


class TradingBotWindow(QMainWindow):
    """Main trading bot window with UI"""
    
    def __init__(self):
        super().__init__()
        self.config = config
        self.polymarket_client = None
        self.polymarket_thread = None
        self.polymarket_worker = None
        self.trading_engine = None
        self.ws_thread = None
        self.strategy = None
        self.pending_price_requests = {}  # Track pending price requests
        
        # Market data tracking
        self.last_market_data = None
        self.last_ws_data = None
        self.token_id_up = None
        self.token_id_down = None
        self.current_symbol = None
        self.current_interval = None
        self.current_market_url = None
        self.current_hour = None  # Track current hour for market switching
        
        self.init_ui()
        self.setup_timers()
        
        # Timer to check for market URL changes every minute (markets change hourly)
        self.url_check_timer = QTimer()
        self.url_check_timer.timeout.connect(self.check_and_update_market_url)
        self.url_check_timer.start(60000)  # Check every 1 minute
        
        # Timer to force regeneration every hour
        self.url_regeneration_timer = QTimer()
        self.url_regeneration_timer.timeout.connect(self.regenerate_market_url)
        self.url_regeneration_timer.start(3600000)  # 1 hour
        
        # Timer to continuously fetch prices every 1 second (will start when bot starts)
        self.price_fetch_timer = QTimer()
        self.price_fetch_timer.timeout.connect(self.fetch_prices_continuously)
        # Don't start it here - will start when bot starts
    
    def init_ui(self):
        """Initialize the user interface"""
        self.setWindowTitle("Polymarket Trading Bot")
        self.setGeometry(100, 100, 1000, 800)
        
        # Create tab widget
        tabs = QTabWidget()
        self.setCentralWidget(tabs)
        
        # Dashboard tab
        tabs.addTab(self.create_dashboard_tab(), "Dashboard")
        
        # Configuration tab
        tabs.addTab(self.create_config_tab(), "Configuration")
        
        # Positions tab
        tabs.addTab(self.create_positions_tab(), "Positions")
        
        # Logs tab
        tabs.addTab(self.create_logs_tab(), "Logs")
    
    def create_dashboard_tab(self):
        """Create the main dashboard tab"""
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        
        # Status section
        status_group = QGroupBox("Bot Status")
        status_layout = QVBoxLayout()
        
        self.bot_status_label = QLabel("Bot Stopped")
        self.bot_status_label.setStyleSheet("color: #F44336; font-size: 16px; font-weight: bold;")
        status_layout.addWidget(self.bot_status_label)
        
        self.ws_status_label = QLabel("WebSocket: Not Connected")
        self.ws_status_label.setStyleSheet("color: #666; font-size: 12px;")
        status_layout.addWidget(self.ws_status_label)
        
        # Control buttons
        btn_layout = QHBoxLayout()
        self.start_btn = QPushButton("Start Bot")
        self.start_btn.clicked.connect(self.start_bot)
        self.start_btn.setStyleSheet("background-color: #4CAF50; color: white; padding: 10px; font-weight: bold;")
        
        self.stop_btn = QPushButton("Stop Bot")
        self.stop_btn.clicked.connect(self.stop_bot)
        self.stop_btn.setEnabled(False)
        self.stop_btn.setStyleSheet("background-color: #F44336; color: white; padding: 10px; font-weight: bold;")
        
        btn_layout.addWidget(self.start_btn)
        btn_layout.addWidget(self.stop_btn)
        status_layout.addLayout(btn_layout)
        
        status_group.setLayout(status_layout)
        layout.addWidget(status_group)
        
        # Market URL section (auto-generated)
        url_group = QGroupBox("Market URL (Auto-Generated)")
        url_layout = QVBoxLayout()
        self.market_url_label = QLabel("Waiting for WebSocket data...")
        self.market_url_label.setWordWrap(True)
        self.market_url_label.setStyleSheet("color: #666; font-size: 10px; padding: 5px; background-color: #F5F5F5; border-radius: 5px;")
        url_layout.addWidget(self.market_url_label)
        url_group.setLayout(url_layout)
        layout.addWidget(url_group)
        
        # Market Data section
        market_group = QGroupBox("Market Data")
        market_layout = QGridLayout()
        
        # Interval and Period Info
        market_layout.addWidget(QLabel("Interval:"), 0, 0)
        self.interval_label = QLabel("N/A")
        self.interval_label.setStyleSheet("color: #666; font-weight: bold; font-size: 12px;")
        market_layout.addWidget(self.interval_label, 0, 1)
        
        market_layout.addWidget(QLabel("Period Open:"), 0, 2)
        self.period_open_label = QLabel("$0.0000")
        self.period_open_label.setStyleSheet("color: #FF9800; font-weight: bold; font-size: 12px;")
        market_layout.addWidget(self.period_open_label, 0, 3)
        
        # Live Price from Software
        market_layout.addWidget(QLabel("Live Price (Software):"), 1, 0)
        self.live_price_label = QLabel("$0.0000")
        self.live_price_label.setStyleSheet("color: #2196F3; font-weight: bold; font-size: 14px;")
        market_layout.addWidget(self.live_price_label, 1, 1)
        
        # Software probabilities
        market_layout.addWidget(QLabel("Software Prob. UP:"), 2, 0)
        self.software_prob_up_label = QLabel("0.0%")
        self.software_prob_up_label.setStyleSheet("color: #2196F3; font-weight: bold;")
        market_layout.addWidget(self.software_prob_up_label, 2, 1)
        
        market_layout.addWidget(QLabel("Software Prob. DOWN:"), 2, 2)
        self.software_prob_down_label = QLabel("0.0%")
        self.software_prob_down_label.setStyleSheet("color: #2196F3; font-weight: bold;")
        market_layout.addWidget(self.software_prob_down_label, 2, 3)
        
        # Software prices (converted from probabilities)
        market_layout.addWidget(QLabel("Software Price UP:"), 3, 0)
        self.software_price_up_label = QLabel("$0.0000")
        self.software_price_up_label.setStyleSheet("color: #2196F3; font-weight: bold;")
        market_layout.addWidget(self.software_price_up_label, 3, 1)
        
        market_layout.addWidget(QLabel("Software Price DOWN:"), 3, 2)
        self.software_price_down_label = QLabel("$0.0000")
        self.software_price_down_label.setStyleSheet("color: #2196F3; font-weight: bold;")
        market_layout.addWidget(self.software_price_down_label, 3, 3)
        
        # Polymarket prices
        market_layout.addWidget(QLabel("Polymarket Price UP:"), 4, 0)
        self.poly_price_up_label = QLabel("$0.0000")
        self.poly_price_up_label.setStyleSheet("color: #4CAF50; font-weight: bold;")
        market_layout.addWidget(self.poly_price_up_label, 4, 1)
        
        market_layout.addWidget(QLabel("Polymarket Price DOWN:"), 4, 2)
        self.poly_price_down_label = QLabel("$0.0000")
        self.poly_price_down_label.setStyleSheet("color: #F44336; font-weight: bold;")
        market_layout.addWidget(self.poly_price_down_label, 4, 3)
        
        # Price differences
        market_layout.addWidget(QLabel("Price Diff UP:"), 5, 0)
        self.diff_up_label = QLabel("$0.0000")
        self.diff_up_label.setStyleSheet("color: #666; font-weight: bold;")
        market_layout.addWidget(self.diff_up_label, 5, 1)
        
        market_layout.addWidget(QLabel("Price Diff DOWN:"), 5, 2)
        self.diff_down_label = QLabel("$0.0000")
        self.diff_down_label.setStyleSheet("color: #666; font-weight: bold;")
        market_layout.addWidget(self.diff_down_label, 5, 3)
        
        # Entry signal
        market_layout.addWidget(QLabel("Entry Signal:"), 6, 0)
        self.entry_signal_label = QLabel("NO SIGNAL")
        self.entry_signal_label.setStyleSheet("color: #999; font-size: 14px; font-weight: bold;")
        market_layout.addWidget(self.entry_signal_label, 6, 1, 1, 3)
        
        market_group.setLayout(market_layout)
        layout.addWidget(market_group)
        
        # Statistics
        stats_group = QGroupBox("Statistics")
        stats_layout = QGridLayout()
        
        stats_layout.addWidget(QLabel("Open Positions:"), 0, 0)
        self.positions_count_label = QLabel("0")
        stats_layout.addWidget(self.positions_count_label, 0, 1)
        
        stats_layout.addWidget(QLabel("Total P&L:"), 0, 2)
        self.total_pnl_label = QLabel("$0.00")
        stats_layout.addWidget(self.total_pnl_label, 0, 3)
        
        stats_layout.addWidget(QLabel("Trades Today:"), 1, 0)
        self.trades_count_label = QLabel("0")
        stats_layout.addWidget(self.trades_count_label, 1, 1)
        
        stats_layout.addWidget(QLabel("Win Rate:"), 1, 2)
        self.win_rate_label = QLabel("0%")
        stats_layout.addWidget(self.win_rate_label, 1, 3)
        
        stats_group.setLayout(stats_layout)
        layout.addWidget(stats_group)
        
        layout.addStretch()
        return widget
    
    def create_config_tab(self):
        """Create configuration tab"""
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        
        # WebSocket config
        ws_group = QGroupBox("WebSocket Configuration")
        ws_layout = QGridLayout()
        ws_layout.addWidget(QLabel("WebSocket URL:"), 0, 0)
        self.ws_url_input = QLineEdit(self.config.websocket_url)
        ws_layout.addWidget(self.ws_url_input, 0, 1)
        ws_group.setLayout(ws_layout)
        layout.addWidget(ws_group)
        
        # Polymarket config
        poly_group = QGroupBox("Polymarket Configuration")
        poly_layout = QVBoxLayout()
        
        # API Host
        host_layout = QGridLayout()
        host_layout.addWidget(QLabel("API Host:"), 0, 0)
        self.poly_host_input = QLineEdit(self.config.polymarket_host)
        host_layout.addWidget(self.poly_host_input, 0, 1)
        poly_layout.addLayout(host_layout)
        
        # Market URL Finder
        url_group = QGroupBox("Find Token IDs from Market URL")
        url_layout = QVBoxLayout()
        url_input_layout = QHBoxLayout()
        url_input_layout.addWidget(QLabel("Market URL:"))
        self.market_url_input = QLineEdit()
        self.market_url_input.setPlaceholderText("https://polymarket.com/event/...")
        url_input_layout.addWidget(self.market_url_input)
        find_token_btn = QPushButton("Find Token IDs")
        find_token_btn.clicked.connect(self.find_token_ids_from_url)
        find_token_btn.setStyleSheet("background-color: #2196F3; color: white; padding: 5px;")
        url_input_layout.addWidget(find_token_btn)
        url_layout.addLayout(url_input_layout)
        
        self.finder_status_label = QLabel("")
        self.finder_status_label.setWordWrap(True)
        self.finder_status_label.setStyleSheet("color: #666; font-size: 10px; padding: 5px;")
        url_layout.addWidget(self.finder_status_label)
        url_group.setLayout(url_layout)
        poly_layout.addWidget(url_group)
        
        # Token ID inputs
        token_layout = QGridLayout()
        token_layout.addWidget(QLabel("Token ID (UP/YES):"), 0, 0)
        self.token_id_up_input = QLineEdit(self.config.token_id_up or self.config.token_id or "")
        self.token_id_up_input.setPlaceholderText("Enter UP token ID or use URL finder above...")
        self.token_id_up_input.setStyleSheet("padding: 5px;")
        token_layout.addWidget(self.token_id_up_input, 0, 1)
        
        token_layout.addWidget(QLabel("Token ID (DOWN/NO):"), 1, 0)
        self.token_id_down_input = QLineEdit(self.config.token_id_down or "")
        self.token_id_down_input.setPlaceholderText("Enter DOWN token ID or use URL finder above...")
        self.token_id_down_input.setStyleSheet("padding: 5px;")
        token_layout.addWidget(self.token_id_down_input, 1, 1)
        
        token_layout.addWidget(QLabel("Chain ID:"), 2, 0)
        self.chain_id_input = QSpinBox()
        self.chain_id_input.setRange(1, 999999)
        self.chain_id_input.setValue(self.config.chain_id)
        token_layout.addWidget(self.chain_id_input, 2, 1)
        poly_layout.addLayout(token_layout)
        
        poly_group.setLayout(poly_layout)
        layout.addWidget(poly_group)
        
        # Entry Strategy config
        entry_group = QGroupBox("Entry Strategy (Auto-Buy/Sell)")
        entry_layout = QGridLayout()
        
        # Auto-trade toggle
        self.auto_trade_check = QCheckBox("Enable Auto-Trading")
        self.auto_trade_check.setChecked(True)  # Enabled by default
        self.auto_trade_check.setStyleSheet("font-weight: bold; color: #4CAF50; font-size: 12px;")
        entry_layout.addWidget(self.auto_trade_check, 0, 0, 1, 2)
        
        entry_layout.addWidget(QLabel("Entry Threshold ($):"), 1, 0)
        self.entry_threshold_input = QDoubleSpinBox()
        self.entry_threshold_input.setRange(0.01, 1.0)
        self.entry_threshold_input.setSingleStep(0.01)
        self.entry_threshold_input.setValue(self.config.entry_threshold)
        self.entry_threshold_input.setDecimals(2)
        entry_layout.addWidget(self.entry_threshold_input, 1, 1)
        
        entry_layout.addWidget(QLabel("Max Position Size ($):"), 2, 0)
        self.max_size_input = QDoubleSpinBox()
        self.max_size_input.setRange(1.0, 10000.0)
        self.max_size_input.setValue(self.config.max_position_size)
        entry_layout.addWidget(self.max_size_input, 2, 1)
        
        entry_layout.addWidget(QLabel("Min Position Size ($):"), 3, 0)
        self.min_size_input = QDoubleSpinBox()
        self.min_size_input.setRange(1.0, 1000.0)
        self.min_size_input.setValue(self.config.min_position_size)
        entry_layout.addWidget(self.min_size_input, 3, 1)
        
        entry_group.setLayout(entry_layout)
        layout.addWidget(entry_group)
        
        # Exit Strategy config
        exit_group = QGroupBox("Exit Strategy")
        exit_layout = QGridLayout()
        
        self.exit_fair_value_check = QCheckBox("Exit when price reaches fair value")
        self.exit_fair_value_check.setChecked(self.config.exit_on_fair_value)
        exit_layout.addWidget(self.exit_fair_value_check, 0, 0)
        
        self.exit_profit_check = QCheckBox("Exit at profit target")
        self.exit_profit_check.setChecked(self.config.exit_on_profit_target)
        exit_layout.addWidget(self.exit_profit_check, 0, 1)
        
        exit_layout.addWidget(QLabel("Profit Target ($):"), 1, 0)
        self.profit_target_input = QDoubleSpinBox()
        self.profit_target_input.setRange(0.01, 1.0)
        self.profit_target_input.setSingleStep(0.01)
        self.profit_target_input.setValue(self.config.profit_target)
        self.profit_target_input.setDecimals(2)
        exit_layout.addWidget(self.profit_target_input, 1, 1)
        
        exit_group.setLayout(exit_layout)
        layout.addWidget(exit_group)
        
        # Simulation mode
        sim_group = QGroupBox("Simulation Mode")
        sim_layout = QVBoxLayout()
        self.simulation_check = QCheckBox("Enable Simulation Mode (No Real Trades)")
        self.simulation_check.setChecked(self.config.use_simulation)
        sim_layout.addWidget(self.simulation_check)
        sim_group.setLayout(sim_layout)
        layout.addWidget(sim_group)
        
        # Save button
        save_btn = QPushButton("Save Configuration")
        save_btn.clicked.connect(self.save_config)
        save_btn.setStyleSheet("background-color: #2196F3; color: white; padding: 10px; font-weight: bold;")
        layout.addWidget(save_btn)
        
        layout.addStretch()
        return widget
    
    def create_positions_tab(self):
        """Create positions tab"""
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        
        self.positions_table = QTableWidget()
        self.positions_table.setColumnCount(8)
        self.positions_table.setHorizontalHeaderLabels([
            "Side", "Entry Price", "Size", "Current Price", 
            "Fair Value", "P&L", "P&L %", "Status"
        ])
        self.positions_table.horizontalHeader().setStretchLastSection(True)
        self.positions_table.setAlternatingRowColors(True)
        layout.addWidget(self.positions_table)
        
        return widget
    
    def create_logs_tab(self):
        """Create logs tab"""
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setFont(QFont("Courier", 9))
        layout.addWidget(self.log_text)
        
        clear_btn = QPushButton("Clear Logs")
        clear_btn.clicked.connect(lambda: self.log_text.clear())
        layout.addWidget(clear_btn)
        
        return widget
    
    def setup_timers(self):
        """Setup timers for periodic updates"""
        self.update_timer = QTimer()
        self.update_timer.timeout.connect(self.update_display)
        self.update_timer.start(1000)  # Update every second
    
    def log(self, message: str):
        """Add message to log"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log_text.append(f"[{timestamp}] {message}")
    
    def find_token_ids_from_url(self):
        """Find token IDs from Polymarket URL"""
        url = self.market_url_input.text().strip()
        if not url:
            self.finder_status_label.setText("Please enter a Polymarket market URL")
            self.finder_status_label.setStyleSheet("color: #F44336; font-size: 10px; padding: 5px;")
            return
        
        self.finder_status_label.setText("Searching for token IDs...")
        self.finder_status_label.setStyleSheet("color: #FF9800; font-size: 10px; padding: 5px;")
        
        try:
            # Extract slug from URL
            slug_match = re.search(r'/event/([^?]+)', url)
            if not slug_match:
                self.finder_status_label.setText("Invalid URL format. Expected: https://polymarket.com/event/...")
                self.finder_status_label.setStyleSheet("color: #F44336; font-size: 10px; padding: 5px;")
                return
            
            slug = slug_match.group(1)
            
            # Try Gamma Markets API
            gamma_url = "https://gamma-api.polymarket.com/markets"
            params = {"slug": slug}
            response = requests.get(gamma_url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and len(data) > 0:
                    market = data[0]
                elif isinstance(data, dict) and 'data' in data and len(data['data']) > 0:
                    market = data['data'][0]
                else:
                    market = data if isinstance(data, dict) else None
                
                if market and isinstance(market, dict):
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
                            outcomes_data = json.loads(outcomes_str)
                            if isinstance(outcomes_data, list):
                                outcome_names = [str(o) for o in outcomes_data]
                        except:
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
                    result_text = f"✅ Found market: {market.get('question', 'N/A')}\n\n"
                    
                    if len(token_ids_list) >= 2:
                        # Determine which is UP and which is DOWN
                        if 'up' in outcome_names[0].lower() or 'yes' in outcome_names[0].lower():
                            up_token = token_ids_list[0]
                            down_token = token_ids_list[1]
                        else:
                            up_token = token_ids_list[1]
                            down_token = token_ids_list[0]
                        
                        # Auto-fill the token ID fields
                        self.token_id_up_input.setText(up_token)
                        self.token_id_down_input.setText(down_token)
                        
                        result_text += f"✅ UP Token ID: {up_token}\n"
                        result_text += f"✅ DOWN Token ID: {down_token}\n\n"
                        result_text += "Token IDs have been automatically filled!"
                        self.finder_status_label.setStyleSheet("color: #4CAF50; font-size: 10px; padding: 5px;")
                        self.finder_status_label.setText(result_text)
                        self.log(f"✅ Token IDs extracted from URL: UP={up_token[:20]}..., DOWN={down_token[:20]}...")
                        return
                    elif len(token_ids_list) == 1:
                        result_text += f"⚠️ Found 1 token ID: {token_ids_list[0]}\n"
                        result_text += "This market may only have one outcome."
                        self.finder_status_label.setStyleSheet("color: #FF9800; font-size: 10px; padding: 5px;")
                    else:
                        result_text += "⚠️ Could not extract token IDs from API response."
                        self.finder_status_label.setStyleSheet("color: #FF9800; font-size: 10px; padding: 5px;")
                    
                    self.finder_status_label.setText(result_text)
                    return
            
            self.finder_status_label.setText("❌ Market not found. Please check the URL and try again.")
            self.finder_status_label.setStyleSheet("color: #F44336; font-size: 10px; padding: 5px;")
            
        except Exception as e:
            self.finder_status_label.setText(f"Error: {str(e)}")
            self.finder_status_label.setStyleSheet("color: #F44336; font-size: 10px; padding: 5px;")
            self.log(f"❌ Error finding token IDs: {e}")
    
    def save_config(self):
        """Save configuration from UI"""
        self.config.websocket_url = self.ws_url_input.text()
        self.config.polymarket_host = self.poly_host_input.text()
        self.config.token_id_up = self.token_id_up_input.text().strip() or None
        self.config.token_id_down = self.token_id_down_input.text().strip() or None
        self.config.token_id = self.config.token_id_up  # Legacy support
        self.config.chain_id = self.chain_id_input.value()
        self.config.entry_threshold = self.entry_threshold_input.value()
        self.config.profit_target = self.profit_target_input.value()
        self.config.max_position_size = self.max_size_input.value()
        self.config.min_position_size = self.min_size_input.value()
        self.config.exit_on_fair_value = self.exit_fair_value_check.isChecked()
        self.config.exit_on_profit_target = self.exit_profit_check.isChecked()
        self.config.use_simulation = self.simulation_check.isChecked()
        
        # Update strategy if exists
        if self.strategy:
            self.strategy.entry_strategy.threshold = self.config.entry_threshold
            self.strategy.exit_strategy.profit_target = self.config.profit_target
            self.strategy.exit_strategy.exit_on_fair_value = self.config.exit_on_fair_value
            self.strategy.exit_strategy.exit_on_profit_target = self.config.exit_on_profit_target
        
                                    # Update auto-trade status
        if self.trading_engine:
            self.trading_engine.auto_trade_enabled = self.auto_trade_check.isChecked()
            auto_status = "enabled" if self.trading_engine.auto_trade_enabled else "disabled"
            self.log(f"Auto-trading: {auto_status}")
        
        self.log("✅ Configuration saved")
    
    def start_bot(self):
        """Start the trading bot"""
        try:
            self.save_config()
        except Exception as e:
            self.log(f"❌ ERROR saving config: {type(e).__name__} - {str(e)}")
            return
        
        if not self.config.private_key:
            self.log("❌ ERROR: Private key not configured. Set PK in .env file")
            self.bot_status_label.setText("Error: No Private Key")
            self.bot_status_label.setStyleSheet("color: #F44336; font-size: 16px; font-weight: bold;")
            return
        
        if not self.config.websocket_url:
            self.log("❌ ERROR: WebSocket URL not configured")
            self.bot_status_label.setText("Error: No WebSocket URL")
            self.bot_status_label.setStyleSheet("color: #F44336; font-size: 16px; font-weight: bold;")
            return
        
        try:
            # Initialize Polymarket client
            try:
                self.polymarket_client = PolymarketClient(
                    host=self.config.polymarket_host,
                    private_key=self.config.private_key,
                    chain_id=self.config.chain_id,
                    api_key=self.config.api_key,
                    api_secret=self.config.api_secret,
                    api_passphrase=self.config.api_passphrase,
                    simulation_mode=self.config.use_simulation
                )
                
                # Start Polymarket worker thread
                self.polymarket_thread = PolymarketThread(self.polymarket_client)
                self.polymarket_worker = self.polymarket_thread.worker
                
                # Connect signals
                self.polymarket_worker.price_fetched.connect(self.on_price_fetched)
                self.polymarket_worker.order_placed.connect(self.on_order_placed)
                self.polymarket_worker.error_occurred.connect(self.on_polymarket_error)
                
                # Start the thread
                self.polymarket_thread.start()
                self.log("✅ Polymarket client initialized (separate thread)")
            except ValueError as e:
                self.log(f"❌ ERROR: Invalid configuration - {str(e)}")
                self.bot_status_label.setText("Error: Invalid Config")
                self.bot_status_label.setStyleSheet("color: #F44336; font-size: 16px; font-weight: bold;")
                return
            except Exception as e:
                self.log(f"❌ ERROR initializing Polymarket client: {type(e).__name__} - {str(e)}")
                self.bot_status_label.setText("Error: Client Init Failed")
                self.bot_status_label.setStyleSheet("color: #F44336; font-size: 16px; font-weight: bold;")
                return
            
            # Initialize strategy
            try:
                self.strategy = TradingStrategy(
                    entry_strategy=EntryStrategy(threshold=self.config.entry_threshold),
                    exit_strategy=ExitStrategy(
                        profit_target=self.config.profit_target,
                        exit_on_fair_value=self.config.exit_on_fair_value,
                        exit_on_profit_target=self.config.exit_on_profit_target
                    )
                )
                self.log("✅ Trading strategy initialized")
            except Exception as e:
                self.log(f"❌ ERROR initializing strategy: {type(e).__name__} - {str(e)}")
                self.bot_status_label.setText("Error: Strategy Init Failed")
                self.bot_status_label.setStyleSheet("color: #F44336; font-size: 16px; font-weight: bold;")
                return
            
            # Initialize trading engine with log callback
            try:
                self.trading_engine = TradingEngine(
                    config=self.config,
                    polymarket_client=self.polymarket_client,
                    strategy=self.strategy,
                    log_callback=self.log  # Pass log function for UI updates
                )
                # Pass worker to trading engine for async order placement
                if self.polymarket_worker:
                    self.trading_engine.polymarket_worker = self.polymarket_worker
                
                # Set auto-trade status from UI
                self.trading_engine.auto_trade_enabled = self.auto_trade_check.isChecked()
                self.trading_engine.start()
                auto_status = "enabled" if self.trading_engine.auto_trade_enabled else "disabled"
                self.log(f"✅ Trading engine started (Auto-trading: {auto_status})")
            except Exception as e:
                self.log(f"❌ ERROR starting trading engine: {type(e).__name__} - {str(e)}")
                self.bot_status_label.setText("Error: Engine Start Failed")
                self.bot_status_label.setStyleSheet("color: #F44336; font-size: 16px; font-weight: bold;")
                return
            
            # Start WebSocket connection
            try:
                self.ws_thread = WebSocketThread(self.config.websocket_url)
                self.ws_thread.data_received.connect(self.on_websocket_data)
                self.ws_thread.connection_status.connect(self.on_websocket_status)
                self.ws_thread.start()
                self.log("✅ WebSocket thread started")
            except Exception as e:
                self.log(f"❌ ERROR starting WebSocket: {type(e).__name__} - {str(e)}")
                # Try to clean up
                if self.trading_engine:
                    try:
                        self.trading_engine.stop()
                    except:
                        pass
                self.bot_status_label.setText("Error: WebSocket Failed")
                self.bot_status_label.setStyleSheet("color: #F44336; font-size: 16px; font-weight: bold;")
                return
            
            # Store token IDs
            self.token_id_up = self.config.token_id_up or self.config.token_id
            self.token_id_down = self.config.token_id_down
            
            if not self.token_id_up and not self.token_id_down:
                self.log("⚠️ WARNING: No token IDs configured. Market URL will be auto-generated from WebSocket data.")
            
            self.running = True
            # Start continuous price fetching every 1 second
            if self.price_fetch_timer:
                self.price_fetch_timer.start(1000)  # Every 1 second
            self.start_btn.setEnabled(False)
            self.stop_btn.setEnabled(True)
            self.bot_status_label.setText("Bot Running")
            self.bot_status_label.setStyleSheet("color: #4CAF50; font-size: 16px; font-weight: bold;")
            self.log("✅ Bot started successfully")
            
        except KeyboardInterrupt:
            self.log("⏹️ Bot stopped by user")
            self.stop_bot()
        except Exception as e:
            error_msg = f"❌ CRITICAL ERROR starting bot: {type(e).__name__} - {str(e)}"
            self.log(error_msg)
            import traceback
            self.log(f"Traceback: {traceback.format_exc()}")
            self.bot_status_label.setText("Error: Startup Failed")
            self.bot_status_label.setStyleSheet("color: #F44336; font-size: 16px; font-weight: bold;")
    
    def stop_bot(self):
        """Stop the trading bot"""
        try:
            self.running = False
            
            # Stop price fetching timer
            if self.price_fetch_timer:
                self.price_fetch_timer.stop()
            
            if self.trading_engine:
                try:
                    self.trading_engine.stop()
                    self.log("✅ Trading engine stopped")
                except Exception as e:
                    self.log(f"⚠️ Error stopping trading engine: {type(e).__name__} - {str(e)}")
            
            if self.polymarket_thread:
                try:
                    self.polymarket_thread.stop()
                    self.log("✅ Polymarket thread stopped")
                except Exception as e:
                    self.log(f"⚠️ Error stopping Polymarket thread: {type(e).__name__} - {str(e)}")
            
            if self.ws_thread:
                try:
                    self.ws_thread.stop()
                    if not self.ws_thread.wait(5000):  # Wait up to 5 seconds
                        self.log("⚠️ WebSocket thread did not stop gracefully")
                        self.ws_thread.terminate()
                except Exception as e:
                    self.log(f"⚠️ Error stopping WebSocket: {type(e).__name__} - {str(e)}")
            
            self.start_btn.setEnabled(True)
            self.stop_btn.setEnabled(False)
            self.bot_status_label.setText("Bot Stopped")
            self.bot_status_label.setStyleSheet("color: #F44336; font-size: 16px; font-weight: bold;")
            self.log("⏹️ Bot stopped")
        except Exception as e:
            error_msg = f"❌ ERROR stopping bot: {type(e).__name__} - {str(e)}"
            self.log(error_msg)
            # Force UI update even if there's an error
            try:
                self.start_btn.setEnabled(True)
                self.stop_btn.setEnabled(False)
                self.bot_status_label.setText("Bot Stopped (with errors)")
                self.bot_status_label.setStyleSheet("color: #FF9800; font-size: 16px; font-weight: bold;")
            except:
                pass
    
    def on_websocket_status(self, status: str, message: str):
        """Handle WebSocket status updates"""
        if status == "connected":
            self.ws_status_label.setText(f"WebSocket: ✅ Connected")
            self.ws_status_label.setStyleSheet("color: #4CAF50; font-size: 12px;")
        elif status == "disconnected":
            self.ws_status_label.setText(f"WebSocket: ❌ Disconnected")
            self.ws_status_label.setStyleSheet("color: #F44336; font-size: 12px;")
        else:
            self.ws_status_label.setText(f"WebSocket: ⚠️ {message}")
            self.ws_status_label.setStyleSheet("color: #FF9800; font-size: 12px;")
        self.log(f"WebSocket: {status} - {message}")
    
    def fetch_prices_continuously(self):
        """Continuously fetch prices from Polymarket every 1 second - Update UI in real-time"""
        if not self.running:
            return
        
        try:
            # Always fetch fresh prices for UP token
            if self.token_id_up:
                if self.polymarket_worker:
                    self.pending_price_requests[self.token_id_up] = "UP"
                    self.polymarket_worker.fetch_price(self.token_id_up, "display")
                elif self.polymarket_client:
                    # Fallback: direct fetch if worker not available
                    try:
                        price = self.polymarket_client.get_market_price_display(self.token_id_up)
                        if price and 0 < price < 1:
                            if not hasattr(self, 'cached_prices'):
                                self.cached_prices = {}
                            self.cached_prices['UP'] = price
                            if self.last_market_data:
                                self.last_market_data.market_price_up = price
                                self.update_market_display(self.last_market_data)
                    except:
                        pass
            
            # Always fetch fresh prices for DOWN token
            if self.token_id_down:
                if self.polymarket_worker:
                    self.pending_price_requests[self.token_id_down] = "DOWN"
                    self.polymarket_worker.fetch_price(self.token_id_down, "display")
                elif self.polymarket_client:
                    # Fallback: direct fetch if worker not available
                    try:
                        price = self.polymarket_client.get_market_price_display(self.token_id_down)
                        if price and 0 < price < 1:
                            if not hasattr(self, 'cached_prices'):
                                self.cached_prices = {}
                            self.cached_prices['DOWN'] = price
                            if self.last_market_data:
                                self.last_market_data.market_price_down = price
                                self.update_market_display(self.last_market_data)
                    except:
                        pass
        except Exception as e:
            self.log(f"⚠️ Error in continuous price fetch: {str(e)[:50]}")
    
    def on_price_fetched(self, token_id: str, price: float):
        """Handle price fetched from Polymarket worker thread - Update UI immediately"""
        try:
            # Validate price
            if not price or price <= 0 or price >= 1:
                return
            
            # Initialize cached prices if needed
            if not hasattr(self, 'cached_prices'):
                self.cached_prices = {}
            
            # Update cache based on token_id
            if token_id in self.pending_price_requests:
                side = self.pending_price_requests[token_id]
                self.cached_prices[side] = price
            
            # Also update by direct token ID match
            price_updated = False
            if token_id == self.token_id_up:
                self.cached_prices['UP'] = price
                if self.last_market_data:
                    self.last_market_data.market_price_up = price
                    price_updated = True
            elif token_id == self.token_id_down:
                self.cached_prices['DOWN'] = price
                if self.last_market_data:
                    self.last_market_data.market_price_down = price
                    price_updated = True
            
            # Update display immediately with new prices
            if price_updated and self.last_market_data:
                # Force immediate UI update
                self.update_market_display(self.last_market_data)
            elif self.last_market_data:
                # Update even if we don't have a direct match, refresh display
                self.update_market_display(self.last_market_data)
        except Exception as e:
            self.log(f"⚠️ Error handling price fetch: {type(e).__name__} - {str(e)[:50]}")
    
    def on_order_placed(self, order_data: dict):
        """Handle order placed from Polymarket worker thread"""
        try:
            token_id = order_data.get('token_id')
            side = order_data.get('side')
            price = order_data.get('price')
            size = order_data.get('size')
            response = order_data.get('response')
            
            if response:
                self.log(f"✅ {side} order placed: {size} shares @ ${price:.4f} (Token: {token_id[:20]}...)")
            else:
                self.log(f"⚠️ {side} order may have failed (no response)")
        except Exception as e:
            self.log(f"⚠️ Error handling order placement: {type(e).__name__} - {str(e)[:50]}")
    
    def on_polymarket_error(self, operation: str, error: str):
        """Handle errors from Polymarket worker thread"""
        self.log(f"⚠️ Polymarket error ({operation}): {error[:50]}")
    
    def on_websocket_data(self, data: dict):
        """Handle WebSocket data updates"""
        if not isinstance(data, dict):
            self.log(f"⚠️ Invalid WebSocket data format: {type(data)}")
            return
        
        try:
            # Store WebSocket data for URL regeneration
            self.last_ws_data = data
            
            # Get market information from WebSocket
            symbol = data.get('symbol', '')
            interval = data.get('interval', '')
            
            # Extract hour from interval to detect hourly market changes
            try:
                current_hour = self._extract_hour_from_interval(interval)
            except Exception as e:
                self.log(f"⚠️ Error extracting hour from interval: {str(e)[:50]}")
                current_hour = None
            
            # Generate market URL if symbol/interval/hour changed
            if symbol and interval:
                try:
                    needs_regeneration = (
                        symbol != self.current_symbol or 
                        interval != self.current_interval or
                        current_hour != self.current_hour
                    )
                    
                    if needs_regeneration:
                        self.current_symbol = symbol
                        self.current_interval = interval
                        self.current_hour = current_hour
                        self.log(f"🔄 Market changed - regenerating URL for {symbol} at {interval}")
                        self.generate_market_url_from_data(symbol, interval, data)
                except Exception as e:
                    self.log(f"⚠️ Error generating market URL: {type(e).__name__} - {str(e)[:50]}")
            
            # Get data from WebSocket (external software)
            try:
                prob_up = float(data.get('prob_up', 0))
                prob_down = float(data.get('prob_down', 0))
                live_price = float(data.get('live_price', 0))
                period_open = float(data.get('period_open', 0))
            except (ValueError, TypeError) as e:
                self.log(f"⚠️ Invalid data values: {str(e)[:50]}")
                prob_up = 0
                prob_down = 0
                live_price = 0
                period_open = 0
            
            # Update interval, live_price, and period_open labels immediately
            try:
                if interval:
                    self.interval_label.setText(interval)
                    self.interval_label.setStyleSheet("color: #4CAF50; font-weight: bold; font-size: 12px;")
                else:
                    self.interval_label.setText("N/A")
                    self.interval_label.setStyleSheet("color: #666; font-weight: bold; font-size: 12px;")
                
                if live_price > 0:
                    self.live_price_label.setText(f"${live_price:,.4f}")
                    self.live_price_label.setStyleSheet("color: #2196F3; font-weight: bold; font-size: 14px;")
                else:
                    self.live_price_label.setText("$0.0000")
                    self.live_price_label.setStyleSheet("color: #999; font-weight: bold; font-size: 14px;")
                
                if period_open > 0:
                    self.period_open_label.setText(f"${period_open:,.4f}")
                    self.period_open_label.setStyleSheet("color: #FF9800; font-weight: bold; font-size: 12px;")
                else:
                    self.period_open_label.setText("$0.0000")
                    self.period_open_label.setStyleSheet("color: #999; font-weight: bold; font-size: 12px;")
            except Exception as e:
                self.log(f"⚠️ Error updating interval/live_price/period_open: {str(e)[:50]}")
            
            # Fetch Polymarket prices asynchronously (non-blocking via worker thread)
            market_price_up = 0.0
            market_price_down = 0.0
            
            # Initialize cached prices if needed
            if not hasattr(self, 'cached_prices'):
                self.cached_prices = {}
            
            # Request prices from worker thread (non-blocking) - ALWAYS fetch fresh prices
            if self.token_id_up and self.polymarket_worker:
                self.pending_price_requests[self.token_id_up] = "UP"
                # Try multiple methods for better reliability
                self.polymarket_worker.fetch_price(self.token_id_up, "display")
            
            if self.token_id_down and self.polymarket_worker:
                self.pending_price_requests[self.token_id_down] = "DOWN"
                self.polymarket_worker.fetch_price(self.token_id_down, "display")
            
            # Use cached prices if available (from previous fetch) as fallback
            if 'UP' in self.cached_prices:
                market_price_up = self.cached_prices['UP']
            if 'DOWN' in self.cached_prices:
                market_price_down = self.cached_prices['DOWN']
            
            # Also try direct fetch as backup if worker not available
            if market_price_up == 0.0 and self.token_id_up and self.polymarket_client:
                try:
                    direct_price = self.polymarket_client.get_market_price_display(self.token_id_up)
                    if direct_price and 0 < direct_price < 1:
                        market_price_up = direct_price
                        if not hasattr(self, 'cached_prices'):
                            self.cached_prices = {}
                        self.cached_prices['UP'] = direct_price
                except:
                    pass
            
            if market_price_down == 0.0 and self.token_id_down and self.polymarket_client:
                try:
                    direct_price = self.polymarket_client.get_market_price_display(self.token_id_down)
                    if direct_price and 0 < direct_price < 1:
                        market_price_down = direct_price
                        if not hasattr(self, 'cached_prices'):
                            self.cached_prices = {}
                        self.cached_prices['DOWN'] = direct_price
                except:
                    pass
            
            # Calculate DOWN from UP if needed
            if market_price_down == 0.0 and market_price_up > 0:
                try:
                    market_price_down = 1.0 - market_price_up
                except:
                    market_price_down = 0.0
            
            # Create market data
            try:
                market_data = MarketData(
                    software_prob_up=prob_up,
                    software_prob_down=prob_down,
                    market_price_up=market_price_up,
                    market_price_down=market_price_down,
                    symbol=symbol,
                    interval=interval,
                    timestamp=time.time()
                )
                
                self.last_market_data = market_data
                
                # Update trading engine
                if self.trading_engine:
                    try:
                        self.trading_engine.update_market_data(market_data)
                    except Exception as e:
                        self.log(f"⚠️ Error updating trading engine: {type(e).__name__} - {str(e)[:50]}")
                
                # Update display
                try:
                    self.update_market_display(market_data)
                except Exception as e:
                    self.log(f"⚠️ Error updating display: {type(e).__name__} - {str(e)[:50]}")
            except Exception as e:
                self.log(f"❌ Error creating market data: {type(e).__name__} - {str(e)[:50]}")
            
        except KeyError as e:
            self.log(f"⚠️ Missing key in WebSocket data: {str(e)}")
        except Exception as e:
            error_msg = f"❌ CRITICAL ERROR processing WebSocket data: {type(e).__name__} - {str(e)}"
            self.log(error_msg)
            import traceback
            self.log(f"Traceback: {traceback.format_exc()[:200]}")
    
    def generate_market_url_from_data(self, symbol: str, interval: str, data: dict):
        """Generate Polymarket market URL from WebSocket data"""
        try:
            if not symbol or not interval:
                self.log("⚠️ Cannot generate URL: missing symbol or interval")
                return
            
            self.market_url_label.setText("Generating market URL...")
            self.market_url_label.setStyleSheet("color: #FF9800; font-size: 10px; padding: 5px; background-color: #FFF3E0; border-radius: 5px;")
            
            # Run in background to avoid blocking
            QTimer.singleShot(100, lambda: self._generate_url_async(symbol, interval, data))
        except Exception as e:
            self.log(f"❌ Error in generate_market_url_from_data: {type(e).__name__} - {str(e)}")
            self.market_url_label.setText(f"Error: {str(e)[:50]}")
            self.market_url_label.setStyleSheet("color: #F44336; font-size: 10px; padding: 5px; background-color: #FFEBEE; border-radius: 5px;")
    
    def _generate_url_async(self, symbol: str, interval: str, data: dict):
        """Generate URL asynchronously"""
        try:
            # Build search terms from symbol
            search_terms = []
            symbol_upper = symbol.upper()
            
            if 'BTC' in symbol_upper or 'BITCOIN' in symbol_upper:
                search_terms.append('bitcoin')
            elif 'ETH' in symbol_upper or 'ETHEREUM' in symbol_upper:
                search_terms.append('ethereum')
            elif 'SOL' in symbol_upper or 'SOLANA' in symbol_upper:
                search_terms.append('solana')
            else:
                search_terms.append(symbol.lower())
            
            # Add "up or down" to search
            search_terms.append('up or down')
            
            # Parse interval/time information
            # Example: "November 18, 5-6AM ET" or "5-6AM ET" or "5am"
            time_info = interval.lower()
            now = datetime.now()
            
            # Try to extract date and time from interval
            date_str = ""
            time_str = ""
            
            # Look for month names
            months = ['january', 'february', 'march', 'april', 'may', 'june',
                     'july', 'august', 'september', 'october', 'november', 'december']
            for month in months:
                if month in time_info:
                    # Extract date
                    month_num = months.index(month) + 1
                    # Try to find day number
                    day_match = re.search(rf'{month}\s+(\d+)', time_info)
                    if day_match:
                        day = int(day_match.group(1))
                        date_str = f"{month}-{day}".lower()
                    else:
                        date_str = f"{month}-{now.day}".lower()
                    break
            
            # Extract time (e.g., "5-6am", "5am", "5-6AM ET")
            # Match patterns like "5-6am", "5am", "6-7AM ET", "6AM"
            time_match = re.search(r'(\d+)(?:-(\d+))?(am|pm)', time_info, re.IGNORECASE)
            if time_match:
                hour1 = int(time_match.group(1))
                hour2 = time_match.group(2)
                am_pm = time_match.group(3).lower()
                # Keep hour in 12-hour format for slug (e.g., "6am" not "18am")
                time_str = f"{hour1}{am_pm}"
            else:
                # Try to find just hour number (e.g., "6" in "6AM ET")
                hour_match = re.search(r'\b(\d+)\s*(am|pm|AM|PM)', time_info, re.IGNORECASE)
                if hour_match:
                    hour1 = int(hour_match.group(1))
                    am_pm = hour_match.group(2).lower()
                    time_str = f"{hour1}{am_pm}"
                else:
                    # Default to current hour
                    hour = now.hour
                    if hour == 0:
                        time_str = "12am"
                    elif hour < 12:
                        time_str = f"{hour}am"
                    elif hour == 12:
                        time_str = "12pm"
                    else:
                        time_str = f"{hour-12}pm"
            
            # Build slug matching Polymarket format: bitcoin-up-or-down-november-18-6am-et
            slug_parts = []
            
            # Asset name
            if 'bitcoin' in search_terms or 'btc' in symbol_upper:
                slug_parts.append('bitcoin')
            elif 'ethereum' in search_terms or 'eth' in symbol_upper:
                slug_parts.append('ethereum')
            elif 'solana' in search_terms or 'sol' in symbol_upper:
                slug_parts.append('solana')
            else:
                slug_parts.append(symbol.lower().replace(' ', '-'))
            
            slug_parts.append('up-or-down')
            
            # Date: november-18 format
            if date_str:
                slug_parts.append(date_str)
            else:
                # Default: current month-day
                month_name = now.strftime('%B').lower()  # "november"
                day = now.strftime('%d').lstrip('0')  # "18" not "018"
                slug_parts.append(f"{month_name}-{day}")
            
            # Time: 6am format (not 6am-et, just 6am)
            if time_match:
                hour1 = int(time_match.group(1))
                am_pm = time_match.group(3).lower()
                # Format as "6am" or "6pm"
                slug_parts.append(f"{hour1}{am_pm}")
            else:
                # Default: current hour
                hour = now.hour
                if hour == 0:
                    time_part = "12am"
                elif hour < 12:
                    time_part = f"{hour}am"
                elif hour == 12:
                    time_part = "12pm"
                else:
                    time_part = f"{hour-12}pm"
                slug_parts.append(time_part)
            
            # Add "et" timezone
            slug_parts.append('et')
            
            constructed_slug = '-'.join(slug_parts)
            constructed_url = f"https://polymarket.com/event/{constructed_slug}"
            
            # Try to find exact match first
            gamma_url = "https://gamma-api.polymarket.com/markets"
            params = {"slug": constructed_slug}
            
            try:
                response = requests.get(gamma_url, params=params, timeout=5)
                if response.status_code == 200:
                    markets = response.json()
                    if isinstance(markets, list) and len(markets) > 0:
                        market = markets[0]
                        slug = market.get('slug', constructed_slug)
                        found_url = f"https://polymarket.com/event/{slug}"
                        self.current_market_url = found_url
                        self.market_url_label.setText(found_url)
                        self.market_url_label.setStyleSheet("color: #4CAF50; font-size: 10px; padding: 5px; background-color: #E8F5E9; border-radius: 5px;")
                        
                        # Extract token IDs from this market
                        self.extract_token_ids_from_market_data(market)
                        self.log(f"✅ Market URL generated: {found_url}")
                        return
            except:
                pass
            
            # If no exact match, search for active markets matching current time
            try:
                # Search for active markets with better time matching
                now = datetime.now()
                current_hour_et = now.hour  # Adjust for ET timezone if needed
                
                # Search active markets
                search_params = {
                    "active": "true", 
                    "limit": 50,  # Get more results for better matching
                    "closed": "false"
                }
                response = requests.get(gamma_url, params=search_params, timeout=10)
                if response.status_code == 200:
                    markets = response.json()
                    if isinstance(markets, list):
                        best_match = None
                        best_score = 0
                        
                        for market in markets:
                            question = market.get('question', '').lower()
                            slug = market.get('slug', '')
                            
                            # Calculate match score
                            score = 0
                            
                            # Check symbol match
                            if any(term in question for term in search_terms):
                                score += 10
                            
                            # Check if it's an "up or down" market
                            if 'up' in question and 'down' in question:
                                score += 5
                            
                            # Check time match - look for hour in question/slug
                            if time_str:
                                hour_from_time = int(re.search(r'(\d+)', time_str).group(1)) if re.search(r'(\d+)', time_str) else None
                                if hour_from_time:
                                    # Check if hour appears in question or slug
                                    if str(hour_from_time) in question or str(hour_from_time) in slug:
                                        score += 10
                                    # Check for am/pm match
                                    if 'am' in time_str and 'am' in question:
                                        score += 5
                                    elif 'pm' in time_str and 'pm' in question:
                                        score += 5
                            
                            # Check date match
                            if date_str:
                                month_day = date_str.split('-')
                                if len(month_day) == 2:
                                    month_name = month_day[0]
                                    day = month_day[1]
                                    if month_name in question and day in question:
                                        score += 10
                            
                            # Prefer markets with token IDs
                            if market.get('clobTokenIds'):
                                score += 5
                            
                            if score > best_score:
                                best_score = score
                                best_match = market
                        
                        if best_match and best_score >= 10:  # Minimum score threshold
                            slug = best_match.get('slug', '')
                            if slug:
                                found_url = f"https://polymarket.com/event/{slug}"
                                self.current_market_url = found_url
                                self.market_url_label.setText(found_url)
                                self.market_url_label.setStyleSheet("color: #4CAF50; font-size: 10px; padding: 5px; background-color: #E8F5E9; border-radius: 5px;")
                                
                                # Extract token IDs
                                self.extract_token_ids_from_market_data(best_match)
                                self.log(f"✅ Market URL found (score: {best_score}): {found_url}")
                                return
            except Exception as e:
                self.log(f"⚠️ Error searching active markets: {str(e)[:50]}")
            
            # Fallback: show constructed URL
            self.current_market_url = constructed_url
            self.market_url_label.setText(f"{constructed_url}\n(Estimated - verify on Polymarket)")
            self.market_url_label.setStyleSheet("color: #FF9800; font-size: 10px; padding: 5px; background-color: #FFF3E0; border-radius: 5px;")
            
            # Try to extract token IDs from constructed URL
            self.extract_token_ids_from_url(constructed_url)
            self.log(f"⚠️ Using estimated URL: {constructed_url}")
            
        except Exception as e:
            self.market_url_label.setText(f"Error generating URL: {str(e)[:50]}")
            self.market_url_label.setStyleSheet("color: #F44336; font-size: 10px; padding: 5px; background-color: #FFEBEE; border-radius: 5px;")
            self.log(f"❌ Error generating market URL: {e}")
    
    def extract_token_ids_from_market_data(self, market: dict):
        """Extract token IDs from market data and update config"""
        try:
            clob_token_ids = market.get('clobTokenIds', '')
            outcomes_str = market.get('outcomes', '')
            
            # Parse token IDs (same logic as find_token_ids_from_url)
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
                    outcomes_data = json.loads(outcomes_str)
                    if isinstance(outcomes_data, list):
                        outcome_names = [str(o) for o in outcomes_data]
                except:
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
            
            # Map token IDs
            if len(token_ids_list) >= 2:
                if 'up' in outcome_names[0].lower() or 'yes' in outcome_names[0].lower():
                    up_token = token_ids_list[0]
                    down_token = token_ids_list[1]
                else:
                    up_token = token_ids_list[1]
                    down_token = token_ids_list[0]
                
                # Update config and UI
                self.config.token_id_up = up_token
                self.config.token_id_down = down_token
                self.token_id_up = up_token
                self.token_id_down = down_token
                
                # Update UI fields
                self.token_id_up_input.setText(up_token)
                self.token_id_down_input.setText(down_token)
                
                self.log(f"✅ Token IDs auto-extracted: UP={up_token[:20]}..., DOWN={down_token[:20]}...")
                
        except Exception as e:
            self.log(f"⚠️ Error extracting token IDs: {e}")
    
    def extract_token_ids_from_url(self, url: str):
        """Extract token IDs from URL"""
        try:
            slug_match = re.search(r'/event/([^?]+)', url)
            if not slug_match:
                return
            
            slug = slug_match.group(1)
            gamma_url = "https://gamma-api.polymarket.com/markets"
            params = {"slug": slug}
            response = requests.get(gamma_url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and len(data) > 0:
                    market = data[0]
                elif isinstance(data, dict) and 'data' in data and len(data['data']) > 0:
                    market = data['data'][0]
                else:
                    market = data if isinstance(data, dict) else None
                
                if market:
                    self.extract_token_ids_from_market_data(market)
        except Exception as e:
            self.log(f"⚠️ Error fetching token IDs from URL: {e}")
    
    def _extract_hour_from_interval(self, interval: str) -> Optional[int]:
        """Extract hour number from interval string for comparison"""
        if not interval:
            return None
        
        time_info = interval.lower()
        # Try to find hour in format like "6am", "6-7am", "6AM ET"
        time_match = re.search(r'(\d+)(?:-(\d+))?(am|pm)', time_info, re.IGNORECASE)
        if time_match:
            hour1 = int(time_match.group(1))
            am_pm = time_match.group(3).lower()
            # Convert to 24-hour format for comparison
            if am_pm == 'pm' and hour1 < 12:
                return hour1 + 12
            elif am_pm == 'am' and hour1 == 12:
                return 0
            else:
                return hour1
        
        # Try alternative pattern
        hour_match = re.search(r'\b(\d+)\s*(am|pm|AM|PM)', time_info, re.IGNORECASE)
        if hour_match:
            hour1 = int(hour_match.group(1))
            am_pm = hour_match.group(2).lower()
            if am_pm == 'pm' and hour1 < 12:
                return hour1 + 12
            elif am_pm == 'am' and hour1 == 12:
                return 0
            else:
                return hour1
        
        return None
    
    def check_and_update_market_url(self):
        """Check if market URL needs updating (called every minute)"""
        try:
            if not self.last_ws_data:
                return
            
            symbol = self.last_ws_data.get('symbol', '')
            interval = self.last_ws_data.get('interval', '')
            
            if symbol and interval:
                try:
                    current_hour = self._extract_hour_from_interval(interval)
                    # If hour changed, regenerate URL
                    if current_hour is not None and current_hour != self.current_hour:
                        self.log(f"🔄 Hour changed from {self.current_hour} to {current_hour} - updating market URL")
                        self.current_hour = current_hour
                        self.generate_market_url_from_data(symbol, interval, self.last_ws_data)
                except Exception as e:
                    self.log(f"⚠️ Error checking market URL update: {type(e).__name__} - {str(e)[:50]}")
        except Exception as e:
            self.log(f"⚠️ Error in check_and_update_market_url: {type(e).__name__} - {str(e)[:50]}")
    
    def regenerate_market_url(self):
        """Regenerate market URL every hour (forced update)"""
        try:
            if self.current_symbol and self.current_interval and self.last_ws_data:
                self.log("🔄 Regenerating market URL (hourly forced update)...")
                # Force regeneration by clearing current hour
                self.current_hour = None
                self.generate_market_url_from_data(self.current_symbol, self.current_interval, self.last_ws_data)
        except Exception as e:
            self.log(f"⚠️ Error in regenerate_market_url: {type(e).__name__} - {str(e)[:50]}")
    
    def update_market_display(self, market_data: MarketData):
        """Update market data display"""
        try:
            if not market_data:
                return
            
            # Software probabilities
            try:
                if market_data.software_prob_up > 0:
                    self.software_prob_up_label.setText(f"{market_data.software_prob_up:.1f}%")
                else:
                    self.software_prob_up_label.setText("0.0%")
            except Exception as e:
                self.software_prob_up_label.setText("Error")
            
            try:
                if market_data.software_prob_down > 0:
                    self.software_prob_down_label.setText(f"{market_data.software_prob_down:.1f}%")
                else:
                    self.software_prob_down_label.setText("0.0%")
            except Exception as e:
                self.software_prob_down_label.setText("Error")
            
            # Software prices (converted from probabilities)
            try:
                if market_data.software_price_up > 0:
                    self.software_price_up_label.setText(f"${market_data.software_price_up:.4f}")
                else:
                    self.software_price_up_label.setText("$0.0000")
            except Exception as e:
                self.software_price_up_label.setText("Error")
            
            try:
                if market_data.software_price_down > 0:
                    self.software_price_down_label.setText(f"${market_data.software_price_down:.4f}")
                else:
                    self.software_price_down_label.setText("$0.0000")
            except Exception as e:
                self.software_price_down_label.setText("Error")
            
            # Polymarket prices - make sure they're visible
            try:
                if market_data.market_price_up > 0:
                    self.poly_price_up_label.setText(f"${market_data.market_price_up:.4f}")
                    self.poly_price_up_label.setStyleSheet("color: #4CAF50; font-weight: bold; font-size: 14px;")
                else:
                    self.poly_price_up_label.setText("N/A")
                    self.poly_price_up_label.setStyleSheet("color: #999; font-weight: bold;")
            except Exception as e:
                self.poly_price_up_label.setText("Error")
            
            try:
                if market_data.market_price_down > 0:
                    self.poly_price_down_label.setText(f"${market_data.market_price_down:.4f}")
                    self.poly_price_down_label.setStyleSheet("color: #F44336; font-weight: bold; font-size: 14px;")
                else:
                    self.poly_price_down_label.setText("N/A")
                    self.poly_price_down_label.setStyleSheet("color: #999; font-weight: bold;")
            except Exception as e:
                self.poly_price_down_label.setText("Error")
            
            # Price differences
            try:
                diff_up = market_data.price_difference_up
                diff_down = market_data.price_difference_down
                
                self.diff_up_label.setText(f"${diff_up:.4f}")
                self.diff_down_label.setText(f"${diff_down:.4f}")
                
                # Color code differences
                if diff_up >= self.config.entry_threshold:
                    self.diff_up_label.setStyleSheet("color: #4CAF50; font-weight: bold; font-size: 12px;")
                else:
                    self.diff_up_label.setStyleSheet("color: #666; font-weight: bold;")
                
                if diff_down >= self.config.entry_threshold:
                    self.diff_down_label.setStyleSheet("color: #4CAF50; font-weight: bold; font-size: 12px;")
                else:
                    self.diff_down_label.setStyleSheet("color: #666; font-weight: bold;")
            except Exception as e:
                self.diff_up_label.setText("Error")
                self.diff_down_label.setText("Error")
            
            # Entry signal
            try:
                if self.trading_engine and self.strategy:
                    signal, reason = self.strategy.entry_strategy.should_enter(market_data, self.trading_engine.positions)
                    if signal.value != "NO_SIGNAL":
                        self.entry_signal_label.setText(f"{signal.value}: {reason}")
                        if signal.value == "BUY":
                            self.entry_signal_label.setStyleSheet("color: #4CAF50; font-size: 14px; font-weight: bold;")
                        else:
                            self.entry_signal_label.setStyleSheet("color: #F44336; font-size: 14px; font-weight: bold;")
                    else:
                        self.entry_signal_label.setText("NO SIGNAL")
                        self.entry_signal_label.setStyleSheet("color: #999; font-size: 14px; font-weight: bold;")
            except Exception as e:
                self.entry_signal_label.setText("Error")
                self.entry_signal_label.setStyleSheet("color: #999; font-size: 14px; font-weight: bold;")
        except Exception as e:
            self.log(f"⚠️ Error updating market display: {type(e).__name__} - {str(e)[:50]}")
    
    def update_display(self):
        """Update display with current status"""
        if self.trading_engine:
            status = self.trading_engine.get_status()
            
            # Update statistics
            self.positions_count_label.setText(str(status['positions']))
            self.total_pnl_label.setText(f"${status['total_pnl']:.2f}")
            self.trades_count_label.setText(str(status.get('trades_today', 0)))
            
            win_rate = status.get('win_rate', 0)
            self.win_rate_label.setText(f"{win_rate:.1f}%")
            
            # Update positions table
            self.update_positions_table()
    
    def update_positions_table(self):
        """Update positions table"""
        if not self.trading_engine:
            return
        
        positions = self.trading_engine.positions
        self.positions_table.setRowCount(len(positions))
        
        for i, position in enumerate(positions):
            # Update position prices
            if self.polymarket_client:
                try:
                    live_price = self.polymarket_client.get_market_price_display(position.token_id)
                    if live_price and 0 < live_price < 1:
                        position.current_price = live_price
                except:
                    pass
            
            # Calculate P&L
            if position.side == "BUY":
                pnl = (position.current_price - position.entry_price) * position.size
            else:
                pnl = (position.entry_price - position.current_price) * position.size
            
            pnl_percent = (pnl / (position.entry_price * position.size)) * 100 if position.entry_price > 0 else 0
            
            # Update table
            self.positions_table.setItem(i, 0, QTableWidgetItem(position.side))
            self.positions_table.setItem(i, 1, QTableWidgetItem(f"${position.entry_price:.4f}"))
            self.positions_table.setItem(i, 2, QTableWidgetItem(f"{position.size:.0f}"))
            self.positions_table.setItem(i, 3, QTableWidgetItem(f"${position.current_price:.4f}"))
            self.positions_table.setItem(i, 4, QTableWidgetItem(f"${position.current_fair_value:.4f}"))
            self.positions_table.setItem(i, 5, QTableWidgetItem(f"${pnl:.2f}"))
            self.positions_table.setItem(i, 6, QTableWidgetItem(f"{pnl_percent:.2f}%"))
            self.positions_table.setItem(i, 7, QTableWidgetItem("Open"))
            
            # Color code P&L
            if pnl > 0:
                self.positions_table.item(i, 5).setForeground(Qt.darkGreen)
                self.positions_table.item(i, 6).setForeground(Qt.darkGreen)
            elif pnl < 0:
                self.positions_table.item(i, 5).setForeground(Qt.darkRed)
                self.positions_table.item(i, 6).setForeground(Qt.darkRed)


def main():
    app = QApplication(sys.argv)
    window = TradingBotWindow()
    window.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()

