"""
Trading engine that monitors markets and executes trades based on strategy.
"""
import asyncio
import time
from typing import Optional, List
from datetime import datetime
from strategy import MarketData, Position, TradingStrategy, TradeSignal
from polymarket_client import PolymarketClient
from config import BotConfig


class TradingEngine:
    """Core trading engine that monitors and executes trades"""
    
    def __init__(self, config: BotConfig, polymarket_client: PolymarketClient, 
                 strategy: TradingStrategy, log_callback=None):
        self.config = config
        self.client = polymarket_client
        self.strategy = strategy
        self.positions: List[Position] = []
        self.running = False
        self.last_market_data: Optional[MarketData] = None
        self.log_callback = log_callback  # Callback function for UI logging
        self.auto_trade_enabled = True  # Auto-trading enabled by default
        
    def start(self):
        """Start the trading engine"""
        self.running = True
        print("🚀 Trading engine started")
    
    def stop(self):
        """Stop the trading engine"""
        self.running = False
        print("🛑 Trading engine stopped")
    
    def update_market_data(self, market_data: MarketData):
        """Update market data and check for trading opportunities"""
        self.last_market_data = market_data
        
        if not self.running:
            return
        
        # Update existing positions
        for position in self.positions[:]:  # Copy list to allow removal
            should_exit, reason = self.strategy.exit_strategy.should_exit(position, market_data)
            if should_exit:
                self._exit_position(position, reason)
        
        # Check for new entry opportunities (AUTO-BUY/SELL)
        if len(self.positions) < self.config.max_open_positions:
            signal, reason = self.strategy.entry_strategy.should_enter(market_data, self.positions)
            if signal != TradeSignal.NO_SIGNAL:
                # Check if auto-trading is enabled
                if not self.auto_trade_enabled:
                    if self.log_callback:
                        self.log_callback(f"⚠️ Entry signal detected but auto-trading is disabled: {signal.value} - {reason}")
                    return
                self._enter_position(market_data, signal, reason)
    
    def _enter_position(self, market_data: MarketData, signal: TradeSignal, reason: str):
        """Enter a new position - AUTO BUY/SELL"""
        try:
            # Determine which token ID to use
            token_id_up = self.config.token_id_up or self.config.token_id
            token_id_down = self.config.token_id_down
            
            if signal == TradeSignal.BUY:
                token_id = token_id_up
                if not token_id:
                    error_msg = "⚠️  No token_id (UP) configured, cannot enter position"
                    print(error_msg)
                    if hasattr(self, 'log_callback'):
                        self.log_callback(error_msg)
                    return
            else:  # SELL
                token_id = token_id_down or token_id_up  # Fallback to UP token if DOWN not set
                if not token_id:
                    error_msg = "⚠️  No token_id configured, cannot enter position"
                    print(error_msg)
                    if hasattr(self, 'log_callback'):
                        self.log_callback(error_msg)
                    return
            
            # Calculate position size
            try:
                size = self.strategy.entry_strategy.calculate_position_size(
                    market_data, signal, self.config.max_position_size, self.config.min_position_size
                )
            except Exception as e:
                error_msg = f"⚠️  Error calculating position size: {str(e)}"
                print(error_msg)
                if hasattr(self, 'log_callback'):
                    self.log_callback(error_msg)
                return
            
            if size < self.config.min_position_size:
                error_msg = f"⚠️  Position size too small: ${size:.2f}"
                print(error_msg)
                if hasattr(self, 'log_callback'):
                    self.log_callback(error_msg)
                return
            
            # Determine entry price and fair value
            if signal == TradeSignal.BUY:
                entry_price = market_data.market_price_up
                fair_value = market_data.software_price_up
                side = "BUY"
            else:  # SELL
                entry_price = market_data.market_price_down
                fair_value = market_data.software_price_down
                side = "SELL"
            
            # Validate prices
            if entry_price <= 0 or entry_price >= 1:
                error_msg = f"⚠️  Invalid entry price: ${entry_price:.4f}"
                print(error_msg)
                if hasattr(self, 'log_callback'):
                    self.log_callback(error_msg)
                return
            
            # Place order
            order_info = f"""
📈 AUTO {side} ORDER PLACED
   Signal: {signal.value}
   Reason: {reason}
   Side: {side}
   Token ID: {token_id[:20]}...
   Entry Price: ${entry_price:.4f}
   Fair Value: ${fair_value:.4f}
   Size: {size} shares
   Total Cost: ${entry_price * size:.2f}
   Mode: {'SIMULATION' if self.config.use_simulation else 'LIVE'}
"""
            print(order_info)
            if hasattr(self, 'log_callback'):
                self.log_callback(f"📈 AUTO {side}: {reason} | Price: ${entry_price:.4f} | Size: {size} | Total: ${entry_price * size:.2f}")
            
            # Place limit order (use worker thread if available, otherwise direct call)
            try:
                if hasattr(self, 'polymarket_worker') and self.polymarket_worker:
                    # Use worker thread for non-blocking order placement
                    self.polymarket_worker.place_order(token_id, side, entry_price, size)
                    # Assume success for now, actual response will come via callback
                    order_response = True
                else:
                    # Fallback to direct call if worker not available
                    order_response = self.client.place_limit_order(
                        token_id=token_id,
                        side=side,
                        price=entry_price,
                        size=size
                    )
            except Exception as e:
                error_msg = f"❌ ERROR placing {side} order: {type(e).__name__} - {str(e)}"
                print(error_msg)
                if hasattr(self, 'log_callback'):
                    self.log_callback(error_msg)
                return
            
            if order_response:
                # Create position (even in simulation mode)
                position = Position(
                    token_id=token_id,  # Use the correct token_id for this position
                    side=side,
                    entry_price=entry_price,
                    size=size,
                    entry_time=time.time(),
                    fair_value_at_entry=fair_value,
                    current_price=entry_price,
                    current_fair_value=fair_value
                )
                self.positions.append(position)
                
                success_msg = f"✅ {side} order placed successfully! Position opened."
                print(success_msg)
                if hasattr(self, 'log_callback'):
                    self.log_callback(success_msg)
                
                # Log trade
                if self.config.log_trades:
                    self._log_trade("ENTER", position, reason)
            else:
                error_msg = f"❌ Failed to place {side} order - no response from API"
                print(error_msg)
                if hasattr(self, 'log_callback'):
                    self.log_callback(error_msg)
        except Exception as e:
            error_msg = f"❌ CRITICAL ERROR in _enter_position: {type(e).__name__} - {str(e)}"
            print(error_msg)
            import traceback
            print(traceback.format_exc())
            if hasattr(self, 'log_callback'):
                self.log_callback(error_msg)
    
    def _exit_position(self, position: Position, reason: str):
        """Exit a position"""
        print(f"\n📉 Exit Signal")
        print(f"   Reason: {reason}")
        print(f"   Side: {position.side}")
        print(f"   Entry Price: ${position.entry_price:.4f}")
        print(f"   Exit Price: ${position.current_price:.4f}")
        print(f"   P&L: ${position.unrealized_pnl:.2f} ({position.unrealized_pnl_percent:.2f}%)")
        print(f"   Size: {position.size} shares")
        
        # Place exit order (opposite side)
        exit_side = "SELL" if position.side == "BUY" else "BUY"
        
        # Use worker thread if available
        if hasattr(self, 'polymarket_worker') and self.polymarket_worker:
            self.polymarket_worker.place_order(
                position.token_id, exit_side, position.current_price, position.size
            )
            order_response = True
        else:
            order_response = self.client.place_limit_order(
                token_id=position.token_id,
                side=exit_side,
                price=position.current_price,
                size=position.size
            )
        
        if order_response:
            # Log trade
            if self.config.log_trades:
                self._log_trade("EXIT", position, reason)
            
            # Remove position
            if position in self.positions:
                self.positions.remove(position)
    
    def _log_trade(self, action: str, position: Position, reason: str):
        """Log trade to file"""
        try:
            with open(self.config.log_file, "a") as f:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"{timestamp} | {action} | {position.side} | "
                       f"Entry: ${position.entry_price:.4f} | "
                       f"Exit: ${position.current_price:.4f} | "
                       f"P&L: ${position.unrealized_pnl:.2f} | "
                       f"Reason: {reason}\n")
        except Exception as e:
            print(f"Error logging trade: {e}")
    
    def get_status(self) -> dict:
        """Get current engine status"""
        total_pnl = sum(p.unrealized_pnl for p in self.positions)
        return {
            "running": self.running,
            "positions": len(self.positions),
            "total_pnl": total_pnl,
            "positions_detail": [
                {
                    "side": p.side,
                    "entry_price": p.entry_price,
                    "current_price": p.current_price,
                    "pnl": p.unrealized_pnl,
                    "pnl_percent": p.unrealized_pnl_percent
                }
                for p in self.positions
            ]
        }

