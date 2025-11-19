"""
Trading strategy classes for entry and exit rules.
Modular design allows easy customization and multiple strategies.
"""
from dataclasses import dataclass
from typing import Optional, Literal
from enum import Enum


class TradeSignal(Enum):
    """Trading signals"""
    NO_SIGNAL = "NO_SIGNAL"
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


@dataclass
class MarketData:
    """Market data structure"""
    software_prob_up: float  # Probability from external software (0-100)
    software_prob_down: float  # Probability from external software (0-100)
    market_price_up: float  # Current Polymarket price for YES (0-1)
    market_price_down: float  # Current Polymarket price for NO (0-1)
    symbol: str = ""
    interval: str = ""
    timestamp: float = 0.0
    
    @property
    def software_price_up(self) -> float:
        """Convert software probability to price (0-1)"""
        return self.software_prob_up / 100.0
    
    @property
    def software_price_down(self) -> float:
        """Convert software probability to price (0-1)"""
        return self.software_prob_down / 100.0
    
    @property
    def price_difference_up(self) -> float:
        """Difference between software price and market price for UP (positive when undervalued)"""
        if self.market_price_up < self.software_price_up:
            return self.software_price_up - self.market_price_up
        return 0.0
    
    @property
    def price_difference_down(self) -> float:
        """Difference between software price and market price for DOWN (positive when undervalued)"""
        if self.market_price_down < self.software_price_down:
            return self.software_price_down - self.market_price_down
        return 0.0
    
    @property
    def is_undervalued_up(self) -> bool:
        """Is UP token undervalued (market price < software price)?"""
        return self.market_price_up > 0 and self.software_price_up > 0 and self.market_price_up < self.software_price_up
    
    @property
    def is_undervalued_down(self) -> bool:
        """Is DOWN token undervalued (market price < software price)?"""
        return self.market_price_down > 0 and self.software_price_down > 0 and self.market_price_down < self.software_price_down


@dataclass
class Position:
    """Represents an open trading position"""
    token_id: str
    side: Literal["BUY", "SELL"]
    entry_price: float
    size: float
    entry_time: float
    fair_value_at_entry: float  # Software price when entered
    current_price: float = 0.0
    current_fair_value: float = 0.0
    
    @property
    def unrealized_pnl(self) -> float:
        """Calculate unrealized profit/loss"""
        if self.side == "BUY":
            return (self.current_price - self.entry_price) * self.size
        else:  # SELL
            return (self.entry_price - self.current_price) * self.size
    
    @property
    def unrealized_pnl_percent(self) -> float:
        """Calculate unrealized P&L as percentage"""
        if self.entry_price == 0:
            return 0.0
        if self.side == "BUY":
            return ((self.current_price - self.entry_price) / self.entry_price) * 100
        else:
            return ((self.entry_price - self.current_price) / self.entry_price) * 100
    
    @property
    def profit_target_price(self) -> float:
        """Calculate profit target price"""
        if self.side == "BUY":
            return self.fair_value_at_entry + 0.05  # $0.05 above fair value
        else:
            return self.fair_value_at_entry - 0.05  # $0.05 below fair value


class EntryStrategy:
    """Base class for entry strategies"""
    
    def __init__(self, threshold: float = 0.10):
        self.threshold = threshold
    
    def should_enter(self, market_data: MarketData, current_positions: list[Position]) -> tuple[TradeSignal, str]:
        """
        Determine if we should enter a trade.
        Returns: (signal, reason)
        """
        # Check if we already have a position
        if len(current_positions) > 0:
            return TradeSignal.NO_SIGNAL, "Already have open position"
        
        # Check UP token opportunity
        if market_data.is_undervalued_up and market_data.price_difference_up >= self.threshold:
            return TradeSignal.BUY, f"UP undervalued: diff=${market_data.price_difference_up:.4f}"
        
        # Check DOWN token opportunity
        if market_data.is_undervalued_down and market_data.price_difference_down >= self.threshold:
            return TradeSignal.SELL, f"DOWN undervalued: diff=${market_data.price_difference_down:.4f}"
        
        return TradeSignal.NO_SIGNAL, "No entry opportunity"
    
    def calculate_position_size(self, market_data: MarketData, signal: TradeSignal, 
                                max_size: float, min_size: float) -> float:
        """Calculate position size based on opportunity"""
        if signal == TradeSignal.BUY:
            diff = market_data.price_difference_up
        elif signal == TradeSignal.SELL:
            diff = market_data.price_difference_down
        else:
            return 0.0
        
        # Scale position size based on opportunity size
        # Larger difference = larger position (up to max)
        size_multiplier = min(diff / self.threshold, 2.0)  # Cap at 2x
        size = min_size * size_multiplier
        return min(size, max_size)


class ExitStrategy:
    """Base class for exit strategies"""
    
    def __init__(self, exit_on_fair_value: bool = True, 
                 exit_on_profit_target: bool = True,
                 profit_target: float = 0.05,
                 stop_loss: Optional[float] = None):
        self.exit_on_fair_value = exit_on_fair_value
        self.exit_on_profit_target = exit_on_profit_target
        self.profit_target = profit_target
        self.stop_loss = stop_loss
    
    def should_exit(self, position: Position, market_data: MarketData) -> tuple[bool, str]:
        """
        Determine if we should exit a position.
        Returns: (should_exit, reason)
        """
        # Update position with current data
        if position.side == "BUY":
            position.current_price = market_data.market_price_up
            position.current_fair_value = market_data.software_price_up
        else:
            position.current_price = market_data.market_price_down
            position.current_fair_value = market_data.software_price_down
        
        # Check stop loss
        if self.stop_loss and position.unrealized_pnl_percent <= -abs(self.stop_loss):
            return True, f"Stop loss triggered: {position.unrealized_pnl_percent:.2f}%"
        
        # Check profit target
        if self.exit_on_profit_target:
            if position.side == "BUY":
                target_price = position.fair_value_at_entry + self.profit_target
                if position.current_price >= target_price:
                    return True, f"Profit target reached: ${position.current_price:.4f} >= ${target_price:.4f}"
            else:  # SELL
                target_price = position.fair_value_at_entry - self.profit_target
                if position.current_price <= target_price:
                    return True, f"Profit target reached: ${position.current_price:.4f} <= ${target_price:.4f}"
        
        # Check fair value convergence
        if self.exit_on_fair_value:
            price_diff = abs(position.current_price - position.current_fair_value)
            if price_diff <= 0.02:  # Within 2 cents of fair value
                return True, f"Reached fair value: price=${position.current_price:.4f}, fair=${position.current_fair_value:.4f}"
        
        return False, "Hold position"


class TradingStrategy:
    """Combined entry and exit strategy"""
    
    def __init__(self, entry_strategy: EntryStrategy, exit_strategy: ExitStrategy):
        self.entry_strategy = entry_strategy
        self.exit_strategy = exit_strategy

