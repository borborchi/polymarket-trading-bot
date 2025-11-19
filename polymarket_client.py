"""
Polymarket API client wrapper for trading operations.
Handles authentication, order placement, and market data fetching.
"""
import sys
import os
from typing import Optional
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, OrderArgs, MarketOrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL
from py_clob_client.exceptions import PolyException


class PolymarketClient:
    """Wrapper for Polymarket CLOB client with trading operations"""
    
    def __init__(self, host: str, private_key: str, chain_id: int,
                 api_key: Optional[str] = None,
                 api_secret: Optional[str] = None,
                 api_passphrase: Optional[str] = None,
                 simulation_mode: bool = False):
        """
        Initialize Polymarket client
        
        Args:
            host: CLOB API URL
            private_key: Private key for signing
            chain_id: Chain ID (80002 for AMOY, 137 for POLYGON)
            api_key: API key (optional, will be created if not provided)
            api_secret: API secret (optional)
            api_passphrase: API passphrase (optional)
            simulation_mode: If True, don't execute real trades
        """
        self.simulation_mode = simulation_mode
        self.host = host
        self.chain_id = chain_id
        
        # Initialize client
        self.client = ClobClient(
            host=host,
            key=private_key,
            chain_id=chain_id
        )
        
        # Set up API credentials
        if api_key and api_secret and api_passphrase:
            creds = ApiCreds(
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_passphrase
            )
            self.client.set_api_creds(creds)
        else:
            # Create or derive API credentials
            try:
                creds = self.client.create_or_derive_api_creds()
                if creds:
                    self.client.set_api_creds(creds)
                    print(f"✅ API credentials set: {creds.api_key[:8]}...")
            except Exception as e:
                print(f"⚠️  Warning: Could not set API credentials: {e}")
                print("   Some features may not work without API credentials")
    
    def get_market_price(self, token_id: str, side: str = "BUY") -> Optional[float]:
        """
        Get current market price for a token
        
        Args:
            token_id: Token ID
            side: "BUY" or "SELL"
        
        Returns:
            Price (0-1) or None if error
        """
        if not token_id or token_id.strip() == "":
            return None
        try:
            response = self.client.get_price(token_id, side)
            if response and isinstance(response, dict) and "price" in response:
                price_str = response["price"]
                if price_str:
                    price = float(price_str)
                    # Validate price is in valid range
                    if 0 < price < 1:
                        return price
                    else:
                        # Price out of range - log for debugging
                        print(f"Warning: Price out of range for token {token_id[:20]}...: {price}")
        except Exception as e:
            # Only log if it's not a 404 (token doesn't exist) - suppress repeated errors
            error_str = str(e)
            if "404" not in error_str and "No orderbook" not in error_str:
                print(f"Error getting market price: {e}")
        return None
    
    def get_midpoint_price(self, token_id: str) -> Optional[float]:
        """
        Get midpoint price for a token
        
        Args:
            token_id: Token ID
        
        Returns:
            Midpoint price (0-1) or None if error
        """
        if not token_id or token_id.strip() == "":
            return None
        try:
            response = self.client.get_midpoint(token_id)
            if response and "mid" in response:
                return float(response["mid"])
        except Exception as e:
            # Only log if it's not a 404 (token doesn't exist) - suppress repeated errors
            error_str = str(e)
            if "404" not in error_str and "No orderbook" not in error_str:
                print(f"Error getting midpoint: {e}")
        return None
    
    def get_orderbook(self, token_id: str):
        """Get orderbook for a token"""
        try:
            return self.client.get_order_book(token_id)
        except Exception as e:
            print(f"Error getting orderbook: {e}")
            return None
    
    def get_best_bid_ask(self, token_id: str) -> Optional[dict]:
        """
        Get best bid and ask prices from orderbook
        
        Returns:
            Dict with 'bid' and 'ask' prices, or None if error
        """
        if not token_id or token_id.strip() == "":
            return None
        try:
            orderbook = self.client.get_order_book(token_id)
            if not orderbook:
                return None
            
            # Extract best bid (highest buy price) and best ask (lowest sell price)
            best_bid = None
            best_ask = None
            
            # OrderBookSummary object has bids and asks as lists of OrderSummary objects
            # OrderSummary has price and size attributes
            try:
                if hasattr(orderbook, 'bids') and orderbook.bids and len(orderbook.bids) > 0:
                    # Get first bid (highest price, sorted by price descending)
                    first_bid = orderbook.bids[0]
                    if hasattr(first_bid, 'price'):
                        price_str = first_bid.price
                        if price_str:
                            price_val = float(price_str)
                            if 0 < price_val < 1:  # Valid price range
                                best_bid = price_val
                    elif isinstance(first_bid, dict):
                        price_val = float(first_bid.get('price', 0))
                        if 0 < price_val < 1:
                            best_bid = price_val
            except (AttributeError, ValueError, IndexError, TypeError) as e:
                pass
            
            try:
                if hasattr(orderbook, 'asks') and orderbook.asks and len(orderbook.asks) > 0:
                    # Get first ask (lowest price, sorted by price ascending)
                    first_ask = orderbook.asks[0]
                    if hasattr(first_ask, 'price'):
                        price_str = first_ask.price
                        if price_str:
                            price_val = float(price_str)
                            if 0 < price_val < 1:  # Valid price range
                                best_ask = price_val
                    elif isinstance(first_ask, dict):
                        price_val = float(first_ask.get('price', 0))
                        if 0 < price_val < 1:
                            best_ask = price_val
            except (AttributeError, ValueError, IndexError, TypeError) as e:
                pass
            
            # Fallback: try dict access if it's a dict
            if best_bid is None or best_ask is None:
                try:
                    if isinstance(orderbook, dict):
                        bids = orderbook.get('bids', [])
                        asks = orderbook.get('asks', [])
                        
                        if bids and len(bids) > 0 and best_bid is None:
                            first_bid = bids[0]
                            if isinstance(first_bid, dict):
                                best_bid = float(first_bid.get('price', 0))
                            elif hasattr(first_bid, 'price'):
                                best_bid = float(first_bid.price)
                            elif isinstance(first_bid, (list, tuple)) and len(first_bid) >= 1:
                                best_bid = float(first_bid[0])
                        
                        if asks and len(asks) > 0 and best_ask is None:
                            first_ask = asks[0]
                            if isinstance(first_ask, dict):
                                best_ask = float(first_ask.get('price', 0))
                            elif hasattr(first_ask, 'price'):
                                best_ask = float(first_ask.price)
                            elif isinstance(first_ask, (list, tuple)) and len(first_ask) >= 1:
                                best_ask = float(first_ask[0])
                except (AttributeError, ValueError, IndexError, TypeError):
                    pass
            
            if best_bid is not None and best_ask is not None:
                return {'bid': best_bid, 'ask': best_ask}
            elif best_bid is not None:
                return {'bid': best_bid, 'ask': None}
            elif best_ask is not None:
                return {'bid': None, 'ask': best_ask}
            
        except Exception as e:
            # Silently fail - will fall back to midpoint or other methods
            pass
        return None
    
    def get_market_price_display(self, token_id: str) -> Optional[float]:
        """
        Get the market price that matches what Polymarket website shows
        This uses the best ask price (what you'd pay to buy)
        """
        if not token_id or token_id.strip() == "":
            return None
        
        # Try multiple methods in order of preference
        # 1. Try get_price API endpoint (most reliable)
        try:
            price_response = self.client.get_price(token_id, "BUY")
            if price_response and isinstance(price_response, dict) and "price" in price_response:
                price_str = price_response["price"]
                if price_str:
                    price = float(price_str)
                    if 0 < price < 1:  # Valid price range
                        return price
        except Exception:
            pass
        
        # 2. Try orderbook best ask
        try:
            bid_ask = self.get_best_bid_ask(token_id)
            if bid_ask and bid_ask.get('ask') is not None:
                ask_price = bid_ask['ask']
                if 0 < ask_price < 1:  # Valid price range
                    return ask_price
        except Exception:
            pass
        
        # 3. Fallback to midpoint
        try:
            midpoint = self.get_midpoint_price(token_id)
            if midpoint and 0 < midpoint < 1:  # Valid price range
                return midpoint
        except Exception:
            pass
        
        return None
    
    def place_limit_order(self, token_id: str, side: str, price: float, 
                         size: float, order_type: OrderType = OrderType.GTC) -> Optional[dict]:
        """
        Place a limit order
        
        Args:
            token_id: Token ID
            side: "BUY" or "SELL"
            price: Order price (0-1)
            size: Order size (number of shares)
            order_type: Order type (GTC, FOK, etc.)
        
        Returns:
            Order response or None if error
        """
        if self.simulation_mode:
            print(f"[SIMULATION] Would place {side} order: {size} shares @ ${price:.4f}")
            return {"simulation": True, "side": side, "price": price, "size": size}
        
        try:
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=side
            )
            
            # Create and sign order
            signed_order = self.client.create_order(order_args)
            
            # Post order
            response = self.client.post_order(signed_order, order_type)
            print(f"✅ Order placed: {side} {size} shares @ ${price:.4f}")
            return response
        except PolyException as e:
            error_msg = str(e)
            print(f"❌ Polymarket error placing order: {e}")
            # Re-raise to allow caller to handle it
            raise
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Error placing order: {e}")
            # Re-raise to allow caller to handle it
            raise
    
    def place_market_order(self, token_id: str, side: str, amount: float,
                          order_type: OrderType = OrderType.FOK) -> Optional[dict]:
        """
        Place a market order
        
        Args:
            token_id: Token ID
            side: "BUY" or "SELL"
            amount: For BUY: USD amount, For SELL: number of shares
            order_type: Order type (FOK, FAK, etc.)
        
        Returns:
            Order response or None if error
        """
        if self.simulation_mode:
            print(f"[SIMULATION] Would place market {side} order: ${amount:.2f}")
            return {"simulation": True, "side": side, "amount": amount}
        
        try:
            order_args = MarketOrderArgs(
                token_id=token_id,
                side=side,
                amount=amount,
                order_type=order_type
            )
            
            # Create and sign order
            signed_order = self.client.create_market_order(order_args)
            
            # Post order
            response = self.client.post_order(signed_order, order_type)
            print(f"✅ Market order placed: {side} ${amount:.2f}")
            return response
        except PolyException as e:
            print(f"❌ Polymarket error placing market order: {e}")
            return None
        except Exception as e:
            print(f"❌ Error placing market order: {e}")
            return None
    
    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order"""
        if self.simulation_mode:
            print(f"[SIMULATION] Would cancel order: {order_id}")
            return True
        
        try:
            self.client.cancel(order_id)
            print(f"✅ Order cancelled: {order_id}")
            return True
        except Exception as e:
            print(f"❌ Error cancelling order: {e}")
            return False
    
    def get_open_orders(self, token_id: Optional[str] = None):
        """Get open orders"""
        try:
            params = None
            if token_id:
                from py_clob_client.clob_types import OpenOrderParams
                params = OpenOrderParams(asset_id=token_id)
            return self.client.get_orders(params)
        except Exception as e:
            print(f"Error getting open orders: {e}")
            return []
    
    def get_balance(self):
        """Get account balance"""
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            response = self.client.get_balance_allowance(params)
            return response
        except Exception as e:
            print(f"Error getting balance: {e}")
            return None

