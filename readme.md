# How to Find YES/NO Token IDs from Polymarket URLs

## Quick Method: Use the Script

For the URL: `https://polymarket.com/event/bitcoin-up-or-down-november-18-4am-et?tid=1763455496480`

Run:
```bash
python find_token_id.py --url "https://polymarket.com/event/bitcoin-up-or-down-november-18-4am-et?tid=1763455496480"
```

This will automatically extract the token IDs from the market.

<iframe src="https://drive.google.com/file/d/1gAo5CZW9WZmlBhX6P3rvtnkbpYuLLkZW/preview" width="640" height="360" allowfullscreen></iframe>


## Alternative Methods

### Method 1: Browser Developer Tools (Most Reliable)

1. **Open the Polymarket market page** in your browser
   - Example: https://polymarket.com/event/bitcoin-up-or-down-november-18-4am-et

2. **Open Developer Tools**
   - Press `F12` (or `Ctrl+Shift+I` on Windows/Linux, `Cmd+Option+I` on Mac)
   - Or right-click → "Inspect"

3. **Go to Network Tab**
   - Click on the "Network" tab in Developer Tools

4. **Refresh the Page**
   - Press `F5` to refresh
   - This will show all network requests

5. **Find API Calls**
   - Look for requests to:
     - `gamma-api.polymarket.com`
     - `clob.polymarket.com`
   - Filter by "XHR" or "Fetch" if needed

6. **Inspect the Response**
   - Click on a request (usually one with "markets" or "event" in the name)
   - Go to the "Response" or "Preview" tab
   - Look for `tokenId` or `token_id` fields
   - You'll see something like:
     ```json
     {
       "outcomes": [
         {
           "outcome": "Yes",
           "tokenId": "71321045679252212594626385532706912750332728571942532289631379312455583992563"
         },
         {
           "outcome": "No", 
           "tokenId": "52114319501245915516055106046884209969926127482827954674443846427813813222426"
         }
       ]
     }
     ```

7. **Copy the Token ID**
   - Copy the long number (70+ digits) for "Yes" or "Up" outcome
   - Use this in your bot's Configuration tab

### Method 2: View Page Source

1. **Right-click on the market page** → "View Page Source" (or `Ctrl+U`)

2. **Search for Token ID**
   - Press `Ctrl+F` (or `Cmd+F` on Mac)
   - Search for: `tokenId` or `token_id`

3. **Find the Long Number**
   - Look for numbers that are 70+ digits long
   - These are the token IDs

### Method 3: Use Polymarket API Explorer

1. Visit: https://docs.polymarket.com/developers/gamma-markets-api/get-markets

2. Use the API Explorer to search for markets

3. Find your market and extract token IDs from the response

### Method 4: Search Markets with Script

If you know part of the market name:

```bash
python find_token_id.py --search "bitcoin"
```

This will show all markets containing "bitcoin" in the name.

## Understanding Token IDs

- **YES/UP Token ID**: Use this for buying UP tokens (when you think price will go up)
- **NO/DOWN Token ID**: Use this for buying DOWN tokens (when you think price will go down)

For your bot:
- If your software says "UP = 85%" and market price is lower → Use **YES token ID**
- If your software says "DOWN = 85%" and market price is lower → Use **NO token ID**

## Example

For the Bitcoin market you shared:
- Market: "Bitcoin Up or Down - November 18, 4AM ET"
- YES token ID: (the long number for "Yes" outcome)
- NO token ID: (the long number for "No" outcome)

**In your bot:**
1. Go to Configuration tab
2. Paste the YES token ID in "Token ID" field (for UP trades)
3. Save configuration
4. Start the bot

The bot will automatically:
- Convert your software's `prob_up` (e.g., 85%) to price ($0.85)
- Fetch market price from Polymarket
- Compare and trade when difference ≥ threshold

## Troubleshooting

**If script doesn't find the market:**
- The market might be new or not in the API yet
- Use Method 1 (Browser Developer Tools) - it always works
- Check if the market URL is correct

**If token ID doesn't work:**
- Make sure you copied the full token ID (all digits)
- Verify the market is still active
- Check if you're using the correct chain (POLYGON mainnet vs AMOY testnet)

