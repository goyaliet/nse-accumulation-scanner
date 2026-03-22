# NSE Institutional Accumulation Scanner

Detects institutional buying via NSE delivery data.
Runs daily at 7 PM IST, pushes report to GitHub Pages.

Live: https://goyaliet.github.io/nse-accumulation-scanner/

## Scoring
- Delivery % (35pts): >80%=Strong
- Volume Surge (30pts): >3x=Strong
- Price vs Day High (20pts)
- vs 20DMA (15pts)

## Usage
```bash
cd scanner && python scanner.py
```

Data: NSE sec_bhavdata_full (free). Not investment advice.
