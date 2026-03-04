#!/bin/bash
set -e

tar czf - --no-xattrs *.py requirements.txt | ssh frankfurt "cd cointegration_scanner && tar xzf -"
ssh frankfurt "cd cointegration_scanner; source venv/bin/activate; pip install -q -r requirements.txt && python3 scanner.py --scan --db ../arbitrage_tracker/arb_tracker.db --resample 5min --min-observations 20 --max-pairs 2000" | tee /tmp/cointegration_scanner.log
