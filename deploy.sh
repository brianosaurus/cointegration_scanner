#!/bin/bash

tar czf - --no-xattrs *.py dex/*.py scripts/*.py | ssh frankfurt "cd cointegration_scanner && tar xzf -"
ssh frankfurt "cd cointegration_scanner; source venv/bin/activate; python3 scanner.py --follow --duration 10" | tee /tmp/cointegration_scanner.log
