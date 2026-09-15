# Electronics clean-market registry

Frozen **manual** audit of Final Market names. Replay does not re-judge cleanliness.

- Registry: `clean_market_registry.csv` (657 CLEAN rows)
- Match key: `market_name` (`accepted_cases.market_label`)
- Apply: `python scripts/apply_clean_market_registry.py --quality-dir ... --registry ... --output-dir ...`

`657` = markets marked CLEAN by name-only conservative review.  
`381` = CLEAN markets that still have Quality-accepted Cases.
