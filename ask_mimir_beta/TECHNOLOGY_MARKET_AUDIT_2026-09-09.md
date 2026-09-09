# Ask Mimir technology-market evidence audit

Date: 2026-09-09

## Purpose

This audit tests whether each governed technology-market route has a defensible product boundary and enough evidence to support a useful answer. It does not treat observed procurement as total market size.

## Results

| Capability | Boundary | Matching NIINs | US supplier sites | Observed DLA value | Directly classified prime obligations | Assessment |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Aircraft actuation and flight controls | FSC 1650/1680 plus actuation descriptions | 6,822 | 349 | $98.3M | $1.002B | Usable bounded market; classification includes adjacent hydraulic and aircraft components. |
| Aircraft braking | Complete FSC 1630 | 5,942 | 282 | $378.6M | $1.821B | Strong classification-backed market. |
| Aircraft electrical power | Electrical FSCs plus aircraft power descriptions | 141 | 20 | $2.4M | $46.8M | Useful bounded supplier view; not a complete electrical-power market. |
| Aircraft environmental control | Complete FSC 1660 | 24,648 | 679 | $513.8M | $849.3M | Strong classification-backed ecosystem. |
| Aircraft landing gear | Complete FSC 1620 | 10,950 | 404 | $191.7M | $1.441B | Strong classification-backed market. |
| Aviation fuel controls | Description-confirmed core plus wider FSC 2915 ecosystem | 6,222 core | 292 core | $86.0M core | $366.7M core | Strong hybrid view when the core and wider ecosystem remain separate. |
| Electro-optical and infrared equipment | Complete FSC 5855 plus matched FSC 1240 records | 5,878 | 371 | $157.2M | $4.360B | Strong hybrid view; adjacent optical equipment remains description-confirmed. |
| Electronic warfare | Complete FSC 5865 | 7,739 | 311 | $46.4M | $17.036B | Strong classification-backed market; award values can include integrated systems and services. |
| Energetic and initiation components | Complete FSC 1375/1376/1390 | 2,276 | 62 | $0.5M | $6.015B | Strong award evidence; DLA item procurement is a small supporting lane. |
| Military antennas and RF equipment | Complete FSC 5985 plus matched adjacent RF records | 73,631 | 1,830 | $438.4M | $2.583B | Broad classification-backed antenna ecosystem with description-confirmed adjacent RF equipment. |
| Military avionics | Complete airborne avionics classes plus matched adjacent electronics | 33,332 | 855 | $259.2M | $7.092B | Broad hybrid view; award descriptions determine inclusion from adjacent electronics classes. |
| Military radar | Complete FSC 1285/5840/5841 | 21,050 | 683 | $36.6M | $28.594B | Strong classification-backed market; distinguish fire-control, airborne and ground/shipboard lanes in prose. |
| Missile propulsion | Complete FSC 1338/2845 plus related award descriptions | 216 | 7 | $200 | $657.8M | Strong award evidence but sparse item-procurement evidence; answer should rely on awards and authoritative program sources. |
| Mission computing | Computing FSCs plus exact military mission-computer descriptions | 6 | 1 | $0 | $0.5M | Bounded exact-match evidence only; not a comprehensive market universe. |
| Tactical communications | Communications FSCs plus tactical descriptions | 2,413 | 160 | $2.8M | $317.9M | Useful bounded supplier view; excludes general communications activity. |

## Aviation fuel-control correction

The narrower evidence set contains 6,222 matched NIINs and 292 US supplier sites. The wider FSC 2915 ecosystem contains 24,009 classified NIINs, with 4,405 NIINs represented by an active-authorized or observed US supplier relationship across 519 commercial sites and $420.7M of observed DLA procurement.

FSC 2915 covers engine fuel-system components for aircraft and missile prime movers. Its full universe is therefore useful context but is not a pure aviation fuel-control market. Ask Mimir must present the description-confirmed core first and the broader FSC ecosystem separately.

## Remaining weakness

Unknown capability questions still use dynamic description and classification matching. That route is suitable for evidence discovery, not a comprehensive market-size or supplier-share claim. Mission computing demonstrates the limitation: broad computing codes create distributor noise, while exact military terms create a small but high-quality evidence set.

The governed ontology now provides stable inclusion rules and evidence modes for the fifteen families above. The next expansion should add reviewed exclusion rules, representative NIIN tests and authoritative product sources for the narrow description-bounded families before any market-size or market-share metric is introduced.

## Financial control correction

The first audit used an aggregate value carried on the NSN/CAGE reference catalogue. That field repeated NIIN-level procurement totals across multiple approved-source and part-number relationships and therefore overstated capability totals. The corrected figures above use `nsn_supplier_lookup.parquet`, which is generated from the deduplicated DLA financial view and keeps each observed amount attached to the CAGE recipient of the procurement award. FLIS relationships now establish item and authorization evidence only.
