"""
DraftKings Sportsbook scraper for MLB pitcher props.

Hits DK's internal v5 API which is unauthenticated and returns full prop
trees as JSON. The endpoint is undocumented but has been stable for years
across multiple open-source projects (betfinder R package, various GH gists).

Structure (as of April 2026):
  /eventgroups/{eg_id}                      -> all events + markets in one tree
  ?category=pitcher-props                   -> filter to pitcher props
  ?subcategory={key}                        -> filter to one prop type

Categories we need:
  strikeouts-thrown   -> Pitcher Strikeouts Over/Under
  outs               -> Pitcher Outs Recorded Over/Under
  earned-runs        -> Pitcher Earned Runs Over/Under
  hits-allowed       -> Pitcher Hits Allowed Over/Under

The same response includes ALL pitchers' lines for the category. We pull
once per category per orchestrator run, cache in-memory for the duration,
join to our pitchers by normalized last_first name.

Failure mode: if DK changes the endpoint or adds auth gating, this returns
{} and the orchestrator falls back to estimate_book_lines() automatically.
We log a warning but never crash.
"""
from __future__ import annotations
import logging
import re
import time
from typing import Optional
import requests

log = logging.getLogger(__name__)

DK_BASE = "https://sportsbook-us-il.draftkings.com/sites/US-IL-SB/api/v5"
MLB_EVENT_GROUP = 84240

# Map our internal category name -> DK subcategory query param
SUBCAT_KEYS = {
    "K":    "strikeouts-thrown",
    "Outs": "outs",
    "ER":   "earned-runs",
    "Hits": "hits-allowed",
}

# Browser-mimicking headers. DK serves the same JSON to all clients but
# blocks obvious bot User-Agents (curl/python-requests).
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://sportsbook.draftkings.com",
    "Referer": "https://sportsbook.draftkings.com/",
}


def _normalize_name(s: str) -> str:
    """
    DK uses 'First Last' format. We use 'Last, First'. Normalize both to
    a stripped lowercase 'last first' for join keys.
    """
    if not s:
        return ""
    if "," in s:
        # 'Crochet, Garrett' -> 'crochet garrett'
        last, _, first = s.partition(",")
        s = f"{last.strip()} {first.strip()}"
    s = re.sub(r"[^a-zA-Z\s]", "", s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    # Drop common suffixes that vary between sources
    for suffix in (" jr", " sr", " ii", " iii", " iv"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    return s


def _american_to_int(price: str | int | None) -> Optional[int]:
    """DK returns prices as strings like '+150' or '-110'. Coerce to int."""
    if price is None:
        return None
    if isinstance(price, int):
        return price
    s = str(price).replace("\u2212", "-").strip()  # unicode minus
    if not s or s in ("EVEN", "EV"):
        return 100
    try:
        return int(s)
    except ValueError:
        return None


def _fetch_subcategory(subcat: str, retries: int = 2,
                       timeout: int = 12) -> Optional[dict]:
    """Fetch one subcategory from DK. Returns parsed JSON or None on failure."""
    url = (f"{DK_BASE}/eventgroups/{MLB_EVENT_GROUP}"
           f"?category=pitcher-props&subcategory={subcat}&format=json")
    last_err: Optional[Exception] = None
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            log.warning("DK %s returned %d", subcat, r.status_code)
            last_err = RuntimeError(f"HTTP {r.status_code}")
        except (requests.RequestException, ValueError) as e:
            last_err = e
            log.warning("DK %s fetch failed: %s", subcat, e)
        if attempt < retries - 1:
            time.sleep(1 + attempt)
    log.warning("DK subcategory %s unavailable (%s)", subcat, last_err)
    return None


def _extract_props_from_payload(payload: dict, category: str) -> dict[str, dict]:
    """
    Walk the DK payload and pull (pitcher_name -> {line, over_price, under_price})
    for one prop category.

    DK schema (v5):
      eventGroup
        offerCategories[]
          offerSubcategoryDescriptors[]
            offerSubcategory
              offers[][]               # one inner list per game
                outcomes[]             # OVER and UNDER outcomes
                  - participant: 'Garrett Crochet'  (pitcher name)
                  - line: 6.5
                  - oddsAmerican: '-110'
                  - label: 'Over' or 'Under'

    Different layouts for nested arrays exist between versions. We try
    multiple paths defensively.
    """
    out: dict[str, dict] = {}
    if not payload:
        return out

    # Paths we've seen DK use. Try each until we find offers.
    candidate_paths = [
        ("eventGroup", "offerCategories", 0, "offerSubcategoryDescriptors", 0,
         "offerSubcategory", "offers"),
        ("offerCategories", 0, "offerSubcategoryDescriptors", 0,
         "offerSubcategory", "offers"),
    ]
    offers_list = None
    for path in candidate_paths:
        try:
            cur = payload
            for key in path:
                cur = cur[key]
            offers_list = cur
            break
        except (KeyError, TypeError, IndexError):
            continue

    if not offers_list:
        # Fallback: walk the tree looking for any 'offers' at depth >= 3
        offers_list = _deep_find_offers(payload)

    if not offers_list:
        log.warning("DK %s: no 'offers' found in payload", category)
        return out

    # offers_list is typically [[offer, offer, ...], [offer, offer, ...]]
    # (one inner list per game). Sometimes it's already flat.
    flat = []
    for inner in offers_list:
        if isinstance(inner, list):
            flat.extend(inner)
        elif isinstance(inner, dict):
            flat.append(inner)

    for offer in flat:
        if not isinstance(offer, dict):
            continue
        outcomes = offer.get("outcomes") or []
        # Each offer has an OVER and UNDER outcome for the same pitcher and line
        pitcher_name = None
        line = None
        over_price = None
        under_price = None
        for outc in outcomes:
            label = (outc.get("label") or "").strip().lower()
            participant = outc.get("participant") or outc.get("playerName")
            if participant and not pitcher_name:
                pitcher_name = participant
            ln = outc.get("line")
            if ln is not None and line is None:
                try:
                    line = float(ln)
                except (TypeError, ValueError):
                    pass
            price = _american_to_int(outc.get("oddsAmerican"))
            if label in ("over", "o", "yes"):
                over_price = price
            elif label in ("under", "u", "no"):
                under_price = price

        if pitcher_name and line is not None:
            key = _normalize_name(pitcher_name)
            out[key] = {
                "line": line,
                "over_price": over_price,
                "under_price": under_price,
                "raw_name": pitcher_name,
            }
    return out


def _deep_find_offers(node, depth: int = 0, max_depth: int = 8):
    """Best-effort recursive walk to locate an 'offers' list anywhere in the tree."""
    if depth > max_depth:
        return None
    if isinstance(node, dict):
        if "offers" in node and isinstance(node["offers"], list):
            return node["offers"]
        for v in node.values():
            found = _deep_find_offers(v, depth + 1, max_depth)
            if found:
                return found
    elif isinstance(node, list):
        for v in node:
            found = _deep_find_offers(v, depth + 1, max_depth)
            if found:
                return found
    return None


def fetch_pitcher_props_for_today() -> dict[str, dict[str, dict]]:
    """
    Fetch all 4 pitcher prop categories from DK and return a unified lookup:

      {
        normalized_name: {
          "K":    {"line": 6.5, "over_price": -110, "under_price": -110},
          "Outs": {"line": 16.5, ...},
          ...
        }
      }

    Pitchers missing a category will have that key absent from their dict.
    Returns {} on total DK failure (falls back to estimate_book_lines elsewhere).
    """
    result: dict[str, dict[str, dict]] = {}
    for cat, subcat in SUBCAT_KEYS.items():
        log.info("DK: fetching %s (%s)", cat, subcat)
        payload = _fetch_subcategory(subcat)
        if not payload:
            continue
        cat_props = _extract_props_from_payload(payload, cat)
        log.info("DK %s: %d pitchers found", cat, len(cat_props))
        for name_key, prop in cat_props.items():
            if name_key not in result:
                result[name_key] = {}
            result[name_key][cat] = prop
    log.info("DK pitcher props: %d unique pitchers across %d categories",
             len(result), len(SUBCAT_KEYS))
    return result


def lookup_lines(pitcher_name: str, dk_lines: dict[str, dict[str, dict]]) -> Optional[dict[str, float]]:
    """
    Given a pitcher's "Last, First" name and the DK lookup, return a dict of
    just the lines (without prices) suitable for plugging into the orchestrator's
    edge calc:

      {"K": 6.5, "Outs": 16.5, "ER": 1.5, "Hits": 5.5}

    Returns None if the pitcher isn't in DK's prop list at all.
    """
    if not pitcher_name or not dk_lines:
        return None
    key = _normalize_name(pitcher_name)
    pitcher_props = dk_lines.get(key)
    if not pitcher_props:
        return None
    out: dict[str, float] = {}
    for cat, prop in pitcher_props.items():
        line = prop.get("line")
        if line is not None:
            out[cat] = float(line)
    return out or None


# CLI for quick verification
if __name__ == "__main__":
    import json as _json
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    props = fetch_pitcher_props_for_today()
    print(f"Found props for {len(props)} pitchers")
    # Print a few samples
    for i, (name, cats) in enumerate(props.items()):
        if i >= 5:
            break
        print(f"\n  {name}:")
        for cat, prop in cats.items():
            print(f"    {cat}: line={prop['line']}, "
                  f"O={prop['over_price']}, U={prop['under_price']}")
