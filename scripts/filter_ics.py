#!/usr/bin/env python3
import os, sys, json, re, logging, copy
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import requests
from icalendar import Calendar, Event, vCalAddress, vText

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

SOURCE_ICS_URL = os.environ.get("SOURCE_ICS_URL", "").strip()
OUTPUT_OUTLOOK = os.environ.get("OUTPUT_OUTLOOK", "docs/outlook.ics")
OUTPUT_MAGIC = os.environ.get("OUTPUT_MAGIC", "docs/magicmirror.ics")
RULES_JSON = os.environ.get("RULES_JSON", "").strip()

DEFAULT_RULES = {
    "timezone": "America/Detroit",
    "exclude": [],                   # ["Admin Flex", "PTO", "re:\\bShadow\\b"]
    "rename": [],                    # [{"pattern": "^Facility\\s+", "repl": ""}]
    "day_conflicts": {
        "enable": True,
        "keep_priority": [],         # ["OR 1st Call", "EAA Late", ...]
        "allow_multiple_per_day": False
    },
    "time_rules": [                  # For Outlook calendar ONLY
        # {"pattern": "EAA Late", "start": "06:00", "end": "21:00"}
    ],
    "calendar_names": {
        "outlook": "QGenda (Outlook, filtered)",
        "magicmirror": "QGenda (MagicMirror, all-day, filtered)"
    }
}

def load_rules():
    if not RULES_JSON:
        logging.warning("RULES_JSON not set; using defaults.")
        return DEFAULT_RULES
    try:
        rules = json.loads(RULES_JSON)
        merged = DEFAULT_RULES | rules
        if "day_conflicts" in rules:
            merged["day_conflicts"] = DEFAULT_RULES["day_conflicts"] | rules["day_conflicts"]
        if "calendar_names" in rules:
            merged["calendar_names"] = DEFAULT_RULES["calendar_names"] | rules["calendar_names"]
        return merged
    except Exception as e:
        logging.error(f"Failed to parse RULES_JSON: {e}")
        sys.exit(1)

def fetch_ics(url: str) -> bytes:
    r = requests.get(url, timeout=45)
    r.raise_for_status()
    return r.content

def as_local_date(dt_value, tzname: str) -> date:
    tz = ZoneInfo(tzname)
    if isinstance(dt_value, datetime):
        if dt_value.tzinfo is None:
            dt_value = dt_value.replace(tzinfo=tz)
        return dt_value.astimezone(tz).date()
    elif isinstance(dt_value, date):
        return dt_value
    else:
        raise TypeError("Unsupported DTSTART/DTEND type")

def compile_matchers(patterns_or_rules, key="pattern"):
    """
    Accepts list of strings or dicts with a 'pattern' key.
    For strings: plain substring (case-insensitive) unless prefixed with 're:'.
    For dicts: read dict[key]; same 're:' handling.
    Returns list of (callable, original_item).
    """
    matchers = []
    for item in patterns_or_rules:
        if isinstance(item, str):
            p = item.strip()
        else:
            p = str(item.get(key, "")).strip()
        if not p:
            # skip empty
            matchers.append((lambda s: False, item))
            continue
        if p.lower().startswith("re:"):
            rx = re.compile(p[3:], re.IGNORECASE)
            matchers.append((lambda s, rx=rx: bool(rx.search(s or "")), item))
        else:
            low = p.casefold()
            matchers.append((lambda s, low=low: low in (s or "").casefold(), item))
    return matchers

def apply_renames(summary: str, rename_rules: list[dict]) -> str:
    out = summary or ""
    for rule in rename_rules:
        pattern = rule.get("pattern", "")
        repl = rule.get("repl", "")
        if not pattern:
            continue
        rx = re.compile(pattern, re.IGNORECASE)
        out = rx.sub(repl, out)
    # tidy spaces/dashes
    out = re.sub(r"\s{2,}", " ", out).strip(" -\u2013\u2014")
    return out

def make_priority_scorer(priority_list):
    compiled = []
    for idx, p in enumerate(priority_list):
        if p.lower().startswith("re:"):
            compiled.append((idx, re.compile(p[3:], re.IGNORECASE), True))
        else:
            compiled.append((idx, p.casefold(), False))
    def score(summary: str) -> int:
        s = (summary or "").casefold()
        best = 10**9
        for idx, patt, is_rx in compiled:
            if is_rx:
                if patt.search(summary or ""):
                    return idx
            else:
                if patt in s:
                    return idx
        return best
    return score

def clone_event(ev: Event) -> Event:
    return copy.deepcopy(ev)

def build_base_calendar(src: Calendar, name: str | None = None) -> Calendar:
    out = Calendar()
    for key in ["PRODID", "VERSION", "CALSCALE", "METHOD", "X-WR-TIMEZONE"]:
        if key in src:
            out[key] = src[key]
    if name:
        out["X-WR-CALNAME"] = name
    return out

def set_event_times_local(ev: Event, d: date, tzname: str, start_hhmm: str, end_hhmm: str):
    tz = ZoneInfo(tzname)
    sh, sm = map(int, start_hhmm.split(":"))
    eh, em = map(int, end_hhmm.split(":"))
    start_dt = datetime(d.year, d.month, d.day, sh, sm, tzinfo=tz)
    end_dt = datetime(d.year, d.month, d.day, eh, em, tzinfo=tz)
    # Guard: if end <= start, push to next day (rare, but safe)
    if end_dt <= start_dt:
        end_dt += timedelta(days=1)
    ev["DTSTART"] = start_dt
    ev["DTEND"] = end_dt

def set_event_all_day(ev: Event, d: date):
    # All-day events: DTSTART is DATE; DTEND is next DATE (exclusive)
    ev["DTSTART"] = d
    ev["DTEND"] = d + timedelta(days=1)
    # Remove any DTSTAMP/UID? Keep UID; GCal handles fine.

def main():
    if not SOURCE_ICS_URL:
        logging.error("SOURCE_ICS_URL is not set.")
        sys.exit(1)

    rules = load_rules()
    tzname = rules.get("timezone") or "America/Detroit"

    # Matchers
    exclude_matchers = compile_matchers(rules.get("exclude", []))
    rename_rules = rules.get("rename", [])
    dc = rules.get("day_conflicts", {})
    dc_enable = bool(dc.get("enable", True))
    allow_multi = bool(dc.get("allow_multiple_per_day", False))
    scorer = make_priority_scorer(dc.get("keep_priority", []))

    time_rule_matchers = compile_matchers(rules.get("time_rules", []))  # for Outlook
    # make a helper to find first matching time rule
    def find_time_rule(summary: str):
        for test, item in time_rule_matchers:
            if test(summary):
                return item
        return None

    logging.info("Downloading source ICS…")
    raw = fetch_ics(SOURCE_ICS_URL)
    src = Calendar.from_ical(raw)

    # First pass: collect events, apply renames/excludes
    kept = []
    skipped_by_exclude = 0
    renamed_count = 0
    for ev in src.walk("VEVENT"):
        summary = str(ev.get("SUMMARY", "") or "")
        new_summary = apply_renames(summary, rename_rules)
        if new_summary != summary:
            ev["SUMMARY"] = new_summary
            renamed_count += 1
        if any(test(new_summary) for test, _ in exclude_matchers):
            skipped_by_exclude += 1
            continue
        kept.append(ev)

    # Resolve per-day conflicts (date in chosen timezone)
    dropped_by_conflict = 0
    if dc_enable and not allow_multi:
        by_date = defaultdict(list)
        for ev in kept:
            dt = ev.get("DTSTART").dt
            d = as_local_date(dt, tzname)
            by_date[d].append(ev)
        kept_final = []
        for d, evs in by_date.items():
            if len(evs) == 1:
                kept_final.append(evs[0])
                continue
            best_ev = None
            best_score = 10**9
            for ev in evs:
                s = str(ev.get("SUMMARY", "") or "")
                sc = scorer(s)
                if sc < best_score:
                    best_score = sc
                    best_ev = ev
            if best_ev is None:
                evs_sorted = sorted(evs, key=lambda e: (as_local_date(e.get("DTSTART").dt, tzname), e.get("DTSTART").dt))
                best_ev = evs_sorted[0]
            kept_final.append(best_ev)
            dropped_by_conflict += (len(evs) - 1)
        kept = kept_final

    # Build Outlook calendar (timed events) and MagicMirror (all-day)
    cal_outlook = build_base_calendar(src, rules["calendar_names"].get("outlook"))
    cal_magic = build_base_calendar(src, rules["calendar_names"].get("magicmirror"))

    for ev in kept:
        # Compute the local calendar date for assignment
        d = as_local_date(ev.get("DTSTART").dt, tzname)
        summary = str(ev.get("SUMMARY", "") or "")

        # --- Outlook version ---
        ev_out = clone_event(ev)
        rule = find_time_rule(summary)
        if rule and rule.get("start") and rule.get("end"):
            set_event_times_local(ev_out, d, tzname, rule["start"], rule["end"])
        else:
            # If no rule, keep original timing as-is
            # But ensure timezone-awareness if naive
            dt = ev_out.get("DTSTART").dt
            if isinstance(dt, datetime) and dt.tzinfo is None:
                ev_out["DTSTART"] = dt.replace(tzinfo=ZoneInfo(tzname))
            dtend = ev_out.get("DTEND")
            if dtend:
                dt2 = dtend.dt
                if isinstance(dt2, datetime) and dt2.tzinfo is None:
                    ev_out["DTEND"] = dt2.replace(tzinfo=ZoneInfo(tzname))
        cal_outlook.add_component(ev_out)

        # --- MagicMirror version (all-day) ---
        ev_mm = clone_event(ev)
        set_event_all_day(ev_mm, d)
        cal_magic.add_component(ev_mm)

    # Write both files
    os.makedirs(os.path.dirname(OUTPUT_OUTLOOK), exist_ok=True)
    with open(OUTPUT_OUTLOOK, "wb") as f:
        f.write(cal_outlook.to_ical())

    os.makedirs(os.path.dirname(OUTPUT_MAGIC), exist_ok=True)
    with open(OUTPUT_MAGIC, "wb") as f:
        f.write(cal_magic.to_ical())

    logging.info(f"Renamed: {renamed_count}")
    logging.info(f"Excluded by rule: {skipped_by_exclude}")
    logging.info(f"Dropped by day-conflict: {dropped_by_conflict}")
    logging.info(f"Wrote: {OUTPUT_OUTLOOK} and {OUTPUT_MAGIC}")

if __name__ == "__main__":
    main()
