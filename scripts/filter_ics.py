#!/usr/bin/env python3
# coding: utf-8
"""
QGenda → two cleaned ICS calendars (Outlook timed + MagicMirror all-day)

- Applies title renames and exclusions
- Pairwise same-day rule (e.g., if "EAA Late" present, drop "EAA" that day)
- Optional per-day conflict resolver (keep one by priority)
- Builds:
    docs/outlook.ics     (timed per time_rules; default = all-day)
    docs/magicmirror.ics (all-day)

ENV:
  SOURCE_ICS_URL  : QGenda subscription ICS URL
  OUTPUT_OUTLOOK  : path for timed ICS (default docs/outlook.ics)
  OUTPUT_MAGIC    : path for all-day ICS (default docs/magicmirror.ics)
  RULES_JSON      : JSON config (exclude/rename/pairwise/time_rules, etc.)
"""

import os
import sys
import json
import re
import logging
import copy
from collections import defaultdict
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from icalendar import Calendar, Event, vDate, vText

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

SOURCE_ICS_URL = os.environ.get("SOURCE_ICS_URL", "").strip()
OUTPUT_OUTLOOK = os.environ.get("OUTPUT_OUTLOOK", "docs/outlook.ics")
OUTPUT_MAGIC   = os.environ.get("OUTPUT_MAGIC",   "docs/magicmirror.ics")
RULES_JSON     = os.environ.get("RULES_JSON", "").strip()

DEFAULT_RULES = {
    "timezone": "America/Detroit",
    "exclude": [],
    "rename": [],
    "day_conflicts": {
        "enable": True,
        "keep_priority": [],
        "allow_multiple_per_day": True
    },
    "pairwise_same_day": [],
    "time_rules": [],
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
        if "pairwise_same_day" in rules and not isinstance(rules["pairwise_same_day"], list):
            raise ValueError("pairwise_same_day must be a list")
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

def compile_matcher(pattern: str):
    if not pattern:
        return lambda s: False
    p = pattern.strip()
    if p.lower().startswith("re:"):
        rx = re.compile(p[3:], re.IGNORECASE)
        return lambda s, rx=rx: bool(rx.search(s or ""))
    low = p.casefold()
    return lambda s, low=low: low in (s or "").casefold()

def compile_matchers(patterns_or_rules, key="pattern"):
    matchers = []
    for item in patterns_or_rules:
        if isinstance(item, str):
            p = item.strip()
        else:
            p = str(item.get(key, "")).strip()
        matchers.append((compile_matcher(p), item))
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
    # normalize spaces/dashes
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

def build_base_calendar(src: Calendar, name: str | None = None) -> Calendar:
    out = Calendar()
    for key in ["PRODID", "VERSION", "CALSCALE", "METHOD", "X-WR-TIMEZONE"]:
        if key in src:
            out[key] = src[key]
    if name:
        out["X-WR-CALNAME"] = name
    return out

def add_timed_with_tz(ev: Event, field: str, dt: datetime, tzname: str):
    """
    Add a timed field with explicit TZID param so Outlook renders reliably:
      DTSTART;TZID=America/Detroit:20251128T070000
    """
    # Remove any existing field to avoid leftover VALUE=DATE params
    if field in ev:
        del ev[field]
    # Format local "floating" wall time with explicit TZID param
    # icalendar will format as YYYYMMDDTHHMMSS by default.
    ev.add(field.lower(), dt, parameters={"TZID": vText(tzname)})

def set_event_times_local_outlook(ev: Event, d: date, tzname: str, start_hhmm: str, end_hhmm: str):
    tz = ZoneInfo(tzname)
    sh, sm = map(int, start_hhmm.split(":"))
    eh, em = map(int, end_hhmm.split(":"))
    start_dt = datetime(d.year, d.month, d.day, sh, sm, tzinfo=tz)
    end_dt   = datetime(d.year, d.month, d.day, eh, em, tzinfo=tz)
    if end_dt <= start_dt:
        end_dt += timedelta(days=1)

    add_timed_with_tz(ev, "DTSTART", start_dt, tzname)
    add_timed_with_tz(ev, "DTEND",   end_dt,   tzname)

    # Clean up all-day flags that confuse Outlook
    if "X-MICROSOFT-CDO-ALLDAYEVENT" in ev:
        del ev["X-MICROSOFT-CDO-ALLDAYEVENT"]
    # Set explicit non-all-day hints Outlook understands
    ev["X-MICROSOFT-CDO-ALLDAYEVENT"] = "FALSE"
    ev["X-MICROSOFT-CDO-BUSYSTATUS"] = "BUSY"
    ev["TRANSP"] = "OPAQUE"

def set_event_all_day(ev: Event, d: date):
    """
    True all-day event (DATE-only) for GCal/Outlook:
      DTSTART;VALUE=DATE:YYYYMMDD
      DTEND;VALUE=DATE:YYYYMMDD+1
    """
    ev["DTSTART"] = vDate(d)
    ev["DTEND"]   = vDate(d + timedelta(days=1))
    # Mark as all-day for Outlook (optional but harmless)
    ev["X-MICROSOFT-CDO-ALLDAYEVENT"] = "TRUE"
    ev["X-MICROSOFT-CDO-BUSYSTATUS"] = "FREE"
    ev["TRANSP"] = "TRANSPARENT"

def main():
    if not SOURCE_ICS_URL:
        logging.error("SOURCE_ICS_URL is not set.")
        sys.exit(1)

    rules = load_rules()
    tzname = rules.get("timezone") or "America/Detroit"

    exclude_matchers = compile_matchers(rules.get("exclude", []))
    rename_rules = rules.get("rename", [])

    dc = rules.get("day_conflicts", {})
    dc_enable  = bool(dc.get("enable", True))
    allow_multi = bool(dc.get("allow_multiple_per_day", True))
    scorer = make_priority_scorer(dc.get("keep_priority", []))

    pairwise = rules.get("pairwise_same_day", []) or []
    pairwise_compiled = []
    for pr in pairwise:
        keep_m = compile_matcher(str(pr.get("keep", "")))
        drop_m = compile_matcher(str(pr.get("drop", "")))
        pairwise_compiled.append((keep_m, drop_m))

    time_rule_matchers = compile_matchers(rules.get("time_rules", []))
    def find_time_rule(summary: str):
        for test, item in time_rule_matchers:
            if test(summary):
                return item
        return None

    logging.info("Downloading source ICS…")
    raw = fetch_ics(SOURCE_ICS_URL)
    src = Calendar.from_ical(raw)

    # pass 1: rename + exclude
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

    # group by local date
    by_date = defaultdict(list)
    for ev in kept:
        d = as_local_date(ev.get("DTSTART").dt, tzname)
        by_date[d].append(ev)

    # pairwise same-day drops (e.g., keep "EAA Late" drop "EAA")
    dropped_by_pairwise = 0
    kept_after_pairwise = []
    for d, evs in by_date.items():
        to_drop = set()
        titles = [str(e.get("SUMMARY", "") or "") for e in evs]
        for keep_m, drop_m in pairwise_compiled:
            has_keep = any(keep_m(t) for t in titles)
            if not has_keep:
                continue
            for idx, t in enumerate(titles):
                if drop_m(t):
                    to_drop.add(idx)
        for idx, ev in enumerate(evs):
            if idx not in to_drop:
                kept_after_pairwise.append(ev)
        dropped_by_pairwise += len(to_drop)
    kept = kept_after_pairwise

    # optional classic one-per-day resolver
    dropped_by_conflict = 0
    if dc_enable and not allow_multi:
        by_date2 = defaultdict(list)
        for ev in kept:
            d = as_local_date(ev.get("DTSTART").dt, tzname)
            by_date2[d].append(ev)
        final = []
        for d, evs in by_date2.items():
            if len(evs) == 1:
                final.append(evs[0])
                continue
            best_ev, best_score = None, 10**9
            for ev in evs:
                s = str(ev.get("SUMMARY", "") or "")
                sc = scorer(s)
                if sc < best_score:
                    best_score, best_ev = sc, ev
            if best_ev is None:
                evs_sorted = sorted(evs, key=lambda e: (as_local_date(e.get("DTSTART").dt, tzname), e.get("DTSTART").dt))
                best_ev = evs_sorted[0]
            final.append(best_ev)
            dropped_by_conflict += (len(evs) - 1)
        kept = final

    # build output calendars
    cal_out = build_base_calendar(src, rules["calendar_names"].get("outlook"))
    cal_mm  = build_base_calendar(src, rules["calendar_names"].get("magicmirror"))

    for ev in kept:
        d = as_local_date(ev.get("DTSTART").dt, tzname)
        title = str(ev.get("SUMMARY", "") or "")

        # --- Outlook (timed if rule matches; else all-day) ---
        ev_out = copy.deepcopy(ev)
        rule = find_time_rule(title)
        if rule and rule.get("start") and rule.get("end"):
            set_event_times_local_outlook(ev_out, d, tzname, rule["start"], rule["end"])
        else:
            # Default to all-day in Outlook when no time rule
            # (ensure any stray timed params/flags are cleared)
            if "DTSTART" in ev_out: del ev_out["DTSTART"]
            if "DTEND" in ev_out:   del ev_out["DTEND"]
            set_event_all_day(ev_out, d)
        cal_out.add_component(ev_out)

        # --- MagicMirror (always all-day) ---
        ev_mm = copy.deepcopy(ev)
        # ensure clean DATE values
        if "DTSTART" in ev_mm: del ev_mm["DTSTART"]
        if "DTEND" in ev_mm:   del ev_mm["DTEND"]
        set_event_all_day(ev_mm, d)
        cal_mm.add_component(ev_mm)

    # write files
    os.makedirs(os.path.dirname(OUTPUT_OUTLOOK), exist_ok=True)
    with open(OUTPUT_OUTLOOK, "wb") as f:
        f.write(cal_out.to_ical())

    os.makedirs(os.path.dirname(OUTPUT_MAGIC), exist_ok=True)
    with open(OUTPUT_MAGIC, "wb") as f:
        f.write(cal_mm.to_ical())

    logging.info(f"Renamed: {renamed_count}")
    logging.info(f"Excluded by rule: {skipped_by_exclude}")
    logging.info(f"Dropped by pairwise: {dropped_by_pairwise}")
    logging.info(f"Dropped by conflict: {dropped_by_conflict}")
    logging.info(f"Wrote: {OUTPUT_OUTLOOK} and {OUTPUT_MAGIC}")

if __name__ == "__main__":
    main()
