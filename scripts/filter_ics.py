#!/usr/bin/env python3
# coding: utf-8
"""
QGenda → two cleaned ICS calendars (Outlook timed + MagicMirror all-day)

- Title renames & exclusions
- Pairwise same-day rule (keep "EAA Late", drop exact "EAASC" only when both exist)
- Optional per-day conflict resolver (keep one by priority)
- Rewrites UIDs per output calendar to avoid cross-calendar de-duplication
- Outputs:
    docs/outlook.ics     (timed per time_rules; DEFAULT = all-day, BUSY/OPAQUE)
    docs/magicmirror.ics (all-day VALUE=DATE, BUSY/OPAQUE)
"""

import os, sys, json, re, logging, copy, hashlib
from collections import defaultdict
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from icalendar import Calendar, Event, vDate

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

SOURCE_ICS_URL = os.environ.get("SOURCE_ICS_URL", "").strip()
OUTPUT_OUTLOOK = os.environ.get("OUTPUT_OUTLOOK", "docs/outlook_v2.ics")
OUTPUT_MAGIC   = os.environ.get("OUTPUT_MAGIC",   "docs/magicmirror_v2.ics")
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

def build_base_calendar_with_timezones(src: Calendar, name: str | None = None) -> Calendar:
    out = Calendar()
    for key in ["PRODID", "VERSION", "CALSCALE", "METHOD", "X-WR-TIMEZONE"]:
        if key in src:
            out[key] = src[key]
    if name:
        out["X-WR-CALNAME"] = name
    # Copy VTIMEZONE and any other non-VEVENT components
    for comp in src.subcomponents:
        if getattr(comp, "name", None) != "VEVENT":
            out.add_component(copy.deepcopy(comp))
    return out

# -------- Normalization (for pairwise title matching) --------

def normalize_title(s: str) -> str:
    if not s:
        return ""
    t = s.strip()
    t = re.sub(r"\s*[-–—]\s*Site\s+\w+\s*$", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s{2,}", " ", t).strip()
    return t

# ---------------- Outlook helpers ----------------

def _clear_all_day_flags(ev: Event):
    for k in ["X-MICROSOFT-CDO-ALLDAYEVENT", "TRANSP", "X-MICROSOFT-CDO-BUSYSTATUS"]:
        if k in ev:
            del ev[k]

def add_timed_with_tzid(ev: Event, field: str, dt_local_naive: datetime, tzname: str):
    if field in ev:
        del ev[field]
    ev.add(field.lower(), dt_local_naive, parameters={"TZID": tzname})

def set_event_times_local_outlook(ev: Event, d: date, tzname: str, start_hhmm: str, end_hhmm: str):
    sh, sm = map(int, start_hhmm.split(":"))
    eh, em = map(int, end_hhmm.split(":"))
    start_naive = datetime(d.year, d.month, d.day, sh, sm)
    end_naive   = datetime(d.year, d.month, d.day, eh, em)
    if end_naive <= start_naive:
        end_naive += timedelta(days=1)

    _clear_all_day_flags(ev)
    add_timed_with_tzid(ev, "DTSTART", start_naive, tzname)
    add_timed_with_tzid(ev, "DTEND",   end_naive,   tzname)

    ev["X-MICROSOFT-CDO-ALLDAYEVENT"] = "FALSE"
    ev["X-MICROSOFT-CDO-BUSYSTATUS"]  = "BUSY"
    ev["TRANSP"] = "OPAQUE"

# ---------------- All-day helper ----------------

def set_event_all_day(ev: Event, d: date, busy: bool = True):
    if "DTSTART" in ev: del ev["DTSTART"]
    if "DTEND"   in ev: del ev["DTEND"]
    ev["DTSTART"] = vDate(d)
    ev["DTEND"]   = vDate(d + timedelta(days=1))
    ev["X-MICROSOFT-CDO-ALLDAYEVENT"] = "TRUE"
    ev["X-MICROSOFT-CDO-BUSYSTATUS"]  = "BUSY" if busy else "FREE"
    ev["TRANSP"] = "OPAQUE" if busy else "TRANSPARENT"

# ---------------- UID rewrite (avoid cross-calendar de-dup) ----------------

def rewrite_uid(ev: Event, calendar_tag: str):
    """
    Make a stable, calendar-specific UID from the original UID + tag.
    This avoids collisions with other calendars (e.g., original QGenda)
    and between our two outputs.
    """
    orig = str(ev.get("UID", "")) or ""
    # Include DTSTART (as text) for stability if source has repeated UIDs across dates
    dtstart_txt = ""
    if "DTSTART" in ev:
        dtstart_txt = str(ev["DTSTART"])

    base = f"{orig}|{dtstart_txt}|{calendar_tag}"
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()
    ev["UID"] = f"{digest}@filtered.local"

def main():
    if not SOURCE_ICS_URL:
        logging.error("SOURCE_ICS_URL is not set.")
        sys.exit(1)

    rules  = load_rules()
    tzname = rules.get("timezone") or "America/Detroit"

    exclude_matchers = compile_matchers(rules.get("exclude", []))
    rename_rules     = rules.get("rename", [])

    dc          = rules.get("day_conflicts", {})
    dc_enable   = bool(dc.get("enable", True))
    allow_multi = bool(dc.get("allow_multiple_per_day", True))
    scorer      = make_priority_scorer(dc.get("keep_priority", []))

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

    # pairwise same-day drops (only drop if a keep-match exists that day)
    dropped_by_pairwise = 0
    kept_after_pairwise = []
    for d, evs in by_date.items():
        to_drop = set()
        titles_norm = [normalize_title(str(e.get("SUMMARY", "") or "")) for e in evs]

        for keep_m, drop_m in pairwise_compiled:
            keep_idx = {i for i, t in enumerate(titles_norm) if keep_m(t)}
            if not keep_idx:
                continue
            drop_idx = {i for i, t in enumerate(titles_norm) if drop_m(t)}
            drop_idx -= keep_idx
            to_drop |= drop_idx

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
                final.append(evs[0]); continue
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
    cal_out = build_base_calendar_with_timezones(src, rules["calendar_names"].get("outlook"))
    cal_mm  = build_base_calendar_with_timezones(src, rules["calendar_names"].get("magicmirror"))

    for ev in kept:
        d = as_local_date(ev.get("DTSTART").dt, tzname)
        title = str(ev.get("SUMMARY", "") or "")

        # --- Outlook (timed if rule matches; else all-day BUSY) ---
        ev_out = copy.deepcopy(ev)
        rule = find_time_rule(title)
        if rule and rule.get("start") and rule.get("end"):
            set_event_times_local_outlook(ev_out, d, tzname, rule["start"], rule["end"])
        else:
            set_event_all_day(ev_out, d, busy=True)
        rewrite_uid(ev_out, "OUTLOOK")
        cal_out.add_component(ev_out)

        # --- MagicMirror (always all-day, BUSY so it's clearly visible) ---
        ev_mm = copy.deepcopy(ev)
        set_event_all_day(ev_mm, d, busy=True)
        rewrite_uid(ev_mm, "MAGIC")
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
