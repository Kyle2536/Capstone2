# generate_week.py
# Produce 5 full days (Mon–Fri) of synthetic traffic events using the
# same logic as your live pipeline:
# - Fixed lat/lon per PMGID (sensor location, not car)
# - Rush hours: 7–9am, 11am–1pm, 4–6pm → lower speed, higher volume
# - Late night: 11pm–5am → higher speed, lower volume
# - Normal hours: mild variation
#
# Output: CSV with columns compatible with your pipeline fields
#   run_id, trace_id, created_at, ts, sensor_created_at, pmgid, location,
#   direction, peakspeed, vehiclecount

import os
import csv
import uuid
import math
import random
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

# ---------- timezone ----------
try:
    from zoneinfo import ZoneInfo
    TZ_CHICAGO = ZoneInfo("America/Chicago")
except Exception:
    TZ_CHICAGO = None  # fallback: UTC

# ---------- config (env or CLI args optional) ----------
import argparse

def parse_args():
    p = argparse.ArgumentParser(description="Generate Mon–Fri traffic CSV.")
    p.add_argument("--start", type=str, default="", help="Start date (YYYY-MM-DD) — must be a Monday. If blank, uses most recent Monday (Chicago).")
    p.add_argument("--days", type=int, default=5, help="Number of days (default 5)")
    p.add_argument("--sensors", type=int, default=int(os.getenv("SENSORS", "50")), help="Number of sensors/PMGIDs (default 50)")
    p.add_argument("--outfile", type=str, default=os.getenv("OUT_CSV", "traffic_week.csv"), help="Output CSV path")
    p.add_argument("--seed", type=int, default=None, help="Random seed (optional, for reproducibility)")
    # “how much to try per minute” knobs (kept simple so volumes are reasonable)
    p.add_argument("--base_cars_min", type=int, default=2, help="Base cars per burst (min)")
    p.add_argument("--base_cars_max", type=int, default=4, help="Base cars per burst (max)")
    p.add_argument("--burst_prob", type=float, default=0.40, help="Baseline probability a sensor emits a burst in a given minute (scaled by volume multiplier)")
    return p.parse_args()

# ---------- helpers ----------
def chicago_now() -> datetime:
    now_utc = datetime.now(timezone.utc)
    return now_utc.astimezone(TZ_CHICAGO) if TZ_CHICAGO else now_utc

def most_recent_monday(local_dt: datetime) -> datetime:
    # truncate to 00:00 local, then step back to Monday
    d = local_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    # weekday(): Monday=0
    return d - timedelta(days=d.weekday())

def coerce_start_monday(s: str) -> datetime:
    if not s:
        return most_recent_monday(chicago_now())
    y, m, d = map(int, s.split("-"))
    base = datetime(y, m, d, tzinfo=TZ_CHICAGO or timezone.utc)
    if base.weekday() != 0:
        raise ValueError("Start date must be a Monday (weekday() == 0).")
    return base

def utc_iso_ms(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds")

def time_of_day_with_ms(local_dt: datetime, ms: int) -> str:
    t = local_dt + timedelta(milliseconds=ms)
    return t.strftime("%H:%M:%S.") + f"{int(t.microsecond/1000):03d}"

def stable_hash(s: str) -> int:
    return int(hashlib.sha256(s.encode()).hexdigest(), 16)

# Dallas-ish bounding box
LON_MIN, LON_MAX = -97.05, -96.55
LAT_MIN, LAT_MAX =  32.60,  33.10

def pmgid_base_lon_lat(pmgid: str):
    """Deterministic single point per PMGID (sensor location)."""
    h = stable_hash(pmgid)
    lon = LON_MIN + (h % 10_000) / 10_000 * (LON_MAX - LON_MIN)
    lat = LAT_MIN + ((h // 10_000) % 10_000) / 10_000 * (LAT_MAX - LAT_MIN)
    return lon, lat

def daypart_multipliers(local_dt: datetime):
    """
    Returns (speed_mult, volume_mult) by local hour:
      Rush: 7–9, 11–13, 16–18  -> speed 0.60–0.80×, volume 1.50–2.00×
      Late: 23–5               -> speed 1.10–1.20×, volume 0.30–0.50×
      Normal:                   speed 0.90–1.10×, volume 1.00×
    """
    hr = local_dt.hour
    if hr >= 23 or hr <= 5:
        return (random.uniform(1.10, 1.20), random.uniform(0.30, 0.50))
    if (7 <= hr < 10) or (11 <= hr < 14) or (16 <= hr < 19):
        return (random.uniform(0.60, 0.80), random.uniform(1.50, 2.00))
    return (random.uniform(0.90, 1.10), 1.00)

@dataclass
class Row:
    run_id: str
    trace_id: str
    created_at: str       # YYYY-MM-DD (UTC date stamp like your pipeline)
    ts: str               # HH:MM:SS.mmm (local->string)
    sensor_created_at: str  # ISO-UTC with ms
    pmgid: str
    location: str         # "lat,lon"
    direction: int        # 0 south / 1 north
    peakspeed: int        # integer mph (round at generation)
    vehiclecount: int     # always 1

def generate_rows_for_minute(local_minute: datetime, pmgid: str,
                             base_cars_min: int, base_cars_max: int,
                             burst_prob: float):
    """
    For the given local minute and sensor, randomly emit 0..N bursts.
    We scale both probability and cars by the volume multiplier.
    Speeds are centered ~40 mph, scaled by daypart, with small per-car noise.
    """
    speed_mult, volume_mult = daypart_multipliers(local_minute)
    # scale burst probability by volume
    p = min(1.0, burst_prob * volume_mult)
    rows = []

    # Decide if the sensor emits a burst this minute
    if random.random() < p:
        # cars scaled by volume
        cars = max(1, int(round(random.randint(base_cars_min, base_cars_max) * volume_mult)))

        # fixed per-sensor location
        lon, lat = pmgid_base_lon_lat(pmgid)
        loc_str = f"{lat:.6f},{lon:.6f}"
        # direction per burst (0/1)
        direction = 1 if random.random() < 0.5 else 0
        # base speed before scaling (mph)
        base_speed = random.gauss(40, 5)

        for i in range(cars):
            # jitters within the minute (spread a bit across ms)
            ms = i * random.choice([5, 7, 9, 11, 13])
            # per-car small noise then scale by time-of-day
            mph = max(1.0, (random.gauss(base_speed, 2.0) * speed_mult))
            mph_int = int(round(mph))

            # local→UTC timestamping like your pipeline: created_at (UTC date), ts (local string),
            # sensor_created_at (true event time in ISO-UTC with ms)
            local_ts = (local_minute + timedelta(milliseconds=ms))
            sensor_created_at = utc_iso_ms(local_ts)
            created_at_utc_date = datetime.now(timezone.utc).date().isoformat()

            r = Row(
                run_id="",                 # filled by caller (week id)
                trace_id=str(uuid.uuid4()),
                created_at=created_at_utc_date,
                ts=time_of_day_with_ms(local_minute, ms),
                sensor_created_at=sensor_created_at,
                pmgid=pmgid,
                location=loc_str,
                direction=direction,
                peakspeed=mph_int,
                vehiclecount=1,
            )
            rows.append(r)

    return rows

def main():
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    # Determine Monday start
    start_local = coerce_start_monday(args.start)
    run_id = f"week_{start_local.date().isoformat()}"

    # Prepare PMGIDs list (PMG00001..)
    pmgids = [f"PMG{(i+1):05d}" for i in range(args.sensors)]

    total = 0
    with open(args.outfile, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["run_id","trace_id","created_at","ts","sensor_created_at",
                    "pmgid","location","direction","peakspeed","vehiclecount"])

        for d in range(args.days):
            day_local = start_local + timedelta(days=d)
            for minute in range(24 * 60):
                # local minute boundary
                local_minute = day_local + timedelta(minutes=minute)

                # generate per sensor
                for pmgid in pmgids:
                    rows = generate_rows_for_minute(local_minute, pmgid,
                                                    args.base_cars_min, args.base_cars_max,
                                                    args.burst_prob)
                    for r in rows:
                        w.writerow([run_id, r.trace_id, r.created_at, r.ts, r.sensor_created_at,
                                    r.pmgid, r.location, r.direction, r.peakspeed, r.vehiclecount])
                    total += len(rows)

    print(f"✔ Wrote {total} rows to {args.outfile} for run_id={run_id} across {args.days} day(s), {len(pmgids)} sensors.")

if __name__ == "__main__":
    main()
