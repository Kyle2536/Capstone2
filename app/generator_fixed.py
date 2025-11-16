# generator_fixed.py
# - Fixed lon/lat per PMGID (no per-row drift)
# - Rush/Late/Normal daypart logic for speed and volume
# - Compatible with existing producer/consumer pipeline (trace_id, sensor_created_at)

import random, hashlib
from datetime import datetime, timezone, timedelta
from tracing import new_trace

try:
    # Python 3.9+ standard library time zone (no extra deps)
    from zoneinfo import ZoneInfo
    TZ_CHICAGO = ZoneInfo("America/Chicago")
except Exception:
    TZ_CHICAGO = None  # fallback: use UTC if tz data missing

# ---------- time helpers ----------
def utc_today_iso_date():
    return datetime.now(timezone.utc).date().isoformat()

def time_of_day_with_ms(base: datetime, ms_jitter: int = 0) -> str:
    t = base + timedelta(milliseconds=ms_jitter)
    return t.strftime("%H:%M:%S.") + f"{int(t.microsecond/1000):03d}"

def now_local_chicago() -> datetime:
    utc_now = datetime.now(timezone.utc)
    if TZ_CHICAGO:
        return utc_now.astimezone(TZ_CHICAGO)
    return utc_now  # fallback

# ---------- deterministic geo per PMGID ----------
def stable_hash(s: str) -> int:
    return int(hashlib.sha256(s.encode()).hexdigest(), 16)

# Dallas-ish box
LON_MIN, LON_MAX = -97.05, -96.55
LAT_MIN, LAT_MAX =  32.60,  33.10

def pmgid_base_lon_lat(pmgid: str):
    """
    Deterministic (seeded by PMGID) single point per sensor.
    PMG00001 -> fixed (lon,lat); PMG00002 -> a different fixed (lon,lat), etc.
    """
    h = stable_hash(pmgid)
    lon = LON_MIN + (h % 10_000) / 10_000 * (LON_MAX - LON_MIN)
    lat = LAT_MIN + ((h // 10_000) % 10_000) / 10_000 * (LAT_MAX - LAT_MIN)
    return lon, lat

# ---------- daypart logic ----------
def daypart_multipliers(local_dt: datetime):
    """
    Returns (speed_mult, volume_mult) based on America/Chicago local time.
      Rush hours:    7–9am, 11am–1pm, 4–6pm
          - Speed:   60–80% of normal
          - Volume:  150–200% of normal
      Late night:    11pm–5am
          - Speed:   110–120% of normal
          - Volume:  30–50% of normal
      Normal:
          - Speed:   90–110% (small variation)
          - Volume:  ~100%
    """
    hr = local_dt.hour

    # Late night 23:00–05:59
    if hr >= 23 or hr <= 5:
        speed_mult  = random.uniform(1.10, 1.20)
        volume_mult = random.uniform(0.30, 0.50)
        return speed_mult, volume_mult

    # Rush windows: 07–09, 11–13, 16–18  (right-inclusive start, exclusive end by ranges below)
    rush_blocks = [(7,10), (11,14), (16,19)]
    in_rush = any(start <= hr < end for (start, end) in rush_blocks)
    if in_rush:
        speed_mult  = random.uniform(0.60, 0.80)
        volume_mult = random.uniform(1.50, 2.00)
        return speed_mult, volume_mult

    # Normal
    speed_mult  = random.uniform(0.90, 1.10)
    volume_mult = 1.00
    return speed_mult, volume_mult

# ---------- record generation ----------
def generate_burst_for_sensor(pmgid: str, base_dt_utc: datetime, cars_in_burst: int):
    """
    Create a burst for a single sensor at an instant (with slight time jitter between cars).
    - lon/lat is FIXED per PMGID (no drift per row).
    - direction is per-burst (all rows in the burst share it).
    - base speed is per-burst, per-row adds small noise.
    """
    # Fixed per-sensor coordinates
    lon0, lat0 = pmgid_base_lon_lat(pmgid)
    fixed_location = f"{lat0:.6f},{lon0:.6f}"

    # Direction (0/1) per-burst
    direction = 1 if random.random() < 0.5 else 0

    # Base speed before daypart scaling (center ~40 mph)
    base_speed = random.gauss(40, 5)

    rows = []
    for i in range(cars_in_burst):
        # Small millisecond spacing between cars for realism
        ms_jitter = i * random.choice([5, 7, 9, 11])

        rec = {
            "created_at": utc_today_iso_date(),
            "timestamp":  time_of_day_with_ms(base_dt_utc, ms_jitter),  # producer/consumer map this to ts
            "peakspeed":  max(1.0, random.gauss(base_speed, 2.0)),      # minor per-car variation
            "pmgid":      pmgid,
            "direction":  direction,
            "location":   fixed_location,                               # <- fixed per PMGID
            "vehiclecount": 1,
        }
        # merges in trace_id and sensor_created_at (ISO-UTC)
        rec = new_trace() | rec
        rows.append(rec)
    return rows

def generate_records(num_records: int):
    """
    Master generator that applies daypart scaling to BOTH speed and volume.
    We compute a per-burst size as: base_cars * volume_mult
    and scale the per-row speed by speed_mult.
    """
    emitted = 0
    pmg_index = 0

    while emitted < num_records:
        # Choose next sensor
        pmg_index += 1
        pmgid = f"PMG{pmg_index:05d}"

        # Base burst size (before time-of-day scaling)
        base_cars = random.randint(3, 5)

        # Compute local (Chicago) time right now and daypart multipliers
        local_now = now_local_chicago()
        speed_mult, volume_mult = daypart_multipliers(local_now)

        # Scale volume: ensure at least 1 car
        cars = max(1, int(round(base_cars * volume_mult)))

        # Use a single base UTC time for the burst; the per-row "timestamp" gets ms jitter
        now_utc = datetime.now(timezone.utc)

        # Generate raw rows
        burst_rows = generate_burst_for_sensor(pmgid, now_utc, cars)

        # Apply speed multiplier to each row
        for row in burst_rows:
            row["peakspeed"] = max(1.0, row["peakspeed"] * speed_mult)

            yield row
            emitted += 1
            if emitted >= num_records:
                break
