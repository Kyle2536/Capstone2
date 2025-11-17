# generator_fixed.py
# - Fixed lon/lat per PMGID (no per-row drift)
# - Rush/Late/Normal daypart logic for speed and volume
# - Compatible with existing producer/consumer pipeline (trace_id, sensor_created_at)

import random, hashlib
from datetime import datetime, timezone, timedelta
from tracing import new_trace


# ---- realistic sensor placement: Poisson-disk in the Dallas box ----
import math
from functools import lru_cache

# Dallas-ish bounding box
LON_MIN, LON_MAX = -97.05, -96.55
LAT_MIN, LAT_MAX =  32.60,  33.10
BOX_COS = math.cos(math.radians(33.0))

def km_to_deg_lat(km: float) -> float:
    return km / 111.0

def km_to_deg_lon(km: float) -> float:
    return km / (111.0 * BOX_COS)

def _poisson_disk_points(n_target: int, min_sep_km: float, seed: int = 4372):
    """
    Bridson-ish Poisson-disk sampler (simple version) in lat/lon degrees.
    Deterministic given (n_target, min_sep_km, seed).
    """
    rnd = random.Random(seed)
    min_sep_lat = km_to_deg_lat(min_sep_km)
    min_sep_lon = km_to_deg_lon(min_sep_km)

    # grid cell sizes (so neighbors are limited)
    cell_lat = min_sep_lat / math.sqrt(2)
    cell_lon = min_sep_lon / math.sqrt(2)

    grid = {}
    points = []

    def to_cell(lat, lon):
        i = int((lat - LAT_MIN) / cell_lat)
        j = int((lon - LON_MIN) / cell_lon)
        return i, j

    def in_box(lat, lon):
        return LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX

    def far_enough(lat, lon):
        ci, cj = to_cell(lat, lon)
        # check this cell and neighbors
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                key = (ci + di, cj + dj)
                if key in grid:
                    for (plat, plon) in grid[key]:
                        dlat = (lat - plat) * 111.0
                        dlon = (lon - plon) * 111.0 * BOX_COS
                        if math.hypot(dlat, dlon) < min_sep_km:
                            return False
        return True

    # seed with a random point
    lat0 = rnd.uniform(LAT_MIN, LAT_MAX)
    lon0 = rnd.uniform(LON_MIN, LON_MAX)
    points.append((lat0, lon0))
    grid[to_cell(lat0, lon0)] = [(lat0, lon0)]
    active = [(lat0, lon0)]

    k = 20  # attempts per active point
    while active and len(points) < n_target:
        idx = rnd.randrange(len(active))
        plat, plon = active[idx]
        found = False
        for _ in range(k):
            # pick a ring between r and 2r in deg
            r_km = min_sep_km * (1 + rnd.random())
            r_lat = km_to_deg_lat(r_km)
            r_lon = km_to_deg_lon(r_km)
            theta = rnd.uniform(0, 2*math.pi)
            lat = plat + r_lat * math.sin(theta)
            lon = plon + r_lon * math.cos(theta)
            if in_box(lat, lon) and far_enough(lat, lon):
                points.append((lat, lon))
                active.append((lat, lon))
                cell = to_cell(lat, lon)
                grid.setdefault(cell, []).append((lat, lon))
                found = True
                break
        if not found:
            active.pop(idx)

        # stop early if we’re close enough
        if len(points) >= n_target:
            break

    return points

@lru_cache(maxsize=8)
def sensor_points(n_target: int = 1000, min_sep_km: float = 0.7, seed: int = 4372):
    return _poisson_disk_points(n_target, min_sep_km, seed)

def pmgid_base_lon_lat(pmgid: str, n_target: int = 1000, min_sep_km: float = 0.7, seed: int = 4372):
    """
    Fixed per-PMGID mapping onto precomputed Poisson-disk points (deterministic).
    PMG00001 → points[0], PMG00002 → points[1], etc. If PMG index exceeds points,
    we wrap around.
    """
    pts = sensor_points(n_target=n_target, min_sep_km=min_sep_km, seed=seed)
    # PMG00001 → 0
    try:
        idx = int(pmgid[-5:]) - 1
    except Exception:
        idx = 0
    lat, lon = pts[idx % len(pts)]
    return lon, lat



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
