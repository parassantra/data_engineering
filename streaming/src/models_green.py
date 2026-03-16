import json
import math
import pandas as pd
from dataclasses import dataclass


@dataclass
class GreenTrip:
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
    PULocationID: int
    DOLocationID: int
    passenger_count: float
    trip_distance: float
    tip_amount: float
    total_amount: float


def _safe_float(val, default=0.0) -> float:
    """Return default if val is NaN/None, otherwise cast to float."""
    try:
        f = float(val)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return default


def green_trip_from_row(row):
    """Convert a DataFrame row to a GreenTrip object"""
    return GreenTrip(
        lpep_pickup_datetime=str(row['lpep_pickup_datetime']),
        lpep_dropoff_datetime=str(row['lpep_dropoff_datetime']),
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        passenger_count=_safe_float(row['passenger_count']),   # was only field guarded
        trip_distance=_safe_float(row['trip_distance']),        # NEW
        tip_amount=_safe_float(row['tip_amount']),              # NEW
        total_amount=_safe_float(row['total_amount']),          # NEW
    )


def green_trip_serializer(trip):
    """Serialize GreenTrip to JSON bytes — NaN is never valid JSON."""
    import dataclasses
    trip_dict = dataclasses.asdict(trip)
    # allow_nan=False raises ValueError immediately if a NaN slips through,
    # so you get a clear error at the producer rather than a silent bad message.
    json_str = json.dumps(trip_dict, allow_nan=False)
    return json_str.encode('utf-8')


def green_trip_deserializer(data):
    """Deserialize JSON bytes to GreenTrip"""
    json_str = data.decode('utf-8')
    trip_dict = json.loads(json_str)
    return GreenTrip(**trip_dict)