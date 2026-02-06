#!/usr/bin/env python3
"""DST Validation Helper

Validates if time differences between timestamps match actual DST transitions.
"""

import datetime
from typing import Optional
from zoneinfo import ZoneInfo


def get_dst_offset_for_date(dt: datetime.datetime, timezone_name: str = "Europe/Berlin") -> int:
    """Get DST offset in seconds for a specific date in the given timezone"""
    try:
        tz = ZoneInfo(timezone_name)

        # Create timezone-aware datetime
        aware_dt = dt.replace(tzinfo=tz)

        # Get the UTC offset (includes DST)
        utc_offset = aware_dt.utcoffset()

        # Get the standard offset (without DST)
        # We do this by checking January 1st of the same year (definitely standard time)
        jan_1st = datetime.datetime(dt.year, 1, 1, tzinfo=tz)
        std_offset = jan_1st.utcoffset()

        # DST offset is the difference
        dst_offset = utc_offset - std_offset
        return int(dst_offset.total_seconds())

    except Exception:
        return 0  # Default to no DST offset if timezone detection fails


def is_valid_dst_difference(
    metadata_time: datetime.datetime, reference_time: datetime.datetime, timezone_name: str = "Europe/Berlin"
) -> tuple[bool, Optional[str]]:
    """
    Check if the time difference between metadata and reference time
    matches expected timezone/DST scenarios.

    Returns:
        (is_valid_dst, explanation)
    """
    time_diff_seconds = int((reference_time - metadata_time).total_seconds())

    # Check if metadata could be UTC and reference is local time
    tz = ZoneInfo(timezone_name)
    reference_with_tz = reference_time.replace(tzinfo=tz)
    utc_offset_seconds = int(reference_with_tz.utcoffset().total_seconds())

    # If time difference matches UTC offset, metadata is likely UTC
    if abs(time_diff_seconds - utc_offset_seconds) <= 10:  # Allow 10 second tolerance
        if utc_offset_seconds == 3600:
            return True, "Metadata in UTC, reference in local time (UTC+1)"
        if utc_offset_seconds == 7200:
            return True, "Metadata in UTC, reference in local time (UTC+2, DST)"
        return True, f"Metadata in UTC, reference in local time (UTC{utc_offset_seconds // 3600:+d})"

    # Check for DST transition differences
    metadata_dst = get_dst_offset_for_date(metadata_time, timezone_name)
    reference_dst = get_dst_offset_for_date(reference_time, timezone_name)
    expected_dst_diff = reference_dst - metadata_dst

    if abs(time_diff_seconds - expected_dst_diff) <= 10:  # Allow 10 second tolerance
        if expected_dst_diff == 3600:
            return True, "1-hour DST spring forward transition"
        if expected_dst_diff == -3600:
            return True, "1-hour DST fall back transition"
        if expected_dst_diff != 0:
            return True, f"{abs(expected_dst_diff) // 3600}-hour DST difference"

    return False, None


def debug_dst_for_date(dt: datetime.datetime, timezone_name: str = "Europe/Berlin"):
    """Debug DST information for a specific date"""
    print(f"Date: {dt}")
    print(f"Timezone: {timezone_name}")

    try:
        tz = ZoneInfo(timezone_name)
        aware_dt = dt.replace(tzinfo=tz)

        print(f"UTC offset: {aware_dt.utcoffset()}")
        print(f"DST offset: {get_dst_offset_for_date(dt, timezone_name)} seconds")
        print(f"Timezone name: {aware_dt.tzname()}")

        # Check if this is around a DST transition
        day_before = dt - datetime.timedelta(days=1)
        day_after = dt + datetime.timedelta(days=1)

        dst_before = get_dst_offset_for_date(day_before, timezone_name)
        dst_current = get_dst_offset_for_date(dt, timezone_name)
        dst_after = get_dst_offset_for_date(day_after, timezone_name)

        if dst_before != dst_current:
            print(f"DST transition detected: {dst_before // 3600}h -> {dst_current // 3600}h")
        if dst_current != dst_after:
            print(f"DST transition coming: {dst_current // 3600}h -> {dst_after // 3600}h")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    # Test with the problematic dates
    test_dates = [
        datetime.datetime(2021, 1, 30, 8, 17, 7),  # January (standard time)
        datetime.datetime(2021, 2, 11, 19, 52, 24),  # February (standard time)
        datetime.datetime(2021, 3, 31, 20, 7, 5),  # March 31 (DST transition day!)
    ]

    for dt in test_dates:
        print("=" * 50)
        debug_dst_for_date(dt)
