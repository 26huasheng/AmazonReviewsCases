from __future__ import annotations

HASH_ALGORITHM = "sha256_first_64_bits_big_endian_v1"
PRODUCT_ID_ENCODING = "utf-8"
DEFAULT_EVENT_STORE_BUCKET_COUNT = 256
EVENT_STORE_SORT_KEYS = ["product_id", "event_time_ms"]


def validate_bucket_count(bucket_count: int) -> int:
    if isinstance(bucket_count, bool) or not isinstance(bucket_count, int) or bucket_count <= 0:
        raise ValueError("event-store bucket_count must be a positive integer")
    return bucket_count


def bucket_directory_width(bucket_count: int) -> int:
    validate_bucket_count(bucket_count)
    return max(3, len(str(bucket_count - 1)))


def event_store_metadata(bucket_count: int) -> dict[str, object]:
    return {
        "hash_algorithm": HASH_ALGORITHM,
        "hash_input": "product_id",
        "product_id_encoding": PRODUCT_ID_ENCODING,
        "hash_bytes": "first_8_bytes_big_endian_unsigned",
        "bucket_count": validate_bucket_count(bucket_count),
        "bucket_directory_width": bucket_directory_width(bucket_count),
        "event_store_sort_keys": EVENT_STORE_SORT_KEYS,
    }
