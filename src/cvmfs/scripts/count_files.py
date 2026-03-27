#!/usr/bin/env python3
"""
Count all files in the current revision of a CVMFS repository and display a
histogram of their sizes.
"""

import argparse
import math
import sys

import cvmfs
from cvmfs.revision import RevisionIterator


# ---------------------------------------------------------------------------
# Histogram helpers
# ---------------------------------------------------------------------------

def _size_bucket(size_bytes, num_buckets, min_exp, max_exp):
    """Return the bucket index for *size_bytes* on a log2 scale.

    Buckets span [2^min_exp, 2^(min_exp+1)), …, [2^(max_exp-1), 2^max_exp).
    Anything below the first boundary lands in bucket 0; anything at or above
    the last boundary lands in the last bucket.
    """
    if size_bytes <= 0:
        return 0
    exp = math.log2(size_bytes)
    idx = int(exp) - min_exp
    return max(0, min(num_buckets - 1, idx))


def _human(size_bytes):
    """Return a human-readable size string."""
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size_bytes < 1024:
            return f"{size_bytes:.0f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.0f} PiB"


def _bucket_label(idx, min_exp, max_exp):
    """Return a human-readable label for bucket *idx*."""
    low_exp = min_exp + idx
    high_exp = low_exp + 1
    low = 2 ** low_exp
    high = 2 ** high_exp
    if idx == 0:
        return f"< {_human(high)}"
    if low_exp >= max_exp:
        return f">= {_human(low)}"
    return f"{_human(low)} – {_human(high)}"


def print_histogram(buckets, min_exp, max_exp, bar_width=40):
    """Pretty-print an ASCII bar chart of *buckets*."""
    num_buckets = len(buckets)
    max_count = max(buckets) if buckets else 1

    label_width = max(
        len(_bucket_label(i, min_exp, max_exp)) for i in range(num_buckets)
    )

    print()
    print("File-size histogram")
    print("=" * (label_width + bar_width + 20))

    for i, count in enumerate(buckets):
        label = _bucket_label(i, min_exp, max_exp)
        filled = int(bar_width * count / max_count) if max_count > 0 else 0
        bar = "█" * filled
        print(f"  {label:<{label_width}}  {bar:<{bar_width}}  {count:>10,}")

    print("=" * (label_width + bar_width + 20))
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Count all files in the current revision of a CVMFS repository "
            "and display a histogram of their sizes."
        )
    )
    parser.add_argument(
        "repo_identifier",
        help="Local repository name or remote repository URL",
    )
    parser.add_argument(
        "--cache-dir",
        default=None,
        metavar="DIR",
        help="Path to a catalog cache directory previously populated by "
             "download_catalogs. Catalog files found there are used directly "
             "instead of being fetched over the network. The repository "
             "metadata (manifest, whitelist, history DB) is still fetched "
             "from the network if not already cached.",
    )
    parser.add_argument(
        "--min-exp",
        type=int,
        default=0,
        metavar="N",
        help="Lowest power-of-2 exponent for histogram buckets (default: 0 → 1 B)",
    )
    parser.add_argument(
        "--max-exp",
        type=int,
        default=40,
        metavar="N",
        help="Highest power-of-2 exponent for histogram buckets (default: 40 → 1 TiB)",
    )
    parser.add_argument(
        "--progress",
        action="store_true",
        help="Print a progress dot for every 100 000 entries processed",
    )

    args = parser.parse_args()

    min_exp = args.min_exp
    max_exp = args.max_exp
    if min_exp >= max_exp:
        print("error: --min-exp must be less than --max-exp", file=sys.stderr)
        sys.exit(1)

    num_buckets = max_exp - min_exp
    buckets = [0] * num_buckets

    print(f"Opening repository: {args.repo_identifier}")
    if args.cache_dir:
        print(f"Cache dir:          {args.cache_dir}")
    repo = cvmfs.open_repository(args.repo_identifier, cache_dir=args.cache_dir)
    revision = repo.get_current_revision()
    print(f"Revision:           {revision.name}  (root hash: {revision.root_hash})")
    print("Scanning …")

    file_count = 0
    chunked_file_count = 0
    dir_count = 0
    link_count = 0
    total_size = 0
    entry_count = 0

    # Object-hash accounting.
    # unique_hashes collects every distinct CAS hash seen across the whole
    # revision (bulk hashes for non-chunked files + per-chunk hashes for
    # chunked files).  total_hash_refs counts raw references (with repeats)
    # so the deduplication ratio can be derived.
    unique_hashes: set[str] = set()
    total_bulk_refs = 0   # one per non-chunked file (bulk object)
    total_chunk_refs = 0  # one per chunk of every chunked file

    for path, dirent in RevisionIterator(revision,
                                          finish_catalog_callback=repo.close_catalog):
        entry_count += 1

        if args.progress and entry_count % 100_000 == 0:
            print(f"  … {entry_count:,} entries processed", flush=True)

        if dirent.is_file():
            file_count += 1
            size = dirent.size
            total_size += size
            idx = _size_bucket(size, num_buckets, min_exp, max_exp)
            buckets[idx] += 1

            if dirent.has_chunks():
                chunked_file_count += 1
                # The bulk hash on a chunked file still identifies a CAS object
                # (the whole-file blob), so count it as one bulk reference too.
                if dirent.content_hash:
                    unique_hashes.add(dirent.content_hash)
                    total_bulk_refs += 1
                for chunk in dirent.chunks:
                    h = chunk.content_hash_string()
                    unique_hashes.add(h)
                    total_chunk_refs += 1
            else:
                if dirent.content_hash:
                    unique_hashes.add(dirent.content_hash)
                    total_bulk_refs += 1

        elif dirent.is_directory():
            dir_count += 1
        elif dirent.is_symlink():
            link_count += 1

    total_hash_refs = total_bulk_refs + total_chunk_refs
    unique_hash_count = len(unique_hashes)
    dedup_ratio = total_hash_refs / unique_hash_count if unique_hash_count else 1.0

    # Summary
    print()
    print("Results")
    print("=" * 50)
    print(f"  Total entries      : {entry_count:>12,}")
    print(f"  Regular files      : {file_count:>12,}")
    print(f"    of which chunked : {chunked_file_count:>12,}")
    print(f"  Directories        : {dir_count:>12,}")
    print(f"  Symlinks           : {link_count:>12,}")
    print()
    print(f"  Bulk hash refs     : {total_bulk_refs:>12,}")
    print(f"  Chunk hash refs    : {total_chunk_refs:>12,}")
    print(f"  Total hash refs    : {total_hash_refs:>12,}")
    print(f"  Unique CAS objects : {unique_hash_count:>12,}")
    print(f"  Dedup ratio        : {dedup_ratio:>11.2f}x")
    print()
    print(f"  Total file size    : {_human(total_size):>12}")
    if file_count > 0:
        print(f"  Average size       : {_human(total_size // file_count):>12}")
    print("=" * 50)

    # Drop leading and trailing empty buckets for a cleaner chart
    first = next((i for i, c in enumerate(buckets) if c > 0), 0)
    last = next((i for i, c in reversed(list(enumerate(buckets))) if c > 0), num_buckets - 1)
    trimmed = buckets[first : last + 1]
    trimmed_min_exp = min_exp + first

    print_histogram(trimmed, trimmed_min_exp, max_exp)


if __name__ == "__main__":
    main()
