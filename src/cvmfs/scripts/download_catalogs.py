#!/usr/bin/env python3
"""
Download all catalogs for a CVMFS repository at the current revision.

Catalogs are stored in a cache subdirectory named after the repository,
making cleanup straightforward.
"""

import argparse
import collections
import logging
import os
import shutil
import sys
import time

import cvmfs

logger = logging.getLogger(__name__)


def _catalog_cache_path(cache_dir, catalog_hash):
    """Return the cache file path for a catalog given its hash."""
    return os.path.join(cache_dir, "data", catalog_hash[:2], catalog_hash[2:] + "C")


def _seed_from_shared_cache(shared_cache_dir, cache_dir, revision):
    """
    Walk the catalog tree (using the shared cache) and copy any catalog files
    that already exist in the shared cache into the exclusive cache directory.
    Returns the number of catalogs copied.
    """
    # First, open the repo against the shared cache to discover catalog hashes
    # We collect all catalog hashes by iterating the catalog tree
    copied = 0
    skipped = 0

    # Collect all nested catalog hashes by walking the tree via the shared cache
    catalog_hashes = _collect_catalog_hashes(revision)

    for cat_hash in catalog_hashes:
        src = _catalog_cache_path(shared_cache_dir, cat_hash)
        dst = _catalog_cache_path(cache_dir, cat_hash)
        if os.path.exists(dst):
            skipped += 1
            continue
        if os.path.exists(src):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copy2(src, dst)
            copied += 1

    return copied, skipped


def _collect_catalog_hashes(revision):
    """Collect all catalog hashes in the current revision by walking the tree."""
    hashes = []
    for clg in revision.catalogs():
        hashes.append(clg.hash)
        revision.repository.close_catalog(clg)
    return hashes


def main():
    parser = argparse.ArgumentParser(
        description="Download all catalogs for a CVMFS repository at the current revision."
    )
    parser.add_argument(
        "repo_url",
        help="Repository URL (e.g. http://cvmfs-stratum-one.cern.ch/cvmfs/unpacked.cern.ch)"
    )
    parser.add_argument(
        "--cache-dir",
        default=None,
        help="Base cache directory (default: ./cache)"
    )
    parser.add_argument(
        "--seed-from",
        default=None,
        metavar="SHARED_CACHE_DIR",
        help="Copy catalogs already present in this shared cache directory "
             "into the exclusive cache before downloading missing ones. "
             "This avoids re-downloading catalogs that were previously fetched."
    )
    args = parser.parse_args()

    repo_url = args.repo_url
    # Derive repo name from URL for the cache subdirectory
    repo_name = repo_url.rstrip("/").split("/")[-1]

    base_cache = args.cache_dir or os.path.join(os.getcwd(), "cache")
    cache_dir = os.path.join(base_cache, repo_name)
    os.makedirs(cache_dir, exist_ok=True)

    print(f"Repository : {repo_url}")
    print(f"Cache dir  : {cache_dir}")

    if args.seed_from:
        shared_cache = args.seed_from
        if not os.path.isdir(shared_cache):
            print(f"Error: shared cache directory does not exist: {shared_cache}",
                  file=sys.stderr)
            sys.exit(1)
        print(f"Seed from  : {shared_cache}")
        print()

        # Open repo against the shared cache to discover catalog hashes
        # without triggering downloads into the exclusive cache
        print("Discovering catalogs via shared cache...")
        shared_repo = cvmfs.open_repository(repo_url, cache_dir=shared_cache)
        shared_revision = shared_repo.get_current_revision()
        print(f"Revision   : {shared_revision.revision_number}")
        print(f"Root hash  : {shared_revision.root_hash}")

        copied, skipped = _seed_from_shared_cache(
            shared_cache, cache_dir, shared_revision)
        print(f"Seeded {copied} catalogs from shared cache "
              f"({skipped} already present)")
        print()

    # Now open against the exclusive cache — catalogs seeded above will
    # be cache hits, the rest will be downloaded
    repo = cvmfs.open_repository(repo_url, cache_dir=cache_dir)
    revision = repo.get_current_revision()
    print(f"Revision   : {revision.revision_number}")
    print(f"Root hash  : {revision.root_hash}")
    print()

    count = 0
    skipped = 0
    downloaded = 0
    total_entries = 0
    start_time = time.monotonic()
    last_log_time = start_time

    # Walk the catalog tree manually so we can check the cache before
    # each catalog is retrieved (the iterator would trigger the download
    # before we can inspect whether it was a cache hit).
    catalog_stack = collections.deque()
    catalog_stack.append(revision.root_hash)

    while catalog_stack:
        cat_hash = catalog_stack.pop()
        already_cached = os.path.exists(_catalog_cache_path(cache_dir, cat_hash))
        clg = revision.retrieve_catalog(cat_hash)
        count += 1

        # Queue nested catalogs for processing
        for nested_ref in clg.list_nested():
            catalog_stack.append(nested_ref.hash)

        entries = clg.run_sql("SELECT count(*) FROM catalog;")[0][0]
        total_entries += entries
        now = time.monotonic()
        elapsed = now - start_time
        rate = count / elapsed if elapsed > 0 else 0

        if already_cached:
            skipped += 1
            status = "cached"
        else:
            downloaded += 1
            status = "downloaded"

        print(f"  [{count:4d}] {clg.root_prefix}  ({entries} entries, hash={clg.hash}, {status})")
        # Log a progress summary every 30 seconds
        if now - last_log_time >= 30:
            print(f"  --- progress: {count} catalogs processed "
                  f"({downloaded} downloaded, {skipped} cached), "
                  f"{total_entries} total entries, "
                  f"{elapsed:.0f}s elapsed, {rate:.1f} catalogs/s ---")
            last_log_time = now
        repo.close_catalog(clg)

    elapsed = time.monotonic() - start_time
    rate = count / elapsed if elapsed > 0 else 0
    print(f"\nProcessed {count} catalogs ({downloaded} downloaded, {skipped} already cached, "
          f"{total_entries} total entries) "
          f"to {cache_dir} in {elapsed:.1f}s ({rate:.1f} catalogs/s)")


if __name__ == "__main__":
    main()
