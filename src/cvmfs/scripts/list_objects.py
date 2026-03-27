#!/usr/bin/env python3
"""
List all unique object hashes reachable via the catalogs of a CVMFS repository
that match a given hex prefix.

Outputs one hash per line to stdout, suitable for piping.
"""

import argparse
import sys

import cvmfs
from cvmfs.revision import RevisionIterator


def main():
    parser = argparse.ArgumentParser(
        description=(
            "List all unique CAS object hashes reachable via the catalogs of "
            "a CVMFS repository that start with a given hex prefix."
        )
    )
    parser.add_argument(
        "repo_identifier",
        help="Local repository name or remote repository URL",
    )
    parser.add_argument(
        "prefix",
        help="Hex prefix to filter hashes (e.g. '00' or 'ab12')",
    )
    parser.add_argument(
        "--cache-dir",
        default=None,
        metavar="DIR",
        help="Path to a catalog cache directory previously populated by "
             "download_catalogs. Catalog files found there are used directly "
             "instead of being fetched over the network.",
    )
    parser.add_argument(
        "--progress",
        action="store_true",
        help="Print progress to stderr for every 100 000 entries processed",
    )

    args = parser.parse_args()
    prefix = args.prefix.lower()

    repo = cvmfs.open_repository(args.repo_identifier, cache_dir=args.cache_dir)
    revision = repo.get_current_revision()

    seen: set[str] = set()
    entry_count = 0

    for _path, dirent in RevisionIterator(revision,
                                          finish_catalog_callback=repo.close_catalog):
        entry_count += 1

        if args.progress and entry_count % 100_000 == 0:
            print(f"  … {entry_count:,} entries processed", file=sys.stderr, flush=True)

        if not dirent.is_file():
            continue

        if dirent.has_chunks():
            if dirent.content_hash and dirent.content_hash.startswith(prefix):
                seen.add(dirent.content_hash)
            for chunk in dirent.chunks:
                h = chunk.content_hash_string()
                if h.startswith(prefix):
                    seen.add(h)
        else:
            if dirent.content_hash and dirent.content_hash.startswith(prefix):
                seen.add(dirent.content_hash)

    for h in sorted(seen):
        print(h)


if __name__ == "__main__":
    main()
