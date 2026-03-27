#!/usr/bin/env python3
"""
Compute the content-hash overlap between two CVMFS repositories.

Given one or more path prefixes (optionally with glob wildcards) from
repository A and a second repository B, this tool reports how many content
hashes (and how much data) are shared between them.  Optionally, it also
shows which directories in repo A have the greatest overlap with repo B.

The script downloads (and caches) all required catalogs for both
repositories.  The hash index for repo B is stored in
~/.cache/cvmfs-overlap/ so that subsequent runs against the same revision
of repo B are fast.
"""

import argparse
import fnmatch
import os
import os.path
import sqlite3
import sys
import time
from collections import defaultdict

import cvmfs

CACHE_VERSION = "v1"


# ---------------------------------------------------------------------------
# Cache location
# ---------------------------------------------------------------------------

def _get_cache_path():
    base = os.environ.get("XDG_CACHE_HOME", "").strip()
    if not base:
        base = os.path.expanduser("~/.cache")
    return os.path.join(base, "cvmfs-overlap", CACHE_VERSION)


DEFAULT_CACHE_PATH = _get_cache_path()


# ---------------------------------------------------------------------------
# Wildcard helpers
# ---------------------------------------------------------------------------

def _has_wildcards(s):
    """Return True if *s* contains any fnmatch wildcard character."""
    return any(c in s for c in "*?[")


def _literal_prefix(pattern):
    """
    Return the portion of *pattern* before the first wildcard character.

    Used for catalog pre-filtering: any path that could match the pattern
    must share this literal prefix with it.
    """
    for i, c in enumerate(pattern):
        if c in "*?[":
            return pattern[:i]
    return pattern


# ---------------------------------------------------------------------------
# Path-relevance helpers
# ---------------------------------------------------------------------------

def _prefix_is_relevant(root_prefix, patterns):
    """
    Return True if a catalog (or catalog reference) with *root_prefix* may
    contain entries relevant to at least one element of *patterns*.

    Patterns may be plain path prefixes or fnmatch glob patterns (``*``,
    ``?``, ``[...]``).

    For plain patterns three cases make a catalog relevant:
      1. Its subtree root is exactly a requested path.
      2. Its subtree root is a descendant of a requested path.
      3. A requested path is a descendant of its subtree root
         (the root catalog always falls here for any absolute path).

    For wildcard patterns a catalog is considered relevant when it could
    plausibly contain matching entries:
      1. The root catalog (``root_norm == ""``) is always included.
      2. The catalog root itself matches the pattern (``fnmatch``).
      3. The catalog root is at or below the pattern's literal prefix
         (i.e. inside the wildcard zone).
      4. The pattern's literal prefix is below the catalog root (i.e. the
         catalog is an ancestor that leads into the wildcard zone).
    """
    if patterns is None:
        return True

    root_norm = root_prefix.rstrip("/")

    for p in patterns:
        if not _has_wildcards(p):
            # Plain prefix logic (unchanged from original).
            p_norm = p.rstrip("/")
            if root_norm == p_norm:
                return True
            if root_norm.startswith(p_norm + "/"):
                return True
            if p_norm.startswith(root_norm + "/"):
                return True
            if root_norm == "":
                return True
        else:
            # Wildcard pattern.
            if root_norm == "":                           # root catalog
                return True
            if fnmatch.fnmatch(root_norm, p):             # root itself matches
                return True
            lit = _literal_prefix(p).rstrip("/")
            if root_norm.startswith(lit):                 # root is in/below wildcard zone
                return True
            if lit.startswith(root_norm + "/") or lit == root_norm:   # root is an ancestor
                return True

    return False


def _entry_is_relevant(entry_path, patterns):
    """
    Return True if *entry_path* falls under at least one element of *patterns*.

    For plain patterns a simple prefix check is used.  For wildcard patterns
    ``fnmatch`` is applied both to the full *entry_path* (direct match) and
    to every directory prefix of *entry_path* (so that files *under* a
    directory that matches the pattern are included).
    """
    if patterns is None:
        return True

    for p in patterns:
        if not _has_wildcards(p):
            p_norm = p.rstrip("/")
            if entry_path == p_norm or entry_path.startswith(p_norm + "/"):
                return True
        else:
            p_norm = p.rstrip("/")
            # Direct match (e.g. pattern "/sw/art*" matches "/sw/artisan/lib/foo.so"
            # because fnmatch treats * as matching any string including "/").
            if fnmatch.fnmatch(entry_path, p_norm):
                return True
            # Match via a directory prefix of entry_path.  This handles patterns
            # like "/sw/*/lib" where the full entry path is "/sw/art/lib/foo.so":
            # the prefix "/sw/art/lib" matches and the file is under it.
            parts = entry_path.split("/")
            for depth in range(1, len(parts)):
                prefix = "/".join(parts[: depth + 1])
                if fnmatch.fnmatch(prefix, p_norm):
                    return True

    return False


# ---------------------------------------------------------------------------
# Symlink resolution
# ---------------------------------------------------------------------------

def _resolve_symlink_target(parent_dir, target):
    """
    Resolve a symlink *target* (from a catalog entry) to an absolute path.

    *parent_dir* is the directory containing the symlink.  Relative targets
    are resolved against it.  The result is normalised with ``os.path.normpath``
    to remove ``..`` components and double slashes.

    Returns *None* if *target* is empty.
    """
    if not target:
        return None
    if target.startswith("/"):
        return os.path.normpath(target)
    return os.path.normpath(os.path.join(parent_dir, target))


# ---------------------------------------------------------------------------
# Catalog traversal that avoids downloading irrelevant catalogs
# ---------------------------------------------------------------------------

def _iter_relevant_catalogs(rev, patterns):
    """
    Yield catalogs from *rev* that are relevant to *patterns*.

    Catalogs are fetched **lazily** — only when they are popped from the
    stack — and **closed** immediately after the caller has finished with
    them.  This means at most one catalog file handle is open at any given
    time, regardless of how many catalogs the repository contains.

    The stack stores plain catalog hash strings rather than ``Catalog``
    objects, so no file handles are held while waiting on the stack.
    ``CatalogReference.root_path`` is still used for the relevance check
    before pushing a hash, so irrelevant subtrees are never downloaded.
    """
    # Seed the stack with the root catalog's hash.
    stack = [rev.root_hash]

    while stack:
        cat_hash = stack.pop()
        try:
            clg = rev.repository.retrieve_catalog(cat_hash)
        except sqlite3.DatabaseError as exc:
            print(
                f"  WARNING: skipping malformed catalog {cat_hash[:12]}…: {exc}\n"
                "           (re-run with --no-cache or clear the catalog cache"
                " to force a fresh download)",
                file=sys.stderr,
            )
            continue
        yield clg
        # Execution resumes here after the caller has finished with clg.
        # Read nested references while the catalog is still open, then
        # close it before moving on to the next one.
        for nested_ref in clg.list_nested():
            if _prefix_is_relevant(nested_ref.root_path, patterns):
                stack.append(nested_ref.hash)
        rev.repository.close_catalog(clg)


# ---------------------------------------------------------------------------
# Build / load the hash index for repo B
# ---------------------------------------------------------------------------

def _collect_hashes(rev, include_chunks):
    """
    Walk all catalogs in *rev* and return a ``{hash_bytes: size_bytes}`` dict.

    The dict maps each unique content-hash BLOB to the uncompressed size of
    the corresponding file or chunk.  When the same hash appears in multiple
    catalog entries the first size encountered is kept (sizes must be
    identical for deduplicated content).
    """
    hash_to_size = {}
    n_catalogs = 0
    n_skipped  = 0

    # Use an explicit next() loop so that a malformed catalog can be caught
    # and skipped without aborting the entire collection.
    catalog_iter = rev.catalogs()
    while True:
        try:
            clg = next(catalog_iter)
        except StopIteration:
            break
        except sqlite3.DatabaseError as exc:
            print(
                f"  WARNING: skipping malformed catalog: {exc}\n"
                "           (re-run with --no-cache or clear the catalog cache"
                " to force a fresh download)",
                file=sys.stderr,
            )
            n_skipped += 1
            continue

        prefix_label = clg.root_prefix or "/"
        print(f"    catalog: {prefix_label}", file=sys.stderr)
        n_catalogs += 1

        # Bulk (whole-file) hashes.
        rows = clg.run_sql(
            "SELECT hash, size FROM catalog "
            "WHERE hash IS NOT NULL AND length(hash) > 0"
        )
        for h, size in rows:
            if h is not None and h not in hash_to_size:
                hash_to_size[h] = size or 0

        # Per-chunk hashes (schema >= 2.4 only).
        if include_chunks and clg.schema >= 2.4:
            try:
                rows = clg.run_sql(
                    "SELECT hash, size FROM chunks "
                    "WHERE hash IS NOT NULL AND length(hash) > 0"
                )
                for h, size in rows:
                    if h is not None and h not in hash_to_size:
                        hash_to_size[h] = size or 0
            except sqlite3.DatabaseError:
                pass  # chunks table absent in some old catalogs

        # Release the catalog so its file handle and SQLite connection are
        # closed promptly rather than accumulating for the entire run.
        rev.repository.close_catalog(clg)

    if n_skipped:
        print(
            f"  WARNING: {n_skipped} catalog(s) skipped due to errors —"
            " overlap results may be incomplete.",
            file=sys.stderr,
        )
    print(
        f"  → {n_catalogs} catalog(s), {len(hash_to_size):,} unique hash(es)",
        file=sys.stderr,
    )
    return hash_to_size


def _remove_silently(path):
    """Remove *path*, ignoring the error if it does not exist."""
    try:
        os.unlink(path)
    except OSError:
        pass


def _build_hash_db(rev, db_path, include_chunks):
    """
    Build a SQLite hash-set cache for *rev* at *db_path*.

    Schema::

        CREATE TABLE hashes (hash BLOB PRIMARY KEY, size INTEGER DEFAULT 0)
    """
    tmp = db_path + ".tmp"
    # Always start from a clean slate so that a stale .tmp left by an
    # interrupted previous run cannot cause "table already exists" errors.
    _remove_silently(tmp)
    db = sqlite3.connect(tmp)
    db.execute("PRAGMA synchronous = OFF").close()
    db.execute("PRAGMA journal_mode = MEMORY").close()
    db.execute(
        "CREATE TABLE hashes (hash BLOB PRIMARY KEY, size INTEGER DEFAULT 0)"
    ).close()

    hash_to_size = _collect_hashes(rev, include_chunks)
    db.executemany(
        "INSERT OR IGNORE INTO hashes(hash, size) VALUES (?, ?)",
        hash_to_size.items(),
    )
    db.commit()
    db.close()

    # Atomic rename so a partial write is never used.
    os.rename(tmp, db_path)
    return hash_to_size


def load_hash_index(rev, include_chunks, use_cache=True, cache_dir=None):
    """
    Return a ``{hash_bytes: size}`` dict for *rev*, using the on-disk cache
    when available (and *use_cache* is True).

    *cache_dir* overrides the default cache location
    (``~/.cache/cvmfs-overlap/v1/``).
    """
    cache_path = cache_dir if cache_dir is not None else DEFAULT_CACHE_PATH
    os.makedirs(cache_path, exist_ok=True)

    tag = "chunks" if include_chunks else "nochunks"
    db_path = os.path.join(cache_path, f"{rev.root_hash}_{tag}.db")

    if use_cache and os.path.exists(db_path):
        print(f"  Loading cached hash index: {db_path}", file=sys.stderr)
        db = sqlite3.connect(db_path)
        rows = db.execute("SELECT hash, size FROM hashes").fetchall()
        db.close()
        return dict(rows)

    # A .tmp file means a previous run was interrupted before it could be
    # renamed to .db.  If it contains valid data, promote it now so this
    # run — and any future ones — can skip the expensive rebuild entirely.
    # Pass --no-cache to discard it and force a fresh download.
    tmp_path = db_path + ".tmp"
    if use_cache and os.path.exists(tmp_path):
        try:
            db   = sqlite3.connect(tmp_path)
            rows = db.execute("SELECT hash, size FROM hashes").fetchall()
            db.close()
            if rows:
                print(
                    f"  Reusing partial hash index from a previous interrupted run"
                    f" ({len(rows):,} hashes).\n"
                    f"  Pass --no-cache to discard it and rebuild from scratch.",
                    file=sys.stderr,
                )
                os.rename(tmp_path, db_path)
                return dict(rows)
        except sqlite3.DatabaseError:
            pass
        # .tmp is corrupt or empty — remove it so the build starts clean.
        _remove_silently(tmp_path)

    print(
        f"  Building hash index (will be cached at {db_path}) …",
        file=sys.stderr,
    )
    return _build_hash_db(rev, db_path, include_chunks)


# ---------------------------------------------------------------------------
# Overlap computation
# ---------------------------------------------------------------------------

def compute_overlap(
    rev_a, hash_b, paths=None, top_n=20, include_chunks=True, follow_symlinks=False
):
    """
    Walk the selected catalogs of *rev_a* and compute overlap with *hash_b*.

    Parameters
    ----------
    rev_a:
        Revision object for repository A.
    hash_b:
        ``{hash_bytes: size}`` dict built from repository B.
    paths:
        Optional list of path strings, which may include fnmatch wildcards
        (e.g. ``["/software/art*", "/software/geant?"]``).  When *None* the
        entire repository is analysed.
    top_n:
        Number of top directories (by overlap size) to report.
    include_chunks:
        Whether to account for per-chunk hashes in addition to bulk hashes.
    follow_symlinks:
        When True, symlinks encountered during traversal are resolved and
        their targets are also traversed (with cycle detection).  Only
        targets that are not already covered by *paths* produce extra passes.

    Returns
    -------
    dict with keys:

    * ``total_files``           – regular files seen in repo A (selected)
    * ``total_size``            – their combined uncompressed size
    * ``unique_hashes_a``       – unique bulk content-hashes in repo A (selected)
    * ``overlap_files``         – files whose bulk hash also exists in repo B
    * ``overlap_size``          – their combined size
    * ``overlap_hashes``        – unique hashes that overlap
    * ``total_chunks``          – individual chunks seen in repo A
    * ``unique_chunks_a``       – unique chunk hashes in repo A
    * ``overlap_chunks``        – chunks whose hash also exists in repo B
    * ``overlap_chunk_hashes``  – unique chunk hashes that overlap
    * ``chunk_overlap_size``    – size represented by overlapping chunks
    * ``symlinks_followed``     – number of distinct symlink targets traversed
    * ``top_dirs``              – list of (path, stats_dict) sorted by overlap size
    """
    total_files          = 0
    total_size           = 0
    overlap_files        = 0
    overlap_size         = 0
    unique_hashes_a      = set()
    overlap_hashes       = set()

    total_chunks         = 0
    overlap_chunks       = 0
    unique_chunks_a      = set()
    overlap_chunk_hashes = set()
    chunk_overlap_size   = 0

    # dir_path → {"files": int, "size": int, "overlap_files": int, "overlap_size": int}
    dir_stats = defaultdict(lambda: {
        "files": 0, "size": 0, "overlap_files": 0, "overlap_size": 0
    })

    # Symlink following state.
    # followed_targets: absolute paths whose subtrees have already been (or are
    #   currently being) scanned.  Used for cycle detection.
    followed_targets = set()
    symlinks_followed = 0

    def _scan(effective_paths):
        """
        Scan catalogs relevant to *effective_paths*, update the counters, and
        return a set of symlink targets that were discovered but not yet
        covered by *effective_paths*.
        """
        nonlocal total_files, total_size, overlap_files, overlap_size
        nonlocal total_chunks, overlap_chunks, chunk_overlap_size

        new_symlink_targets = set()

        for clg in _iter_relevant_catalogs(rev_a, effective_paths):
            prefix_label = clg.root_prefix or "/"
            print(f"    catalog: {prefix_label}", file=sys.stderr)

            for entry_path, dirent in clg:
                if not _entry_is_relevant(entry_path, effective_paths):
                    continue

                # --- Symlink handling ---
                if dirent.is_symlink():
                    if follow_symlinks and dirent.symlink:
                        parent_dir = os.path.dirname(entry_path) or "/"
                        target = _resolve_symlink_target(parent_dir, dirent.symlink)
                        if (
                            target is not None
                            and target not in followed_targets
                            # Skip targets already covered by the current paths
                            # to avoid double-counting files in-scope of paths.
                            and not _entry_is_relevant(target, effective_paths)
                        ):
                            new_symlink_targets.add(target)
                    continue  # symlinks themselves have no content hash

                # --- Regular files only ---
                if not dirent.is_file() or not dirent.content_hash:
                    continue

                parent_dir = os.path.dirname(entry_path) or "/"
                hash_bytes = bytes.fromhex(dirent.content_hash)
                size       = dirent.size or 0

                total_files += 1
                total_size  += size
                unique_hashes_a.add(hash_bytes)
                dir_stats[parent_dir]["files"] += 1
                dir_stats[parent_dir]["size"]  += size

                if hash_bytes in hash_b:
                    overlap_files += 1
                    overlap_size  += size
                    overlap_hashes.add(hash_bytes)
                    dir_stats[parent_dir]["overlap_files"] += 1
                    dir_stats[parent_dir]["overlap_size"]  += size

                # Chunk-level overlap (populated by _read_chunks in catalog.py).
                if include_chunks and dirent.has_chunks():
                    for chunk in dirent.chunks:
                        ch         = chunk.content_hash  # bytes from the SQL BLOB
                        chunk_size = chunk.size or 0
                        total_chunks += 1
                        unique_chunks_a.add(ch)
                        if ch in hash_b:
                            overlap_chunks += 1
                            overlap_chunk_hashes.add(ch)
                            chunk_overlap_size += chunk_size

        return new_symlink_targets

    # --- Initial scan ---
    pending = _scan(paths)

    # --- Follow symlinks iteratively until no new targets are discovered ---
    if follow_symlinks:
        while pending:
            # Mark all pending targets as "being followed" before we start
            # scanning them so that circular references within the same batch
            # are detected immediately.
            followed_targets |= pending
            symlinks_followed += len(pending)

            next_pending: set = set()
            for target in sorted(pending):  # sorted for deterministic progress output
                print(
                    f"  Following symlink → {target}",
                    file=sys.stderr,
                )
                discovered = _scan([target])
                # Only queue targets not already followed or pending.
                next_pending |= discovered - followed_targets

            pending = next_pending

    top_dirs = sorted(
        dir_stats.items(),
        key=lambda kv: kv[1]["overlap_size"],
        reverse=True,
    )[:top_n]

    return {
        "total_files":          total_files,
        "total_size":           total_size,
        "unique_hashes_a":      len(unique_hashes_a),
        "overlap_files":        overlap_files,
        "overlap_size":         overlap_size,
        "overlap_hashes":       len(overlap_hashes),
        "total_chunks":         total_chunks,
        "unique_chunks_a":      len(unique_chunks_a),
        "overlap_chunks":       overlap_chunks,
        "overlap_chunk_hashes": len(overlap_chunk_hashes),
        "chunk_overlap_size":   chunk_overlap_size,
        "symlinks_followed":    symlinks_followed,
        "top_dirs":             top_dirs,
    }


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt_size(n):
    """Return a human-readable representation of *n* bytes."""
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    for unit in units[:-1]:
        if abs(n) < 1024.0:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} {units[-1]}"


def _pct(num, denom):
    return f"{100.0 * num / denom:.1f} %" if denom else "n/a"


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def print_report(r, repo_a_id, repo_b_id, paths, top_n, follow_symlinks):
    sep  = "=" * 66
    sep2 = "-" * 66

    print()
    print(sep)
    print("  CVMFS Repository Overlap Report")
    print(sep)
    print(f"  Repo A : {repo_a_id}")
    if paths:
        for p in paths:
            print(f"  Path   : {p}")
    else:
        print("  Paths  : (entire repository)")
    if follow_symlinks:
        print(f"  Symlinks followed : {r['symlinks_followed']}")
    print(f"  Repo B : {repo_b_id}")
    print(sep2)

    print()
    print("  [ File-level (bulk-hash) overlap ]")
    print()
    print(f"  {'Files in repo A (selected):':<34} {r['total_files']:>10,}")
    print(f"  {'Unique content hashes in repo A:':<34} {r['unique_hashes_a']:>10,}")
    print(f"  {'Total uncompressed size in repo A:':<34} {_fmt_size(r['total_size']):>12}")
    print()
    print(f"  {'Overlapping files:':<34} {r['overlap_files']:>10,}"
          f"   ({_pct(r['overlap_files'], r['total_files'])})")
    print(f"  {'Overlapping unique hashes:':<34} {r['overlap_hashes']:>10,}"
          f"   ({_pct(r['overlap_hashes'], r['unique_hashes_a'])})")
    print(f"  {'Overlap size:':<34} {_fmt_size(r['overlap_size']):>12}"
          f"   ({_pct(r['overlap_size'], r['total_size'])})")

    if r["total_chunks"] > 0:
        print()
        print("  [ Chunk-level overlap ]")
        print()
        print(f"  {'Chunks in repo A (selected):':<34} {r['total_chunks']:>10,}")
        print(f"  {'Unique chunk hashes in repo A:':<34} {r['unique_chunks_a']:>10,}")
        print()
        print(f"  {'Overlapping chunks:':<34} {r['overlap_chunks']:>10,}"
              f"   ({_pct(r['overlap_chunks'], r['total_chunks'])})")
        print(f"  {'Overlapping unique chunk hashes:':<34} {r['overlap_chunk_hashes']:>10,}"
              f"   ({_pct(r['overlap_chunk_hashes'], r['unique_chunks_a'])})")
        print(f"  {'Chunk overlap size:':<34} {_fmt_size(r['chunk_overlap_size']):>12}")

    top_dirs = [(d, s) for d, s in r["top_dirs"] if s["overlap_size"] > 0]
    if top_dirs:
        print()
        print(f"  [ Top {top_n} directories by overlap size ]")
        print()
        print(f"  {'Overlap size':>14}  {'%':>6}  {'Ovlp / Total files':<20}  Directory")
        print(f"  {'-'*14}  {'-'*6}  {'-'*20}  {'-'*42}")
        for dpath, stats in top_dirs:
            pct       = _pct(stats["overlap_files"], stats["files"])
            files_col = f"{stats['overlap_files']:>7} / {stats['files']:<9}"
            print(
                f"  {_fmt_size(stats['overlap_size']):>14}  "
                f"{pct:>6}  "
                f"{files_col:<20}  "
                f"{dpath}"
            )

    print()
    print(sep)
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Compute the content-hash overlap between two CVMFS repositories.\n\n"
            "All required catalogs are downloaded (and cached). The script reports\n"
            "how many hashes and how much data are shared between the selected\n"
            "part of repo A and all of repo B.\n\n"
            "Path arguments (--paths) support fnmatch wildcards:\n"
            "  *   matches any string (including path separators)\n"
            "  ?   matches any single character\n"
            "  []  matches a character class\n\n"
            "Examples:\n"
            "  --paths '/software/art*'          all art* packages\n"
            "  --paths '/software/*/lib'         lib dirs in any package\n"
            "  --paths '/sw/art' '/sw/geant[34]' two explicit selections"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "repo_a",
        help="Repository A: local FQRN, local path, or HTTP(S) URL. "
             "Overlap is measured for the (sub)catalogs of this repo.",
    )
    parser.add_argument(
        "repo_b",
        help="Repository B: local FQRN, local path, or HTTP(S) URL. "
             "Used as the reference hash set.",
    )
    parser.add_argument(
        "--paths",
        metavar="PATTERN",
        nargs="+",
        help="Restrict the analysis to these absolute path patterns in repo A. "
             "Supports fnmatch wildcards (* ? []).  If omitted the entire "
             "repository is analysed.",
    )
    parser.add_argument(
        "--follow-symlinks",
        dest="follow_symlinks",
        action="store_true",
        default=False,
        help="Follow symlinks encountered during traversal of repo A. "
             "Each symlink's resolved target is scanned as an additional "
             "subtree (with cycle detection to prevent infinite loops). "
             "Only targets not already covered by --paths produce extra passes.",
    )
    parser.add_argument(
        "--top",
        metavar="N",
        type=int,
        default=20,
        help="Show the top N directories with the greatest overlap (default: 20).",
    )
    parser.add_argument(
        "--no-chunks",
        dest="chunks",
        action="store_false",
        default=True,
        help="Ignore file chunks; only compare bulk (whole-file) hashes. "
             "Faster but may undercount overlap for large chunked files.",
    )
    parser.add_argument(
        "--no-cache",
        dest="cache",
        action="store_false",
        default=True,
        help="Do not use the on-disk hash-index cache for repo B. "
             "Useful when you suspect the cache is stale.",
    )
    parser.add_argument(
        "--cache-dir",
        metavar="DIR",
        default=None,
        help="Directory used to cache downloaded catalog files "
             "(passed to the CVMFS fetcher for both repos).",
    )
    parser.add_argument(
        "--index-cache-dir",
        metavar="DIR",
        default=None,
        help="Directory used to store the compiled hash-index for repo B "
             f"(default: {DEFAULT_CACHE_PATH}).",
    )

    args = parser.parse_args()

    # ------------------------------------------------------------------
    # Open both repositories
    # ------------------------------------------------------------------
    print(f"\nOpening repo A: {args.repo_a}", file=sys.stderr)
    repo_a = cvmfs.open_repository(args.repo_a, cache_dir=args.cache_dir)
    rev_a  = repo_a.get_current_revision()
    print(f"  revision {rev_a.revision_number}, root hash: {rev_a.root_hash}",
          file=sys.stderr)

    print(f"\nOpening repo B: {args.repo_b}", file=sys.stderr)
    repo_b = cvmfs.open_repository(args.repo_b, cache_dir=args.cache_dir)
    rev_b  = repo_b.get_current_revision()
    print(f"  revision {rev_b.revision_number}, root hash: {rev_b.root_hash}",
          file=sys.stderr)

    # ------------------------------------------------------------------
    # Build / load the hash index for repo B
    # ------------------------------------------------------------------
    print("\nBuilding hash index for repo B …", file=sys.stderr)
    t0     = time.monotonic()
    hash_b = load_hash_index(
        rev_b,
        include_chunks=args.chunks,
        use_cache=args.cache,
        cache_dir=args.index_cache_dir,
    )
    print(f"  {len(hash_b):,} unique hash(es) in {time.monotonic() - t0:.1f}s",
          file=sys.stderr)

    # ------------------------------------------------------------------
    # Walk repo A and compute overlap
    # ------------------------------------------------------------------
    if args.paths:
        paths_label = ", ".join(args.paths)
    else:
        paths_label = "(entire repository)"
    print(f"\nAnalysing repo A — {paths_label} …", file=sys.stderr)
    if args.follow_symlinks:
        print("  (symlink following enabled)", file=sys.stderr)

    t0     = time.monotonic()
    result = compute_overlap(
        rev_a,
        hash_b,
        paths=args.paths,
        top_n=args.top,
        include_chunks=args.chunks,
        follow_symlinks=args.follow_symlinks,
    )
    print(f"  Done in {time.monotonic() - t0:.1f}s", file=sys.stderr)

    # ------------------------------------------------------------------
    # Print the report
    # ------------------------------------------------------------------
    print_report(
        result, args.repo_a, args.repo_b, args.paths, args.top, args.follow_symlinks
    )


if __name__ == "__main__":
    main()
