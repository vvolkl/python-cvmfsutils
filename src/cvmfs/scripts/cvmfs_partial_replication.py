#!/usr/bin/env python3
"""
Estimate the backend storage freed on a stratum 1 by partial replication.

Given a repository and an inclusion spec (the file passed to
``cvmfs_swissknife pull -E`` / ``CVMFS_PARTIAL_REPLICATION_SPEC``), this tool
walks the full catalog tree of the current revision and determines which
objects a partial replica would store and which it would skip.

Selection mirrors ``CommandPull``:
  * The root catalog is always replicated.
  * A nested catalog whose mountpoint is excluded by the spec is pruned
    together with its entire subtree (catalogs and data objects).
  * Every replicated catalog has all its objects pulled
    (``SqlAllChunks``: bulk hashes, nested catalog hashes and file chunks).

An object is only freed if no replicated catalog references it, so content
shared between included and excluded subtrees is not counted as freed.

Sizes are the uncompressed sizes recorded in the catalogs.  The backend stores
compressed objects; --sample-compressed estimates compressed sizes from the
stored size of random samples of kept and freed objects.
"""

import argparse
import os
import re
import sqlite3
import sys
import time
from collections import defaultdict

import cvmfs
from cvmfs.scripts.cvmfs_overlap import (
    Reservoir, _fmt_size, _pct, estimate_compression_ratio, fmt_ratio_line,
    object_name, walk_catalogs,
)

# Flags from cvmfs/catalog_sql.h
FLAG_FILE          = 4
FLAG_FILE_CHUNK    = 64
FLAG_FILE_EXTERNAL = 128

SUPPORTED_SPEC_VERSION = 1


# ---------------------------------------------------------------------------
# Inclusion spec (port of cvmfs/path_filters/{inclusion_spec,relaxed_path_filter,
# dirtab}.cc and cvmfs/pathspec)
# ---------------------------------------------------------------------------

def _pathspec_regex(spec, relaxed):
    """
    Translate a pathspec into a regex.  Strict wildcards stay within one path
    element; relaxed ones (used for negations) may cross ``/``.
    """
    star = ".*" if relaxed else "[^/]*"
    qmark = "." if relaxed else "[^/]"
    absolute = spec.startswith("/")
    elements = [e for e in spec.split("/") if e]
    parts = []
    for elem in elements:
        out = ""
        escaped = False
        for c in elem:
            if escaped:
                out += re.escape(c)
                escaped = False
            elif c == "\\":
                escaped = True
            elif c == "*":
                out += star
            elif c == "?":
                out += qmark
            else:
                out += re.escape(c)
        parts.append(out)
    return re.compile("^" + ("/" if absolute else "") + "/".join(parts) + "/?$")


def _parent_paths(path):
    """Yield the proper ancestors of *path*, deepest first, ending with ''."""
    while path:
        path = path[: path.rfind("/")]
        yield path


class InclusionSpec:
    def __init__(self, text):
        lines = text.split("\n")
        version_idx = None
        for i, line in enumerate(lines):
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            m = re.fullmatch(r"version\s+(\d+)", s)
            if not m or not s.startswith("version "):
                raise ValueError(
                    f"first non-comment line must be 'version N', got '{s}'")
            self.version = int(m.group(1))
            version_idx = i
            break
        if version_idx is None:
            raise ValueError("spec file is empty or contains only comments")
        if self.version != SUPPORTED_SPEC_VERSION:
            raise ValueError(
                f"unsupported version {self.version}"
                f" (expected {SUPPORTED_SPEC_VERSION})")

        self.rules = []          # (raw_line, is_negation)
        exact = []               # positive rules as written
        positive = []            # positive rules plus all their parent paths
        negative = []
        for line in lines[version_idx + 1:]:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            neg = s.startswith("!")
            spec = s[1:].strip() if neg else s
            if not spec:
                continue
            if not neg and not spec.startswith("/"):
                raise ValueError(f"inclusion path must be absolute: '{spec}'")
            self.rules.append((s, neg))
            if neg:
                negative.append(_pathspec_regex(spec, relaxed=True))
                continue
            exact.append(_pathspec_regex(spec, relaxed=False))
            # RelaxedPathFilter: listing a path also includes its parents.
            cur = spec
            while cur:
                positive.append(_pathspec_regex(cur, relaxed=False))
                cur = cur[: cur.rfind("/")]
        self._exact = exact
        self._positive = positive
        self._negative = negative

    def _is_opposing(self, path):
        for p in [path, *_parent_paths(path)]:
            if p and any(r.match(p) for r in self._negative):
                return True
        return False

    def is_excluded(self, path):
        """Mirror of ``InclusionSpec::IsExcluded``."""
        path = path.rstrip("/")
        if not path:
            return False
        included = any(r.match(path) for r in self._positive)
        if not included:
            # Sub paths of listed paths are included as well.
            included = any(
                any(r.match(p) for r in self._exact)
                for p in _parent_paths(path) if p
            )
        return not (included and not self._is_opposing(path))


# ---------------------------------------------------------------------------
# Catalog walk
# ---------------------------------------------------------------------------

def _catalog_objects(clg, include_legacy_bulk):
    """
    Yield ``(object_name, size)`` for every object a pull of *clg* fetches,
    following ``SqlAllChunks``.
    """
    skip = FLAG_FILE_EXTERNAL | (0 if include_legacy_bulk else FLAG_FILE_CHUNK)
    rows = clg.run_sql(
        "SELECT hash, size, flags FROM catalog "
        f"WHERE hash IS NOT NULL AND length(hash) > 0 AND (flags & {skip}) = 0"
    )
    for h, size, flags in rows:
        suffix = "" if flags & FLAG_FILE else "L"
        yield object_name(h, flags, suffix), size or 0

    if clg.schema >= 2.4:
        try:
            rows = clg.run_sql(
                "SELECT chunks.hash, chunks.size, catalog.flags FROM chunks, catalog "
                "WHERE chunks.md5path_1 = catalog.md5path_1 "
                "AND chunks.md5path_2 = catalog.md5path_2 "
                f"AND (catalog.flags & {FLAG_FILE_EXTERNAL}) = 0"
            )
        except sqlite3.DatabaseError:
            return
        for h, size, flags in rows:
            yield object_name(h, flags, "P"), size or 0


def analyse(rev, spec, include_legacy_bulk, sample_n=0, jobs=1):
    """
    Walk all catalogs of *rev* and classify objects as kept or freed.

    Returns a dict with totals and per-pruned-subtree freed sizes.  With
    *sample_n* > 0, also returns random samples of up to *sample_n* kept and
    freed ``(object_name, size)`` pairs each.
    """
    # key -> [size, owner]; owner is None once any replicated catalog
    # references the object, otherwise the pruned subtree root it came from.
    objects = {}
    catalogs_kept = 0
    catalogs_pruned = 0
    catalog_bytes_kept = 0
    catalog_bytes_pruned = 0
    pruned_roots = []

    def expand(clg, ctx):
        _, owner = ctx
        children = []
        for ref in clg.list_nested():
            child_owner = owner
            if owner is None and spec.is_excluded(ref.root_path):
                child_owner = ref.root_path
                pruned_roots.append(ref.root_path)
            children.append((ref.hash, (ref.size or 0, child_owner)))
        return children

    # ctx: (catalog size, pruned subtree root or None)
    for clg, (cat_size, owner) in walk_catalogs(
            rev.repository, rev.root_hash, expand, jobs, root_ctx=(0, None)):
        label = clg.root_prefix or "/"
        print(f"    catalog: {label}{' (pruned)' if owner else ''}",
              file=sys.stderr)
        if owner is None:
            catalogs_kept += 1
            catalog_bytes_kept += cat_size
        else:
            catalogs_pruned += 1
            catalog_bytes_pruned += cat_size

        for key, size in _catalog_objects(clg, include_legacy_bulk):
            entry = objects.get(key)
            if entry is None:
                objects[key] = [size, owner]
            elif owner is None:
                entry[1] = None

    total_objects = len(objects)
    total_size = 0
    freed_objects = 0
    freed_size = 0
    freed_by_root = defaultdict(lambda: [0, 0])
    kept_sample = Reservoir(sample_n)
    freed_sample = Reservoir(sample_n)
    for key, (size, owner) in objects.items():
        total_size += size
        if sample_n:
            (kept_sample if owner is None else freed_sample).add((key, size))
        if owner is not None:
            freed_objects += 1
            freed_size += size
            freed_by_root[owner][0] += 1
            freed_by_root[owner][1] += size

    return {
        "total_objects":        total_objects,
        "total_size":           total_size,
        "freed_objects":        freed_objects,
        "freed_size":           freed_size,
        "catalogs_kept":        catalogs_kept,
        "catalogs_pruned":      catalogs_pruned,
        "catalog_bytes_kept":   catalog_bytes_kept,
        "catalog_bytes_pruned": catalog_bytes_pruned,
        "pruned_roots":         pruned_roots,
        "freed_by_root":        dict(freed_by_root),
        "kept_sample":          kept_sample.items,
        "freed_sample":         freed_sample.items,
    }


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def print_report(r, repo_id, spec_path, spec, top_n, kept_ratio=None,
                 freed_ratio=None):
    sep  = "=" * 66
    sep2 = "-" * 66

    print()
    print(sep)
    print("  CVMFS Partial Replication Savings Report")
    print(sep)
    print(f"  Repo : {repo_id}")
    print(f"  Spec : {spec_path} (version {spec.version})")
    for line, _ in spec.rules:
        print(f"         {line}")
    print(sep2)

    print()
    print("  [ Catalogs ]")
    print()
    total_cat = r["catalogs_kept"] + r["catalogs_pruned"]
    print(f"  {'Catalogs in repository:':<34} {total_cat:>10,}")
    print(f"  {'Replicated:':<34} {r['catalogs_kept']:>10,}")
    print(f"  {'Pruned:':<34} {r['catalogs_pruned']:>10,}"
          f"   ({_pct(r['catalogs_pruned'], total_cat)})")
    print(f"  {'Pruned subtree roots:':<34} {len(r['pruned_roots']):>10,}")
    if r["catalog_bytes_pruned"]:
        print(f"  {'Pruned catalog size:':<34}"
              f" {_fmt_size(r['catalog_bytes_pruned']):>12}")

    print()
    print("  [ Data objects (unique) ]")
    print()
    kept_objects = r["total_objects"] - r["freed_objects"]
    kept_size = r["total_size"] - r["freed_size"]
    print(f"  {'Full replica:':<34} {r['total_objects']:>10,}"
          f"   {_fmt_size(r['total_size']):>12}")
    print(f"  {'Partial replica:':<34} {kept_objects:>10,}"
          f"   {_fmt_size(kept_size):>12}")
    print(f"  {'Freed:':<34} {r['freed_objects']:>10,}"
          f"   {_fmt_size(r['freed_size']):>12}"
          f"   ({_pct(r['freed_size'], r['total_size'])})")
    print()
    print("  Sizes are uncompressed (as recorded in the catalogs).")
    if kept_ratio is not None:
        print(fmt_ratio_line("Partial replica, compressed (est.):",
                             kept_size, kept_ratio))
    if freed_ratio is not None:
        print(fmt_ratio_line("Freed, compressed (est.):",
                             r["freed_size"], freed_ratio))

    top = sorted(r["freed_by_root"].items(), key=lambda kv: kv[1][1],
                 reverse=True)[:top_n]
    if top:
        print()
        print(f"  [ Top {top_n} pruned subtrees by freed size ]")
        print()
        print(f"  {'Freed size':>14}  {'Objects':>10}  Subtree")
        print(f"  {'-'*14}  {'-'*10}  {'-'*38}")
        for root, (n, size) in top:
            print(f"  {_fmt_size(size):>14}  {n:>10,}  {root}")

    print()
    print(sep)
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Estimate how much backend storage a stratum 1 saves by replicating\n"
            "a repository partially with the given inclusion spec.\n\n"
            "Spec format (as for CVMFS_PARTIAL_REPLICATION_SPEC):\n"
            "  version 1\n"
            "  /sw/included\n"
            "  !/sw/included/but_not_this\n\n"
            "Only nested catalog mountpoints are evaluated against the spec;\n"
            "excluded catalogs are pruned with their whole subtree."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "repo",
        help="Repository: local FQRN, local path, or HTTP(S) URL.",
    )
    parser.add_argument(
        "spec",
        help="Path to the partial replication inclusion spec file.",
    )
    parser.add_argument(
        "--top",
        metavar="N",
        type=int,
        default=20,
        help="Show the top N pruned subtrees by freed size (default: 20).",
    )
    parser.add_argument(
        "--legacy-bulk-chunks",
        dest="legacy_bulk",
        action="store_true",
        default=False,
        help="Also count bulk hashes of chunked files, as pull does with "
             "CVMFS_NO_IGNORE_LEGACY_BULKHASHES set.",
    )
    parser.add_argument(
        "--cache-dir",
        metavar="DIR",
        default=None,
        help="Directory used to cache downloaded catalog files.",
    )
    parser.add_argument(
        "-j", "--jobs",
        metavar="N",
        type=int,
        default=8,
        help="Number of parallel catalog downloads (default: 8).",
    )
    parser.add_argument(
        "--sample-compressed",
        metavar="N",
        type=int,
        default=0,
        help="Estimate compressed partial replica and freed sizes by "
             "measuring the stored size of N randomly sampled kept and N "
             "freed objects in the backend "
             "(HEAD requests for HTTP repos).",
    )

    args = parser.parse_args()

    try:
        with open(args.spec) as f:
            spec = InclusionSpec(f.read())
    except (OSError, ValueError) as exc:
        parser.error(f"cannot load spec '{args.spec}': {exc}")

    print(f"\nOpening repo: {args.repo}", file=sys.stderr)
    repo = cvmfs.open_repository(args.repo, cache_dir=args.cache_dir)
    rev  = repo.get_current_revision()
    print(f"  revision {rev.revision_number}, root hash: {rev.root_hash}",
          file=sys.stderr)

    print("\nWalking catalogs …", file=sys.stderr)
    t0 = time.monotonic()
    result = analyse(rev, spec, args.legacy_bulk, args.sample_compressed,
                     args.jobs)
    print(f"  Done in {time.monotonic() - t0:.1f}s", file=sys.stderr)

    kept_ratio = freed_ratio = None
    if args.sample_compressed > 0:
        print("\nMeasuring stored size of sampled objects …", file=sys.stderr)
        kept_ratio = estimate_compression_ratio(repo, result["kept_sample"])
        freed_ratio = estimate_compression_ratio(repo, result["freed_sample"])

    print_report(result, args.repo, os.path.abspath(args.spec), spec, args.top,
                 kept_ratio, freed_ratio)


if __name__ == "__main__":
    main()
