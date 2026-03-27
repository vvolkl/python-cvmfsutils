#!/usr/bin/env python3
"""
Analyse unpacked container images on a CVMFS repository.

For repositories like unpacked.cern.ch, this script discovers all container
images by walking the catalog tree, then downloads /etc/os-release (and
/etc/redhat-release) for each image to classify them by OS/platform.

Directory structure expected:
  /<registry>/<namespace>/[<subnamespace>/]<image>:<tag>/  (a root filesystem)

The script works entirely via the CVMFS HTTP protocol and catalogs — it does
not require a local CVMFS mount.
"""

import argparse
import collections
import os
import re
import sys
import zlib

import cvmfs
from cvmfs._exceptions import FileNotFoundInRepository


# Top-level entries to skip (not registries)
SKIP_TOPLEVEL = {"README.md", "logDir", "util", ".cvmfscatalog", ".cvmfsautocatalog"}

# Known registries (heuristic: contains a dot or colon)
def _looks_like_registry(name):
    return "." in name or ":" in name


def parse_os_release(content):
    """Parse os-release file content into a dict."""
    info = {}
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        info[key] = value
    return info


def classify_os(os_release_info, redhat_release_content=None):
    """Return a short OS classification string from os-release fields."""
    name = os_release_info.get("ID", "").lower()
    version = os_release_info.get("VERSION_ID", "")
    pretty = os_release_info.get("PRETTY_NAME", "")

    if name and version:
        return f"{name} {version}"
    elif pretty:
        return pretty
    elif name:
        return name
    elif redhat_release_content:
        return redhat_release_content.strip().split("\n")[0]
    return "unknown"


def retrieve_file_content(repo, dirent):
    """Download a regular file object and return its content as a string."""
    try:
        if dirent.has_chunks():
            parts = []
            for chunk in dirent.chunks:
                hash_str = chunk.content_hash_string()
                f = repo.retrieve_object(hash_str)
                parts.append(f.read())
                f.close()
            return b"".join(parts).decode("utf-8", errors="replace")
        else:
            hash_str = dirent.content_hash_string()
            f = repo.retrieve_object(hash_str)
            data = f.read()
            f.close()
            return data.decode("utf-8", errors="replace")
    except Exception:
        return None


def discover_images_from_catalogs(revision):
    """
    Walk the catalog tree and discover image root paths.

    An image is identified as a directory at depth 3 or 4 under / whose name
    contains a colon (image:tag pattern), e.g.:
        /registry.hub.docker.com/library/ubuntu:22.04
        /registry.cern.ch/atlas/athena:21.0.129.sw17-0
    """
    images = []
    root_catalog = revision.retrieve_root_catalog()

    # Level 1: registries
    for entry in root_catalog.list_directory("/"):
        if entry.name in SKIP_TOPLEVEL:
            continue
        if not entry.is_directory():
            continue
        if not _looks_like_registry(entry.name):
            continue

        registry = entry.name
        registry_path = f"/{registry}"

        # We need the right catalog for this path
        _collect_images_under(revision, registry_path, depth=1, images=images)

    return images


def _collect_images_under(revision, path, depth, images, max_depth=4):
    """Recursively list directories looking for image:tag dirs."""
    try:
        clg = revision.retrieve_catalog_for_path(path)
        entries = list(clg.list_directory(path))
    except Exception:
        return

    for entry in entries:
        if not entry.is_directory():
            continue
        child_path = f"{path}/{entry.name}"
        if ":" in entry.name:
            # This looks like an image:tag directory
            images.append(child_path)
        elif depth < max_depth:
            _collect_images_under(revision, child_path, depth + 1, images, max_depth)


def analyze_image(revision, repo, image_path):
    """
    For a given image root path, try to read /etc/os-release and
    /etc/redhat-release to classify the OS.
    """
    os_release_path = f"{image_path}/etc/os-release"
    redhat_release_path = f"{image_path}/etc/redhat-release"

    os_release_content = None
    redhat_release_content = None

    # Try os-release
    try:
        clg = revision.retrieve_catalog_for_path(os_release_path)
        dirent = clg.find_directory_entry(os_release_path)
        if dirent is not None:
            if dirent.is_symlink():
                # os-release is often a symlink to /usr/lib/os-release
                target = dirent.symlink
                if not target.startswith("/"):
                    target = os.path.normpath(os.path.join(
                        os.path.dirname(os_release_path), target))
                else:
                    target = f"{image_path}{target}"
                clg2 = revision.retrieve_catalog_for_path(target)
                dirent2 = clg2.find_directory_entry(target)
                if dirent2 is not None and dirent2.is_file():
                    os_release_content = retrieve_file_content(repo, dirent2)
            elif dirent.is_file():
                os_release_content = retrieve_file_content(repo, dirent)
    except Exception:
        pass

    # Try redhat-release as fallback
    try:
        clg = revision.retrieve_catalog_for_path(redhat_release_path)
        dirent = clg.find_directory_entry(redhat_release_path)
        if dirent is not None:
            if dirent.is_symlink():
                target = dirent.symlink
                if not target.startswith("/"):
                    target = os.path.normpath(os.path.join(
                        os.path.dirname(redhat_release_path), target))
                else:
                    target = f"{image_path}{target}"
                clg2 = revision.retrieve_catalog_for_path(target)
                dirent2 = clg2.find_directory_entry(target)
                if dirent2 is not None and dirent2.is_file():
                    redhat_release_content = retrieve_file_content(repo, dirent2)
            elif dirent.is_file():
                redhat_release_content = retrieve_file_content(repo, dirent)
    except Exception:
        pass

    if os_release_content:
        info = parse_os_release(os_release_content)
        return classify_os(info, redhat_release_content)
    elif redhat_release_content:
        return redhat_release_content.strip().split("\n")[0]
    else:
        return "unknown"


def main():
    parser = argparse.ArgumentParser(
        description="Analyse unpacked container images on a CVMFS repository "
                    "by inspecting /etc/os-release in each image."
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
        "--registry",
        default=None,
        help="Only analyse images from this registry (e.g. registry.hub.docker.com)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print per-image classification"
    )
    args = parser.parse_args()

    repo_url = args.repo_url
    repo_name = repo_url.rstrip("/").split("/")[-1]

    base_cache = args.cache_dir or os.path.join(os.getcwd(), "cache")
    cache_dir = os.path.join(base_cache, repo_name)
    os.makedirs(cache_dir, exist_ok=True)

    print(f"Repository : {repo_url}")
    print(f"Cache dir  : {cache_dir}")

    repo = cvmfs.open_repository(repo_url, cache_dir=cache_dir)
    revision = repo.get_current_revision()
    print(f"Revision   : {revision.revision_number}")
    print(f"Root hash  : {revision.root_hash}")
    print()

    # Discover images
    print("Discovering images...")
    images = discover_images_from_catalogs(revision)

    if args.registry:
        images = [img for img in images if img.startswith(f"/{args.registry}/")]

    print(f"Found {len(images)} images")
    print()

    # Analyse each image
    os_counts = collections.Counter()
    registry_counts = collections.Counter()
    errors = 0

    for i, image_path in enumerate(sorted(images), 1):
        try:
            os_class = analyze_image(revision, repo, image_path)
        except Exception as e:
            os_class = "error"
            errors += 1
            if args.verbose:
                print(f"  [{i:4d}/{len(images)}] {image_path}  -> ERROR: {e}")
                continue

        os_counts[os_class] += 1
        # Extract registry
        parts = image_path.strip("/").split("/")
        if parts:
            registry_counts[parts[0]] += 1

        if args.verbose:
            print(f"  [{i:4d}/{len(images)}] {image_path}  -> {os_class}")

        # Progress indicator (non-verbose)
        if not args.verbose and i % 50 == 0:
            print(f"  Analysed {i}/{len(images)} images...", file=sys.stderr)

    # Print statistics
    print()
    print("=" * 70)
    print("OS / Platform Distribution")
    print("=" * 70)
    for os_name, count in os_counts.most_common():
        pct = 100.0 * count / len(images) if images else 0
        bar = "█" * int(pct / 2)
        print(f"  {os_name:40s} {count:5d}  ({pct:5.1f}%)  {bar}")

    print()
    print("=" * 70)
    print("Images per Registry")
    print("=" * 70)
    for reg, count in registry_counts.most_common():
        print(f"  {reg:40s} {count:5d}")

    print()
    print(f"Total images : {len(images)}")
    print(f"Classified   : {len(images) - os_counts.get('unknown', 0) - errors}")
    print(f"Unknown      : {os_counts.get('unknown', 0)}")
    print(f"Errors       : {errors}")


if __name__ == "__main__":
    main()
