#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import datetime
import json
import pathlib
import re
import sys


SUPPORT_DAYS = 90
RELEASE_TAG = re.compile(r"src/libraries/rust/stargate/v[0-9]+\.[0-9]+\.[0-9]+")


def select_release(releases, retained, now):
    cutoff = now - datetime.timedelta(days=SUPPORT_DAYS)
    stable = {}
    for release in releases:
        tag = release["tag_name"]
        if not RELEASE_TAG.fullmatch(tag) or release["draft"] or release["prerelease"]:
            continue
        published = datetime.datetime.fromisoformat(release["published_at"].replace("Z", "+00:00"))
        if published.tzinfo is None:
            raise ValueError(f"release publication date must include a timezone: {tag}")
        if published <= now:
            stable[tag] = (published, release)
    for tag in retained:
        if tag not in stable:
            raise ValueError(f"retained release has no published stable release metadata: {tag}")
    supported = [entry for tag, entry in stable.items() if entry[0] >= cutoff or tag in retained]
    if not supported:
        raise ValueError("no stable release in the 90-day window or retained release list")
    published, release = min(supported, key=lambda entry: (entry[0], entry[1]["tag_name"]))
    return {
        "tag": release["tag_name"],
        "published_at": published.isoformat(),
        "support_days": SUPPORT_DAYS,
        "as_of": now.isoformat(),
        "cutoff": cutoff.isoformat(),
    }


def main():
    releases = [json.loads(line) for line in sys.stdin if line.strip()]
    policy = pathlib.Path(__file__).resolve().parents[1] / "compatibility_retained_releases.json"
    retained = json.loads(policy.read_text())
    if not isinstance(retained, list) or any(not isinstance(tag, str) for tag in retained):
        raise ValueError("retained releases must be a JSON array of stable release tags")
    print(json.dumps(select_release(releases, retained, datetime.datetime.now(datetime.timezone.utc))))


if __name__ == "__main__":
    main()
