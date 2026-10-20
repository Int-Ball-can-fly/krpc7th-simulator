#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
archive_name="${1:-my_submission}"

# Keep archive members rooted at catkin_ws/src regardless of the caller's cwd.
tar -czvf "${archive_name}.tar.gz" -C "$script_dir" catkin_ws/src
