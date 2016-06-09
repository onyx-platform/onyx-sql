#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

git clone https://github.com/onyx-platform/onyx-release-scripts.git
bash onyx-release-scripts/release_plugin.sh "$@"
