#!/usr/bin/env bash
snapcraft
snapcraft push --release=stable dynocsv_1.1.3_amd64.snap

# sudo snap install --dangerous dynocsv_1.1.0_amd64.snap

# Debug
# snap debug confinement
# snap debug sandbox-features
