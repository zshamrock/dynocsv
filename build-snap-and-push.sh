#!/usr/bin/env bash
snapcraft
snapcraft push --release=stable dynocsv_1.0.0_amd64.snap

# sudo snap install --dangerous dynocsv_1.0.0_amd64.snap

# Debug
# snap debug confinement
# snap debug sandbox-features
