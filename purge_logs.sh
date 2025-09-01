#!/usr/bin/env bash
set -euo pipefail
KEEP_HOURS="${RETENTION_HOURS:-48}"
find audio_segments -type f -name "*.wav" -mmin +$((KEEP_HOURS*60)) -delete 2>/dev/null || true
find transcripts -type f -name "*.txt" -mmin +$((KEEP_HOURS*60)) -delete 2>/dev/null || true
