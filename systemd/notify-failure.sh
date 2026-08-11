#!/usr/bin/env bash
# Posts a failure alert to the shared cdsci-lake-ops ntfy topic. Invoked by
# ntfy-notify@%N.service (see that unit) with the failing unit's name as $1.
#
# The token is fetched from GSM at call time and never written to disk --
# same "secrets always come from GSM" rule this repo already follows for
# R2/Postgres creds (see cdsci.lake.config). See monode/infrastructure's
# SCHEDULING.md for the full convention this script is part of.
set -euo pipefail

unit="${1:?usage: notify-failure.sh <unit-name>}"
token=$(gcloud secrets versions access latest \
  --secret=cdsci-ntfy-lake-ops-service-token --project=cdsci-infra)

curl -fsS -X POST "https://ntfy.cancerdatasci.org/cdsci-lake-ops" \
  -H "Authorization: Bearer ${token}" \
  -H "Title: cdsci-lake job failed: ${unit}" \
  -H "Priority: high" \
  -H "Tags: rotating_light" \
  -d "systemd unit ${unit} failed on $(hostname). journalctl --user -u ${unit} for detail."
