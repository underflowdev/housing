#!/usr/bin/env bash
# Deploy to S3. Configure scripts/deploy.env (see deploy.env.example).
#
# Shares one S3 bucket + CloudFront distribution with the peer sun_overlay
# and cu_salary projects, each under its own PREFIX - same convention as
# their scripts/deploy.sh.
#
# web/ is the entire deployable unit (see CLAUDE.md) - including web/data/,
# which must be freshly built before deploying:
#   python build_income.py && python create_output.py && python web_data_build.py

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

ENV_FILE="$SCRIPT_DIR/deploy.env"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "Error: $ENV_FILE not found. Copy deploy.env.example and fill in your values." >&2
  exit 1
fi
# shellcheck source=deploy.env.example
source "$ENV_FILE"

PREFIX="housing"

if [[ ! -f "$REPO_ROOT/web/data/dataset.json" ]]; then
  echo "Error: web/data/dataset.json missing. Run the build pipeline first:" >&2
  echo "  python build_income.py && python create_output.py && python web_data_build.py" >&2
  exit 1
fi

echo "Syncing web/ -> s3://$BUCKET/$PREFIX/"
aws s3 sync "$REPO_ROOT/web/" "s3://$BUCKET/$PREFIX/" \
  --delete \
  --cache-control "max-age=300"

echo "Invalidating CloudFront distribution..."
aws cloudfront create-invalidation \
  --distribution-id "$CLOUDFRONT_DISTRIBUTION_ID" \
  --paths "/$PREFIX/*"

echo "Done. Site live at https://$BUCKET/$PREFIX"
