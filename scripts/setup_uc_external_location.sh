#!/usr/bin/env bash
# Gives Unity Catalog read access to the S3 events prefix.
#
# The IAM role and its trust policy come from aws/template.yaml; this script
# only creates the Databricks side. Unity Catalog requires the role to be
# self-assuming, and the role can only trust itself once it exists, so the
# setup takes two runs of this script with a redeploy in between:
#
#   1. `sam deploy` with SelfAssume=false creates the bucket and the role.
#   2. This script creates the storage credential, then stops.
#   3. `sam deploy` with SelfAssume=true adds the self-trust.
#   4. This script again: it creates the external location and validates it.
#
# File events are off on the external location. They would need the role to
# create SNS and SQS resources, and Auto Loader can list a prefix that only
# gets a few objects a day.
#
#     $ scripts/setup_uc_external_location.sh private
set -euo pipefail

PROFILE="${1:?usage: $0 <databricks-cli-profile>}"
ACCOUNT_ID="${AWS_ACCOUNT_ID:-068325471140}"
BUCKET="${EVENTS_BUCKET:-car-trip-events-${ACCOUNT_ID}}"
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/car-trip-events-databricks-reader"
CREDENTIAL_NAME="car_trip_events_reader"
LOCATION_NAME="car_trip_events_location"

if ! databricks storage-credentials get "$CREDENTIAL_NAME" --profile "$PROFILE" >/dev/null 2>&1; then
  echo "==> creating storage credential ${CREDENTIAL_NAME}"
  databricks storage-credentials create --profile "$PROFILE" --json "$(cat <<JSON
{
  "name": "${CREDENTIAL_NAME}",
  "comment": "Reads the trip events the ingest Lambda writes to S3",
  "read_only": true,
  "aws_iam_role": {"role_arn": "${ROLE_ARN}"}
}
JSON
)"

  EXTERNAL_ID=$(databricks storage-credentials get "$CREDENTIAL_NAME" --profile "$PROFILE" -o json \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["aws_iam_role"]["external_id"])')

  echo
  echo "==> Unity Catalog will present external id ${EXTERNAL_ID}"
  echo "    Now make the role self-assuming. Run 'sam deploy --guided' in aws/ and set"
  echo "    SelfAssume=true (and DatabricksAccountId=${EXTERNAL_ID} if it differs)."
  echo "    The guided deploy saves both to samconfig.toml, so later deploys keep them."
  echo "    Then run this script again."
  exit 0
fi

echo "==> storage credential ${CREDENTIAL_NAME} exists"

if ! databricks external-locations get "$LOCATION_NAME" --profile "$PROFILE" >/dev/null 2>&1; then
  echo "==> creating external location ${LOCATION_NAME}"
  databricks external-locations create --profile "$PROFILE" --json "$(cat <<JSON
{
  "name": "${LOCATION_NAME}",
  "url": "s3://${BUCKET}/",
  "credential_name": "${CREDENTIAL_NAME}",
  "read_only": true,
  "enable_file_events": false,
  "comment": "Trip events landed by the ingest Lambda"
}
JSON
)"
fi

echo "==> validating"
databricks storage-credentials validate --profile "$PROFILE" \
  --storage-credential-name "$CREDENTIAL_NAME" \
  --external-location-name "$LOCATION_NAME" \
  --read-only

echo "done. The pipeline can now read s3://${BUCKET}/events/"
