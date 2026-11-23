# Trip event ingest

The AWS half of car_trip_tracker. An iOS Shortcut posts a trip start or stop
event to a Lambda Function URL, and the Lambda writes it to S3 as one JSON
object. The Databricks pipeline reads that prefix with Auto Loader.

```
iOS Shortcut --HTTPS + X-Api-Key--> Lambda (car-trip-ingest)
                                         |  PUT
                                         v
            s3://car-trip-events-<account>/events/<event_id>.json
```

Ingest is on AWS so that logging a trip never depends on Databricks compute
being awake. The pipeline only has to run once an hour; the endpoint has to
answer at any time of day.

## Layout

| Path | What it is |
| --- | --- |
| `template.yaml` | SAM template: the bucket, the Lambda, its Function URL and the IAM role Unity Catalog assumes |
| `samconfig.toml` | Deploy settings: stack name, region, AWS CLI profile, IAM capabilities, `SelfAssume=true` |
| `src/trip_event.py` | Pure logic: unwrap the posted body, validate it, derive the event id and the S3 key. No boto3 |
| `src/handler.py` | The Lambda: check the shared secret, call `trip_event`, write to S3 |
| `tests/` | Unit tests for both, run with the repo's `uv run pytest`. No AWS account needed |

## The stored object

```json
{
  "event_id": "a3f19c0b7d5e2481",
  "event": "start",
  "event_time": "2026-09-03T06:18:57Z",
  "latitude": 47.51136133402844,
  "longitude": 19.00823675836947,
  "logged_at": "Thu, 03 Sep 2026 08:18:57 +0200",
  "received_at": "2026-09-03T06:19:02Z"
}
```

`event_time` is `logged_at` parsed into UTC. The Lambda does that conversion,
so Spark never has to deal with the RFC 2822 weekday name. `logged_at` is kept
as it arrived for traceability.

`event_id` is a SHA-256 hash of `event`, `event_time`, `latitude` and
`longitude`, truncated to 16 hex characters. It does not include `received_at`,
so posting the same event twice produces the same id and the same S3 key. A
retry from the Shortcut overwrites one object rather than creating a duplicate,
which is why the pipeline needs no dedup step.

## What the Lambda accepts

The body can be any of these, because the Shortcut has sent all of them:

```json
{"event": "start", "latitude": "47.5", "longitude": "19.0", "logged_at": "..."}
{"payload": {"event": "start", ...}}
{"payload": "{\"event\":\"start\", ...}"}
```

Coordinates may be strings or numbers. Anything else gets a 400 and is not
stored.

## Deploying

The shared secret lives in SSM Parameter Store, not in this repo and not in the
stack parameters. Create it once:

```
$ aws ssm put-parameter --name /car-trip-tracker/api-key --type SecureString \
    --value "$(openssl rand -base64 48 | tr -d '/+=\n' | cut -c1-40)"
```

A standard-tier `SecureString` with the default `aws/ssm` key has no monthly
fee.

Then deploy. `samconfig.toml` holds the stack name, the region, the AWS CLI
profile and the IAM capabilities, so no prompts are needed:

```
$ cd aws
$ sam build
$ sam deploy
```

`sam deploy` prints the Function URL, the bucket name and the reader role ARN.
The Shortcut needs the Function URL and the secret; `databricks.yml` needs the
`EventsPath` output as its `events_path` variable.

## Giving Databricks read access

`template.yaml` creates `car-trip-events-databricks-reader`, whose trust policy
lets Unity Catalog assume it. Databricks requires this role to be
self-assuming: the trust policy has to name the role itself. IAM rejects a
trust policy that names a role which does not exist yet, so a fresh setup
takes two deploys:

```
$ cd aws && sam deploy --parameter-overrides SelfAssume=false   # creates the role
$ cd .. && scripts/setup_uc_external_location.sh private        # creates the storage credential
$ cd aws && sam deploy                                          # SelfAssume=true from samconfig.toml
$ cd .. && scripts/setup_uc_external_location.sh private        # creates the external location, validates
```

`samconfig.toml` sets `SelfAssume=true`, so every plain `sam deploy` keeps the
self-trust. If the script reports an external id different from the
`DatabricksAccountId` parameter, add it to `parameter_overrides` in
`samconfig.toml` and deploy again.

The external location has file events turned off. File events would need the
role to create SNS and SQS resources, and Auto Loader lists the prefix
instead, which is cheap for a few objects a day.

## Testing the endpoint

A request with a valid key and an invalid body gets a 400 and writes nothing,
so it checks the key without creating a fake event:

```
$ curl -X POST "$FUNCTION_URL" \
    -H "X-Api-Key: $API_KEY" \
    -H 'Content-Type: application/json' \
    -d '{"event": "test"}'
```

A request without the key gets a 401. A valid event gets a 200 with the
`event_id` and the S3 key it wrote. A test event like that stays in the tables
until its S3 object is deleted and the pipeline runs with a full refresh,
because Auto Loader does not remove rows for deleted files.

## Rotating the API key

```
$ aws ssm put-parameter --name /car-trip-tracker/api-key --type SecureString --overwrite \
    --value "$(openssl rand -base64 48 | tr -d '/+=\n' | cut -c1-40)"
$ aws lambda put-function-concurrency --function-name car-trip-ingest --reserved-concurrent-executions 0
$ aws lambda delete-function-concurrency --function-name car-trip-ingest
```

The Lambda reads the key once per instance. Setting the reserved concurrency
to 0 stops the running instances, so the next request reads the new key. Then
put the new key into the Shortcut.

## Cost

Lambda's free tier is 1 million requests a month and does not expire. A few
events a day is far inside it. S3 holds a few hundred kilobytes a year, so
storage and requests come to a few cents a year.
