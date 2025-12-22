### Getting started

This chart deploys `mongo2s3` and, optionally, a local Postgres instance. Out of the box it’s meant to just work with minimal changes.

At a minimum, you should review and adjust the configuration section in `values.yaml`. This is where the application reads its runtime config from. The chart renders this config into a Kubernetes Secret and mounts it as a single file at `/etc/mongo2s3/config.yaml`, which the app reads on startup .

```yaml
config:
  db_url: "postgres://myuser:mypass@postgres:5432/mydb?sslmode=disable"
  mongo_url: "mongodb://mongodb.mongo2s3.orb.local:27017"
  local_base_dir: "/work/"
  s3_bucket: "test"
  s3_region: "us-east-1"
  s3_prefix: "/data"
```

The working directory is backed by a PVC and mounted at `/work`. This is required, as mongo2s3 writes intermediate files there before pushing them to S3 .

### Postgres configuration

By default, the chart deploys a local Postgres container:

```yaml
postgres:
  enabled: true
  auth:
    username: mongo2s3
    password: mongo2s3
    database: mongo2s3
```

These values are used directly unless you provide a secret. If you want to override credentials using an existing Secret, set `postgres.auth.secretName` and the chart will pull `username`, `password`, and `database` from it instead. You don’t need to change anything else.

This lets you:

* run locally without managing secrets
* switch to externally managed Postgres in staging or prod
* override credentials per environment cleanly

### Installing

Once values are set:

```sh
helm install mongo2s3 ./chart
```

Or for upgrades:

```sh
helm upgrade mongo2s3 ./chart
```

That’s it. If the pod starts and the config file is mounted, mongo2s3 will begin syncing based on the configured collections.
