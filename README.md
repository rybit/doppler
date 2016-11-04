# ingest METRICS data

Expected format:
```
{
    "name": "metric-name",
    "type": "counter",
    "value": 12,
    "timestamp": "iso8601 timestamp",
    "dims": { }
}
```

It will then push this to redshift. The data gets put in two tables:

 dimensions:
    - id
    - key
    - value
    - metric_id

 metrics:
    - id
    - name
    - timestamp
    - value
    - type

non-string dimensional values are converted to strings. This is sad. Let me know if you have a better way.

# queries (lightly tested)

all names:
```
select uniq(name) from metrics
```
dims for a MTS:
```
select uniq(key) from dimensions where metric_id in (select id from metrics where name = $NAME)
```
points in a time period:
```
select * from metrics
```

filtered query:
```
select * from metrics m
    where m.timestamp between 'xxx' and 'xxx'
    and m.id in (
        select id from dimensions d
        where d.key = $KEY and d.value = $VALUE
    )
```

# configuration

Configuration is done via JSON config file usually. You can use ENV overrides prefixed with 'DOPPLER_'. Checkout the example.config.json for what options are available.
