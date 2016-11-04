# ingest METRICS data

Expected format: 
```
{
    "name": "metric-name",
    "type": "counter",
    "value": 12,
    "timestamp": "iso8601 timestamp",
    "dims": { }
```

It will then push this to redshift. The data gets put in two tables:

> Question: how to store non-string dims

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
 
# queries

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
