
## web_decay
### url_sample
Sampled urls. For every host with >= urls, sample 100 from them
```json
{
    "_id": "url",
    "url": "string",
    "hostname": "string",
    "year": "int"
}
```

### host_status
Status that each host has. One host could have different status \\
Unique on each obj (all fields composition)
```json
{
    "hostname": "string",
    "year": "int",
    "status": "status (2/3/4/5xx / DNS / Other)",
    "detail": "string"
}
```