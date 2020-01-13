
## web_decay

### seeds
Union of seeds from DMOZ from 2005, 2011, 2013, 2017. Around 7M links in total
```json
{
    "url": "string",
    "category": "str"
}
```

### host_meta
Hostname encountered on random walking on the wayback machine in certain year
```json
{
    "hostname": "string",
    "year": "int",
}
```

### hosts_added_links
Number of links newly added to wayback machine for host in certain year
```json
{
    "hostname": "string",
    "year": "int",
    "added_links": "int"
}
```

### url_sample
Sampled urls. For every host with >= 500 urls, sample 100 from them
```json
{
    "_id": "url",
    "url": "string",
    "hostname": "string",
    "year": ["int"]
}
```

### host_status
Status that each host has. One host could have different status \
Unique on each obj (all fields composition)
```json
{
    "hostname": "string",
    "year": ["int"],
    "status": "status (2/3/4/5xx / DNS / Other)",
    "detail": "string"
}
```

### url_status
Status for each url, urls with hosts in host_status
Unique on (url + year)
```json
{
    "url": "string",
    "hostname": "string",
    "year": ["int"],
    "status": "status (2/3/4/5xx / DNS / Other)",
    "detail": "string"
}
```

### url_year_added
For each hosts in hosts_added_links, all the urls newly crawled on wayback
```json
{
    "url": "string",
    "hostname": "string",
    "year": "int",
}
```

### url_population
only url in url_year_added with host has >=100 new urls in certain year
```json
{
    "url": "string",
    "hostname": "string",
    "year": "int",
}
```

### url_content
HTML and content of url, both from wayback machine and realweb \
src defines the where the html was crawled \
If src: wayback, ts is added.
```json
{
    "url": "string",
    "src": "string",
    "(ts)":"int",
    "html": "Bytes (compressed by brotli)",
    "content": "string"
}
```

### host_sample
Sample host which contains 2xx/3xx status code
```json
{
    "hostname": "string",
    "year": "int"
}
```