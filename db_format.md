
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
    "detail": "string",
    "(similarity)": "TF-IDF similarity of page on liveweb vs. wayback machine"
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
If src: wayback, ts, usage are added. \
usage: represent --> represent the content \
     archive --> Just archive for crawl
     updating --> Used for deterct archiving
```json
{
    "url": "string",
    "src": "string",
    "(ts)":"int",
    "html": "Bytes (compressed by brotli)",
    "content": "string",
    "(usage)": "represent / archive | updating"
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

### url_status_implicit_broken
The joined results between url_status and host_sample \
In other word, all urls in url_status which hostname is sampled in host_sample\
Format: Same as url_status

### url_update
Record whether a url has high link density / update frequently on wayback
```json
{
    "_id": "string (url)",
    "url": "string",
    "updating": "boolean, false means no / unknown (1 snapshot, similar, etc... )" ,
    "tss": [],
    "detail": "updating True: HLD, not similar, no contents. False: 1 snapshot, no content, similar, no html"
}
```

### url_broken
Sampled broken urls. 1000 per cell: {year * status} \
Subset of url_status_implicit_broken
Format: Sampe as url_status 

### search_meta
For each broken page, record its html, content and queries for search
Usage is like url_content
```json
{
    "url": "string",
    "html": "byte (brotli)",
    "ts": "int",
    "content": "string",
    "topN": "string",
    "titleMatch": "string",
    "usage": "(represent | archive)"
}
```

### search
##### Index: url_from (unique)
Search results for broken pages

```json
{
    "url": "string",
    "from": "string (url)",
    "html": "byte (brotli)",
    "content": "string",
    "rank": "top5 / top10"
}
```