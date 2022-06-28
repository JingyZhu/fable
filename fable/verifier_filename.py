import os
from .utils import url_utils
from collections import defaultdict
import json, regex
import re
from enum import IntEnum

from urllib.parse import urlsplit, urlunsplit, parse_qsl, unquote
VERTICAL_BAR_SET = '\u007C\u00A6\u2016\uFF5C\u2225\u01C0\u01C1\u2223\u2502\u0964\u0965'
OTHER_DELIMITER_SET = '::'

def _throw_unuseful_query(url):
    return url
    url = url_utils.url_norm(url)
    us = urlsplit(url)
    filename, _ = os.path.splitext(us.path.split('/')[-1])
    if us.query and len(filename) > 5: # ? 5 enough?
        us = us._replace(query='')
    return urlunsplit(us)


class Match(IntEnum):
    PRED = 3
    PREFIX = 2
    MIX = 1
    UNPRED = 0

class MixType(IntEnum):
    ID = 2
    STR = 1
    NA = 0

class URLAlias:
    def __init__(self, url, alias, reason, title=""):
        self.url = url
        self.alias = alias
        self.norm_alias = url_utils.url_norm(alias, trim_slash=True)
        self.method = reason.get('method', '')
        self.matched = reason.get('type', '')
        self.title = title
        self.others_pairs = []
    
    def to_tuple(self):
        return (self.url, self.alias, self.matched)
    
    def __str__(self):
        return f"{self.to_tuple()}"

    def diffs(self):
        url_tokens = url_utils.tokenize_url(self.url, include_all=True, process='file')
        alias_tokens = url_utils.tokenize_url(self.norm_alias, include_all=True, process='file')
        example_diffs = url_utils.url_token_diffs(url_tokens, alias_tokens)
        return tuple(sorted(e[:2] for e in example_diffs))
    
    def _looks_noid(self, s):
        """s should be int"""
        if not s.isdigit():
            return True
        if int(s) < 100:
            return True
        s = str(s)
        if len(s) >= 4:
            year = int(s[:4])
            if year >= 1900 and year <= 2022:
                return True
        return False

    def get_digit(self, alias=False):
        url = self.alias if alias else self.url
        us = urlsplit(url)
        path, query = us.path, us.query
        digits = []
        tokens = []
        if query:
            qsl = parse_qsl(query)
            for _, v in qsl: tokens.append(v)
        if path != '/' and path[-1] == '/': path = path[:-1]
        path = path.split('/')[1:]
        for i in range(min(2, len(path))):
            tokens.append(path[-(i+1)])
        r_digit = re.compile("((\d+[^a-zA-Z\d\s:]*)+\d+)")
        for t in tokens:
            se = r_digit.findall(t)
            if len(se) <= 0:
                continue
            for s in se:
                s = s[0]
                # * Filter out date
                if self._looks_noid(s):
                    continue
                digits.append(s)
        return digits
    
    def same_digit(self, unique_counter=None):
        """unique_counter: uniqueness check dict for different digits"""
        urld = self.get_digit(alias=False)
        aliasd = self.get_digit(alias=True)
        if unique_counter:
            urld = [d for d in urld if unique_counter.get(d, 1) <= 1]
            aliasd = [d for d in aliasd if unique_counter.get(d, 1) <= 1]
        return len(set(urld).intersection(aliasd)) > 0
    
    def get_token(self, alias=False):
        url = self.alias if alias else self.url
        last_tokens = url_utils.tokenize_url(url, process='file')[-1].split()
        us = urlsplit(url)
        if us.query:
            us = us._replace(query='')
        else:
            us = us._replace(path=os.path.dirname(us.path[:-1]))
        url = urlunsplit(us)
        second_last_tokens = url_utils.tokenize_url(url, process='file')[-1].split()
        return [second_last_tokens, last_tokens]
    
    def overlap_token(self):
        """Return: #same token, #diff tokens"""
        url_tokens = self.get_token(alias=False)
        alias_tokens = self.get_token(alias=True)
        max_overlap = None
        for t1 in url_tokens:
            for t2 in alias_tokens:
                t1, t2 = set(t1), set(t2)
                same, diff = len(t1.intersection(t2)), len(t1-t2)+len(t2-t1)
                max_overlap = max_overlap if max_overlap else (same, diff)
                if same > max_overlap[0]:
                    max_overlap = (same, diff)
        return max_overlap 

    def transformation_rules(self, others_pairs=None):
        """Return: What rule?"""
        if not others_pairs: others_pairs = self.others_pairs
        others_pairs = [o for o in others_pairs if o.to_tuple()[:2] != self.to_tuple()[:2]]
        
        url_tokens = url_utils.tokenize_url(self.url, include_all=True, process=False)
        alias_tokens = url_utils.tokenize_url(self.norm_alias, include_all=True, process=False)

        def _intersect_prefix(s, i):
            if s.isdigit():
                return False
            for pa in others_pairs:
                pa_tokens = url_utils.tokenize_url(pa.alias, include_all=True, process=False)
                if len(pa_tokens) > i+1 and pa_tokens[i+1] == s:
                    return True
            return False

        def _predictability(s1, s2):
            """Return matched"""
            # * Check total predictable
            # * Exemption: 01 vs. 1
            if s1.isdigit() and s2.isdigit() and int(s1) == int(s2):
                return Match.PRED
            s1 = os.path.splitext(s1)[0]
            s2 = os.path.splitext(s2)[0]
            t1s = set(url_utils.tokenize(s1, stop_words=None))
            t2s = set(url_utils.tokenize(s2, stop_words=None))
            if len(t2s) == 0 or len(t1s) == 0:
                return Match.UNPRED
            # * One of the token is length 1 and the other is not
            if len(t2s)+len(t1s) > 2 and len(t1s)*len(t2s) in [len(t1s),len(t2s)]:
                return Match.UNPRED
            itst = t1s.intersection(t2s)
            ratio1 = len(itst) / len(t1s)
            if len(itst) / len(t2s) == 1 and ratio1 > 0.5:
                return Match.PRED
            return Match.UNPRED
            # * Separate tokens into digit and non-digit
            # * If there are some digit predictable, partially pred with ID
            # * Else, str partial pred needs to have majority
            for token in itst:
                if token.isdigit() and not self._looks_noid(token):
                    return Match.MIX, MixType.ID
            t1s_alpha = set([t for t in t1s if t.isalpha()])
            t2s_alpha = set([t for t in t2s if t.isalpha()])
            if len(t2s_alpha) > 0:
                itst = t1s_alpha.intersection(t2s_alpha)
                ratio2 = len(itst) / len(t2s_alpha)
                if ratio2 > 0.6: 
                    return Match.MIX, MixType.STR
        
        def _partial_predictability(s1, s2):
            """Sequence of partial predictability of filename"""
            seq = []
            s1 = os.path.splitext(s1)[0]
            s2 = os.path.splitext(s2)[0]
            t1 = url_utils.tokenize(s1, stop_words=None)
            t2 = url_utils.tokenize(s2, stop_words=None)
            t1s = set(t1)
            for t in t2:
                if t not in t1s:
                    match = (Match.UNPRED, MixType.NA)
                elif not self._looks_noid(t):
                    match = (Match.PRED, MixType.ID)
                else:
                    match = (Match.PRED, MixType.STR)
                if len(seq) == 0 or tuple(seq[-1][:2]) != match:
                    seq.append(list(match) + [int(match[0] > 0)])
                else:
                    seq[-1][2] += int(match[0] > 0)
                    seq[-1][1] = max(seq[-1][1], match[1])
            return [tuple(s) for s in seq]

        titles = regex.split(f'_| [{VERTICAL_BAR_SET}] |[{VERTICAL_BAR_SET}]| \p{{Pd}} |\p{{Pd}}| (?:{OTHER_DELIMITER_SET}) |(?:{OTHER_DELIMITER_SET})', self.title)
        if len(titles) > 1:
            titles = [' '.join(titles[:-1]), ' '.join(titles[1:])]
        
        dir_rules = []
        file_rules = []
        # * Directory
        for i, at in enumerate(alias_tokens[1:-1]):
            # * Check prefix from other_pairs
            best_match = (Match.UNPRED, "")
            if _intersect_prefix(at, i):
                best_match = (Match.PREFIX, at)
            src_dict = {Match.PREFIX: at, Match.PRED: 'url/title', Match.MIX: 'url/title', Match.UNPRED: 'N/A'}
            # ! URL
            for ut in url_tokens[1:]:
                match = _predictability(ut, at)
                best_match = max(best_match, (match, src_dict[match]))
            # ! Title
            # src_dict = {Match.PREFIX: at, Match.PRED: 'title', Match.MIX: 'title', Match.UNPRED: 'N/A'}
            if best_match[0] < Match.PREFIX:
                for title in titles:
                    match = _predictability(title, at)
                    best_match = max(best_match, (match, src_dict[match]))
            dir_rules.append(best_match)
        # * Filename
        def _partial_rank(x):
            """Max matched type, matched"""
            return max([(xx[0], xx[1]) for xx in x]), sum([xx[-1] for xx in x])
        
        file = alias_tokens[-1]
        best_match = [(0, 0, 0)] # * Pred/Unpred, PredType, Source, Num_tokens
        src_dict = {Match.PRED: 'url/title', Match.MIX: 'url/title', Match.UNPRED: 'N/A'}
        # ! URL
        for ut in url_tokens[1:]:
            match = _partial_predictability(ut, file)
            best_match = max(best_match, match, key=_partial_rank)
        # ! Title
        # src_dict = {Match.PREFIX: at, Match.PRED: 'title', Match.MIX: 'title', Match.UNPRED: 'N/A'}
        for title in titles:
            match = _partial_predictability(title, file)
            best_match = max(best_match, match, key=_partial_rank)
        file_rules = [b[:2] for b in best_match]
        return (alias_tokens[0], dir_rules, file_rules)

class Verifier:
    def __init__(self):
        self.url_candidates = defaultdict(lambda: defaultdict(set)) # * {url: {cand: {matched}}}
        self.url_title = {}
        self.s_clusters = None
        self._clusters = None
        self.valid_hints = {
            'archive_canonical':10 , 
            'title':1, 'content':1, 
            'wayback_alias':2, 
            'token':0.5, 
            "anchor": 1, 
            'redirection': 2
        }

    def clear(self):
        self.url_candidates = defaultdict(lambda: defaultdict(set)) # * {url: {cand: {matched}}}
        self.url_title = {}
        self.s_clusters = None
        self._clusters = None

    def _method_str(self, reason):
        return f"{reason['method']}:{reason.get('type', '')}"

    def _url_norm(self, url):
        return url_utils.url_norm(url, ignore_scheme=True, trim_www=True, trim_slash=True)

    def add_aliasexample(self, aliasexmaple, clear=False):
        """
        Transfer raw data of 
            {"alias": [], "examples": []} to url_candidates
        clear: Whether to clear previous url_candidate
        """
        if clear:
            self.clear()
        for obj in aliasexmaple['alias']:
            url, cand = obj[0], obj[2]
            if cand is None or len(cand) == 0:
                continue
            # ! Currently go with the last URL. Maybe able to optimize
            if isinstance(cand, list):
                cand = cand[-1]
            url = self._url_norm(url)
            cand = self._url_norm(cand)

            title, reason = obj[1][0], obj[3]
            # * Patch for token match filters
            if reason.get('type') == "token":
                if reason.get('value', 0) < 0.8:
                    continue
                matched_token = reason['matched_token']
                if len(matched_token.split(' ')) <= 1:
                    continue
            method = self._method_str(reason)
            self.url_candidates[url][cand].add(method)
            self.url_title[url] = title
        
        for obj in aliasexmaple['examples']:
            url, cand = obj[0], obj[2]
            if cand is None or len(cand) == 0:
                continue
            # ! Currently go with the last URL. Maybe able to optimize
            if isinstance(cand, list):
                cand = cand[-1]
            url = self._url_norm(url)
            cand = self._url_norm(cand)

            title, reason = obj[1][0], obj[3]
            # * Patch for low value matched tokens
            if reason.get('type') == "token" and reason.get('value', 0) < 0.8:
                continue
            method = self._method_str(reason)
            self.url_candidates[url][cand].add(method)
            self.url_title[url] = title
    
    def add_gtobj(self, gt_obj, clear=False):
        """
        Transfer raw data of gt_obj to url_candidates
        clear: Whether to clear previous url_candidate
        """
        if clear:
            self.clear()
        url = gt_obj['url']
        self.url_title[url] = gt_obj.get('title', '')
        # * Search
        search_aliases = gt_obj.get('search', None)
        if search_aliases is not None:
            if not isinstance(search_aliases[0], list): search_aliases = [search_aliases]
            for search_alias in search_aliases:
                if search_alias[0]:
                    search_alias[1]['method'] = 'search'
                    self.url_candidates[url][search_alias[0]].add(self._method_str(search_alias[1]))
        # * Backlink
        backlink_alias = gt_obj.get('backlink', None)
        if backlink_alias is not None and backlink_alias[0] is not None:
            backlink_alias[1]['method'] = 'backlink'
            self.url_candidates[url][backlink_alias[0]].add(self._method_str(backlink_alias[1]))
        # * Inference
        infer_alias = gt_obj.get('inference', None)
        if infer_alias is not None and infer_alias[0] is not None:
            infer_alias[1]['method'] = 'inference'
            self.url_candidates[url][infer_alias[0]].add(self._method_str(infer_alias[1]))
        
        # * Prepare for examples
        examples = gt_obj.get('examples', [])
        for example in examples:
            ex_url = example[0]
            ex_cand = example[2]
            ex_title = example[1][0]
            self.url_title[ex_url] = ex_title
            self.url_candidates[ex_url][ex_cand].add(self._method_str(example[3]))  

    def _filter_suspicious_cands(self):
        new_url_candidates = defaultdict(lambda: defaultdict(set))
        # * Filter cands that looks suspicious
        for url, cands in self.url_candidates.items():
            for cand, v in cands.items():
                if url_utils.suspicious_alias(url, cand):
                    continue
                new_url_candidates[url][cand] = v
        url_candidates = new_url_candidates
        new_url_candidates = defaultdict(lambda: defaultdict(set))
        cand_urls = defaultdict(set)
        # * Filter cands that match to multiple URLs
        for url, cands in url_candidates.items():
            for cand, v in cands.items():
                if len(v) > 1 or 'search:fuzzy_search' not in v:
                    url = _throw_unuseful_query(url)
                    cand_urls[cand].add(url)
        for url, cands in url_candidates.items():
            for cand, v in cands.items():
                if len(cand_urls[cand]) > 1:
                    continue
                new_url_candidates[url][cand] = v
        return new_url_candidates

    def _gen_cluster(self):
        """
        Generate clusters from self.url_candidates
        1. Filter suspicious candidates
        2. Form clusters
        Return cluster: [{pattern, [candidates]}]
        """
        url_candidates = self._filter_suspicious_cands()
        all_pairs = []
        for url, candidates in url_candidates.items(): 
            for cand in candidates:
                all_pairs.append(URLAlias(url, cand, {}))

        cluster = defaultdict(list)
        for turl, tcands in url_candidates.items():
            title = self.url_title.get(turl, '')
            for tcand, reason in tcands.items():
                if len(reason) > 1 and 'search:fuzzy_search' in reason:
                    reason.remove('search:fuzzy_search')
                ua = URLAlias(turl, tcand, {}, title=title)
                rule = ua.transformation_rules(others_pairs=all_pairs)
                rule = (rule[0], tuple([r for r in rule[1]]), tuple([r for r in rule[2]]))
                ua_tuple = list(ua.to_tuple())
                ua_tuple[-1] = '+'.join(reason)
                cluster[rule].append(ua_tuple)
        cluster = [{'values': v, "rule": [k[0],list(k[1]),list(k[2])]} for k, v in cluster.items()]
        return cluster
     
    def _rank_cluster(self, cluster):
        def __predictability(rule):
            pred = len([r for r in rule if r[0] == 0])
            return -pred
        cluster_score = []
        for c in cluster:
            seen_orig_url = set()
            seen_hints = set()
            pred = __predictability(c['rule'][1]) + __predictability(c['rule'][2])
            if pred <= -len(c['rule'][1])-len(c['rule'][2]):
                    continue 
            for url, cand, method in c['values']:
                seen_orig_url.add(url)
                method = method.split('+')
                method = [m.split(":")[0] for m in method] + [m.split(":")[1] for m in method]
                seen_hints.update(set(self.valid_hints.keys()).intersection(method))
            hint_score = sum([self.valid_hints[s] for s in seen_hints])
            if hint_score > 0:
                cluster_score.append((c, (hint_score, pred, len(seen_orig_url))))
        return [c[0] for c in sorted(cluster_score, key=lambda x: x[1], reverse=True)]

    def _satisfied_cluster(self, cluster, top_cluster):
        def __more_trustable(r1, r2):
            """Whether r1 is more trustable than r2"""
            if r1[0] != r2[0]:
                return False
            file_r1, file_r2 = r1[2], r2[2]
            if max([tuple(f[:2]) for f in file_r1]) < max([tuple(f[:2]) for f in file_r2]):
                return False
            dir_r1, dir_r2 = r1[1], r2[1]
            if len(dir_r1) != len(dir_r2):
                return False
            its = len(dir_r1)
            dir_good = True
            for i in range(-1, -its-1, -1):
                if dir_r1[i][:2] < dir_r2[i][:2]:
                    dir_good = False
                    break
            return dir_good
        final_clusters = [top_cluster]
        for c in cluster[1:]:
            if __more_trustable(c['rule'], top_cluster['rule']):
                final_clusters.append(c)
        return final_clusters

    def _valid_cluster(self, cluster, target_url):
        """Check whether the target (top) cluster looks valid"""
        url_cand = defaultdict(set)
        cand_url = defaultdict(set)
        def _norm(url):
            url = _throw_unuseful_query(url)
            return url_utils.url_norm(url.lower(), ignore_scheme=True, trim_slash=True)

        valid = False
        for url, cand, method in cluster['values']:
            # * Archive canonical considered as valid automatically
            if url == target_url and 'archive_canonical' in method:
                valid = True
            url = _norm(url)
            cand = url_utils.url_norm(cand.lower(), ignore_scheme=True, trim_www=True, trim_slash=True)
            url_cand[url].add(cand)
            cand_url[cand].add(url)

        # * Target URL matched to more than 3 candidates in the cluster
        if not valid and len(url_cand[_norm(target_url)]) >= 4:
            return False

        # * Cluster has no matched property at all
        # total_matched = 0
        # for _, _, method in cluster['values']:
        #     total_matched += self.valid_hints.get(method.split(':')[1], 0)
        # # print(total_matched)
        # if total_matched <= 1:
        #     return False
        return True

    def verify_url(self, url):
        """
        verify candidates for url, return [verified obj]
        1. Generate cluster and rank cluster
        2. Get valid clusters
        Return: [(cand, method_str)]
        """
        url = self._url_norm(url)
        if self.s_clusters is None:
            cluster = self._gen_cluster()
            cluster = self._rank_cluster(cluster)
            self._clusters = cluster
            if len(cluster) > 0:
                top_cluster = cluster[0]
                self.s_clusters = self._satisfied_cluster(cluster, top_cluster)
            else:
                self.s_clusters = []
        s_clusters = [s for s in self.s_clusters if self._valid_cluster(s, url)]
        if len(s_clusters) == 0:
            s_clusters = [{'values': []}]
        
        valid_cands = []
        for c in s_clusters:
            cand_seen = defaultdict(int)
            for ourl, ocand, method in c['values']:
                cand_seen[ocand] += 1
            for ourl, ocand, method_str in c['values']:
                if ourl != url:
                    continue
                method_str = method_str.split('+')
                method = [m.split(':')[0] for m in method_str]
                matched = [m.split(':')[1] for m in method_str]
                # * 1. cand is found only by fuzzy search 2.1 cand appears for other URLs 2.2 rule for filename is not very predictive
                if matched == ['fuzzy_search']:
                    if cand_seen[ocand] > 1:
                        continue
                    if tuple(c['rule'][1][-1]) < (1, 0, ""):
                        continue
                valid_cands.append((ocand, method_str))
        
        cred = lambda x: sum([self.valid_hints.get(xx.split(':')[1], 0) for xx in x[1]])
        valid_cands.sort(reverse=True, key=cred)        
        return valid_cands