"""Parse Kafka's generated configuration docs (site-docs/generated/*.html) into JSON."""
import html, json, re, sys, pathlib

def parse(path):
    txt = pathlib.Path(path).read_text(encoding="utf-8")
    out = {}
    for li in re.split(r"<li>\s*<h4>", txt)[1:]:
        m = re.search(r'href="#[^"]*">([^<]+)</a></h4>', li)
        if not m:
            continue
        key = html.unescape(m.group(1)).strip()
        rows = dict((html.unescape(re.sub("<[^>]+>", "", a)).strip().rstrip(":"),
                     html.unescape(re.sub("<[^>]+>", "", b)).strip())
                    for a, b in re.findall(r"<tr><th>(.*?)</th><td>(.*?)</td></tr>", li, re.S))
        desc = html.unescape(re.sub("<[^>]+>", "", (re.search(r"<p>(.*?)</p>", li, re.S) or [None, ""])[1])).strip()
        out[key] = {"type": rows.get("Type"), "default": rows.get("Default"),
                    "valid_values": rows.get("Valid Values"), "importance": rows.get("Importance"),
                    "update_mode": rows.get("Update Mode"), "deprecated": "deprecated" in desc.lower()[:300],
                    "description": desc[:400]}
    return out

if __name__ == "__main__":
    d = pathlib.Path(sys.argv[1])
    res = {}
    for name in ["topic_config", "kafka_config", "producer_config", "consumer_config", "admin_client_config",
                 "group_config", "streams_config", "connect_config", "mirror_connector_config",
                 "mirror_source_config", "mirror_checkpoint_config", "mirror_heartbeat_config"]:
        p = d / f"{name}.html"
        if p.exists():
            res[name] = parse(p)
            print(name, len(res[name]), file=sys.stderr)
    json.dump(res, open(sys.argv[2], "w"), indent=1)
