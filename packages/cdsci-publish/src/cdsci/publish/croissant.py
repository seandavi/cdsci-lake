"""Croissant 1.1 JSON-LD (MLCommons) from a release bundle's manifest.json (ADR-0027 step 6).

One `cr:FileObject` per table (its Parquet: contentUrl, sha256, contentSize) and one
`cr:RecordSet` per table with one `cr:Field` per column: the column doc as `description`,
the DuckDB type mapped to a Croissant data type, LIST columns as `isArray`, STRUCT columns
as `subField`. Everything the manifest does not carry -- the dataset's name, description,
url, license, creator, citeAs, keywords, and per-column semantics -- is the product's
`dataset` and `field_semantics` input; domain meaning stays product-local.

Not emitted: the manifest's `provenance` rows. Their shape is product-defined (a source
row with `url`, `source_version`, `retrieved_at`, ...) and has no clean PROV-O mapping yet.
"""

import re

CONFORMS_TO = "http://mlcommons.org/croissant/1.1"
PARQUET = "application/x-parquet"  # what mlcroissant and Hugging Face use
# Appendix 1 of the 1.1 spec, except schema.org over https: the appendix says http, but
# mlcroissant (the reference implementation) and Hugging Face both require/emit https, and
# mlcroissant rejects the http form ("doesn't extend https://schema.org/Dataset").
CONTEXT = {
    "@language": "en",
    "@vocab": "https://schema.org/",
    "sc": "https://schema.org/",
    "cr": "http://mlcommons.org/croissant/",
    "rai": "http://mlcommons.org/croissant/RAI/",
    "dct": "http://purl.org/dc/terms/",
    "annotation": "cr:annotation",
    "arrayShape": "cr:arrayShape",
    "citeAs": "cr:citeAs",
    "column": "cr:column",
    "conformsTo": "dct:conformsTo",
    "containedIn": "cr:containedIn",
    "data": {"@id": "cr:data", "@type": "@json"},
    "dataType": {"@id": "cr:dataType", "@type": "@vocab"},
    "equivalentProperty": "cr:equivalentProperty",
    "examples": {"@id": "cr:examples", "@type": "@json"},
    "excludes": "cr:excludes",
    "extract": "cr:extract",
    "field": "cr:field",
    "fileProperty": "cr:fileProperty",
    "fileObject": "cr:fileObject",
    "fileSet": "cr:fileSet",
    "format": "cr:format",
    "includes": "cr:includes",
    "isArray": "cr:isArray",
    "isLiveDataset": "cr:isLiveDataset",
    "jsonPath": "cr:jsonPath",
    "key": "cr:key",
    "md5": "cr:md5",
    "parentField": "cr:parentField",
    "recordSet": "cr:recordSet",
    "references": "cr:references",
    "regex": "cr:regex",
    "readLines": "cr:readLines",
    "sdVersion": "cr:sdVersion",
    "separator": "cr:separator",
    "source": "cr:source",
    "subField": "cr:subField",
    "transform": "cr:transform",
    "unArchive": "cr:unArchive",
    "value": "cr:value",
}

# DuckDB scalar type -> Croissant atomic data type. Parametrised types (DECIMAL(18,3),
# ENUM('a','b'), TIMESTAMP WITH TIME ZONE) match on their head. Anything else (BLOB, MAP,
# UNION, BIT, INTERVAL, ...) has no Croissant data type and becomes sc:Text with the raw
# type recorded in the description.
SCALAR = {
    "sc:Integer": ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT",
                   "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT"),
    "sc:Float": ("FLOAT", "DOUBLE", "DECIMAL"),
    "sc:Boolean": ("BOOLEAN",),
    "sc:Text": ("VARCHAR", "UUID", "JSON", "ENUM"),
    "sc:Date": ("DATE",),
    "sc:Time": ("TIME",),
    "sc:DateTime": ("TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS"),
}
_HEAD = {head: dt for dt, heads in SCALAR.items() for head in heads}
UNMAPPED = "no Croissant data type"


def _split_top(s):
    """`s` split on commas outside parentheses and quotes."""
    parts, depth, quote, start = [], 0, None, 0
    for i, ch in enumerate(s):
        if quote:
            quote = None if ch == quote else quote
        elif ch in ("'", '"'):
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(s[start:i].strip())
            start = i + 1
    return parts + [s[start:].strip()]


def _struct_members(inner):
    """`name TYPE, "quoted name" TYPE, ...` -> [(name, TYPE), ...]."""
    out = []
    for part in _split_top(inner):
        if part.startswith('"'):
            end = part.index('"', 1)
            out.append((part[1:end], part[end + 1:].strip()))
        else:
            name, _, typ = part.partition(" ")
            out.append((name, typ.strip()))
    return out


def _type(duckdb_type):
    """`duckdb_type` -> (Croissant data type or None, array depth, struct members or None).

    None data type with no struct members means unmapped (the caller falls back to sc:Text).
    """
    t = duckdb_type.strip()
    depth = 0
    while m := re.search(r"\[\d*\]$", t):  # LIST `T[]` and fixed-size ARRAY `T[n]`
        t, depth = t[:m.start()], depth + 1
    if t.upper().startswith("STRUCT(") and t.endswith(")"):
        return None, depth, _struct_members(t[7:-1])
    head = re.split(r"[ (]", t.upper(), maxsplit=1)[0]
    return _HEAD.get(head), depth, None


def field(record_set, column, source, semantics=None):
    """One `cr:Field` for a manifest column `{name, type, doc}` of `record_set`.

    `source` is the FileObject `@id` (None for a STRUCT member: the parent carries the
    source). `semantics` is the product's extra properties for this column, merged in:
    `dataType` values are appended to the atomic type (a field may carry several, at least
    one atomic, per the spec), anything else (`equivalentProperty`, an external-vocabulary
    property) is set as given.
    """
    name, raw, doc = column["name"], column["type"], column.get("doc")
    data_type, depth, members = _type(raw)
    # An @id is an IRI: no whitespace (mlcroissant refuses even escaped). `name` and
    # `extract.column` keep the real column name.
    fid = f"{record_set}/{re.sub(r'\s+', '_', name)}"
    f = {"@type": "cr:Field", "@id": fid, "name": name}
    if members is not None:
        f["subField"] = [field(fid, {"name": m, "type": mt, "doc": None}, None)
                         for m, mt in members]
    else:
        if data_type is None:
            data_type = "sc:Text"
            doc = f"{doc + ' ' if doc else ''}(DuckDB type {raw}; {UNMAPPED})"
        f["dataType"] = data_type
    if doc:
        f["description"] = doc
    if depth:
        f["isArray"] = True
        if depth > 1:
            f["arrayShape"] = ",".join(["-1"] * depth)
    if source is not None:
        f["source"] = {"fileObject": {"@id": source}, "extract": {"column": name}}
    for k, v in (semantics or {}).items():
        if k == "dataType":
            atomic = [f["dataType"]] if "dataType" in f else []
            f["dataType"] = atomic + (v if isinstance(v, list) else [v])
        else:
            f[k] = v
    return f


def croissant(manifest: dict, *, dataset: dict, field_semantics: dict | None = None) -> dict:
    """Croissant 1.1 JSON-LD for `manifest` (a `cdsci.publish` manifest.json).

    `dataset` is merged onto the sc:Dataset: at least `name`, `description`, `url` and
    `license` (the spec's required properties the manifest cannot supply), plus `creator`,
    `citeAs`, `keywords`, ... as schema.org/Croissant allow; a `"@context"` entry is merged
    into the standard context (external-vocabulary prefixes). `version` and `datePublished`
    come from the manifest's `release` / `published_at` unless `dataset` overrides them.

    `field_semantics`: `{"schema.table": {"column": {property: value, ...}}}`, see `field`.
    """
    semantics = field_semantics or {}
    base = manifest["base_url"].rstrip("/")
    distribution, record_sets = [], []
    for t in manifest["tables"]:
        name, path = f"{t['schema']}.{t['table']}", t["path"]
        file_object = {"@type": "cr:FileObject", "@id": path, "name": path,
                       "contentUrl": f"{base}/{path}", "encodingFormat": PARQUET,
                       "contentSize": f"{t['bytes']} B", "sha256": t["sha256"]}
        distribution.append(file_object)
        rs = {"@type": "cr:RecordSet", "@id": name, "name": name}
        if t.get("comment"):
            rs["description"] = t["comment"]
        rs["field"] = [field(name, c, path, semantics.get(name, {}).get(c["name"]))
                       for c in t["columns"]]
        ids = [f["@id"] for f in rs["field"]]
        if len(set(ids)) != len(ids):
            raise ValueError(f"{name}: column names collide as Croissant field ids: "
                             f"{sorted({i for i in ids if ids.count(i) > 1})}")
        record_sets.append(rs)
    doc = {
        "@context": {**CONTEXT, **dataset.get("@context", {})},
        "@type": "sc:Dataset",
        "conformsTo": CONFORMS_TO,
        "version": manifest["release"],
        "datePublished": manifest.get("published_at", manifest["release"]),
        **{k: v for k, v in dataset.items() if k != "@context"},
        "distribution": distribution,
        "recordSet": record_sets,
    }
    return doc


def relocate(doc: dict, base_url: str) -> dict:
    """`doc` with every table FileObject's contentUrl under `base_url` (each upload folder
    serves its own URL; a Hugging Face copy reads download-relative paths, `base_url="."`)."""
    base = base_url.rstrip("/")
    distribution = [{**f, "contentUrl": f"{base}/{f['@id']}"} if f.get("@type") == "cr:FileObject"
                    else f for f in doc["distribution"]]
    return {**doc, "distribution": distribution}


def unmapped(doc: dict) -> list[str]:
    """Field ids whose DuckDB type had no Croissant data type (emitted as sc:Text)."""
    def walk(fields):
        for f in fields:
            if UNMAPPED in f.get("description", ""):
                yield f["@id"]
            yield from walk(f.get("subField", []))
    return [fid for rs in doc["recordSet"] for fid in walk(rs["field"])]
