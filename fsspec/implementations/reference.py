import base64
import collections
import io
import itertools
import logging
import math
import os
from functools import lru_cache
from itertools import chain
from typing import TYPE_CHECKING, Literal

import fsspec.core
from fsspec.spec import AbstractBufferedFile

try:
    import ujson as json
except ImportError:
    if not TYPE_CHECKING:
        import json

from fsspec.asyn import AsyncFileSystem
from fsspec.callbacks import DEFAULT_CALLBACK
from fsspec.core import filesystem, open, split_protocol
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper
from fsspec.utils import (
    check_contained,
    isfilelike,
    merge_offset_ranges,
    other_paths,
)

logger = logging.getLogger("fsspec.reference")


class ReferenceNotReachable(RuntimeError):
    def __init__(self, reference, target, *args):
        super().__init__(*args)
        self.reference = reference
        self.target = target

    def __str__(self):
        return f'Reference "{self.reference}" failed to fetch target {self.target}'


class ReferenceResolutionError(ValueError):
    """A reference is malformed or cannot be resolved to readable bytes.

    Raised for unknown segment types, out-of-range coordinates, references
    whose logical length cannot be determined, nested references that do not
    resolve to a bounded length, and reference cycles.
    """


# A remote segment that is not allowed to contain a reference cycle may
# traverse at most this many ``reference://`` hops before giving up.
MAX_REFERENCE_HOPS = 64


def _first(d):
    return next(iter(d.values()))


def _inline_data(part):
    """The bytes an inline (in-memory) reference delivers to a reader.

    A ``base64:`` prefix marks base64-encoded content; other text is encoded
    as UTF-8. ``info`` and ``ls`` measure the result of this rather than the
    stored form, so that a size always matches what ``cat`` returns.
    """
    if isinstance(part, (str, bytes)):
        return _decode_inline(part)
    if hasattr(part, "to_bytes"):
        part = part.to_bytes()
    return _decode_inline(part)


@lru_cache(maxsize=1024)
def _decode_inline(part):
    """Decode a stored inline part once and stash the result.

    The same inline value is decoded for ``cat`` and again for the length
    check in ``info``/``ls``; caching avoids repeating the work for every
    listing. Inline references are small and few, so a bounded cache is
    enough.
    """
    if isinstance(part, str):
        part = part.encode()
    if part.startswith(b"base64:"):
        return base64.b64decode(part[7:])
    return part


# Normalized segments are tuples tagged by kind:
#   ("inline", data_bytes)
#   ("zeros", length)
#   ("range", url, offset, length)   length is None for a whole-file segment
def _is_multi_segment(part):
    """Whether a reference value uses the ordered-segment form.

    The legacy forms stay flat: inline bytes/strings, ``[url]`` and
    ``[url, offset, size]``, whose elements are all scalars. A reference
    containing any list/tuple/dict element is treated as ordered segments
    (remote parts or ``{"inline": ...}`` / ``{"zeros": n}`` dicts); mixed
    content then fails validation with a clear error.
    """
    if not isinstance(part, (list, tuple)) or not part:
        return False
    return any(isinstance(seg, (list, tuple, dict)) for seg in part)


def _normalize_segment(segment, path=None):
    """Validate one segment and return its tagged, normalized tuple."""
    if isinstance(segment, dict):
        if "inline" in segment:
            value = segment["inline"]
            if isinstance(value, (str, bytes)):
                return ("inline", _inline_data(value))
            if hasattr(value, "to_bytes") and not isinstance(
                value, (int, float, bool)
            ):
                # e.g. numpy scalars, matching legacy inline references
                return ("inline", _inline_data(value))
            raise ReferenceResolutionError(
                f"Inline segment for {path!r} must hold str or bytes, "
                f"got {type(value).__name__}"
            )
        if "zeros" in segment:
            length = segment["zeros"]
            if not isinstance(length, int) or isinstance(length, bool) or length < 0:
                raise ReferenceResolutionError(
                    f"Zero-fill segment for {path!r} needs a non-negative integer "
                    f"length, got {length!r}"
                )
            return ("zeros", length)
        raise ReferenceResolutionError(
            f"Unknown dict segment for {path!r}: expected 'inline' or 'zeros', "
            f"got {sorted(segment)}"
        )
    if isinstance(segment, (list, tuple)):
        if not segment or not (
            segment[0] is None or isinstance(segment[0], str)
        ):
            raise ReferenceResolutionError(
                f"Remote segment for {path!r} must start with a URL string, "
                f"got {segment!r}"
            )
        url = segment[0]
        if len(segment) == 1:
            return ("range", url, 0, None)
        if len(segment) == 3:
            offset, length = segment[1], segment[2]
            if (
                not isinstance(offset, int)
                or isinstance(offset, bool)
                or not isinstance(length, int)
                or isinstance(length, bool)
                or offset < 0
                or length < 0
            ):
                raise ReferenceResolutionError(
                    f"Remote range segment for {path!r} needs non-negative "
                    f"integer offset and length, got {segment!r}"
                )
            return ("range", url, offset, length)
        raise ReferenceResolutionError(
            f"Remote segment for {path!r} must be [url] or [url, offset, length], "
            f"got {segment!r}"
        )
    raise ReferenceResolutionError(
        f"Unsupported segment for {path!r}: {segment!r}"
    )


def _normalize_segments(part, path=None):
    """Validate a multi-segment reference and return normalized segments."""
    return [_normalize_segment(segment, path) for segment in part]


def _segment_length(segment):
    """Logical length of one normalized segment, or None if unknown."""
    kind = segment[0]
    if kind == "inline":
        return len(segment[1])
    if kind == "range":
        return segment[3]
    return segment[1]  # zeros


def _known_length(segments):
    """Total logical length when every segment is bounded, else None."""
    total = 0
    for segment in segments:
        length = _segment_length(segment)
        if length is None:
            return None
        total += length
    return total


def _extract_ranges(urls, starts, ends, merged_paths, merged_starts, merged_ends, data):
    """Pull each requested range back out of merged fetch results.

    Merging only widens requests, so every requested range is covered by at
    least one returned block; the returned bytes are sliced back to the
    original coordinates. Exceptions in ``data`` are passed through.
    """
    out = []
    for url, start, end in zip(urls, starts, ends):
        for mpath, mstart, mend, chunk in zip(
            merged_paths, merged_starts, merged_ends, data
        ):
            if mpath != url:
                continue
            if mend is None:
                covers = mstart <= start
            else:
                covers = mstart <= start and (end is not None and end <= mend)
            if not covers:
                continue
            if isinstance(chunk, Exception):
                out.append(chunk)
            elif end is None:
                out.append(chunk[start - mstart :])
            else:
                out.append(chunk[start - mstart : end - mstart])
            break
        else:
            raise ReferenceResolutionError(
                f"Merged fetch did not cover range [{start}, {end}) of {url!r}"
            )
    return out


def _slice_segments(segments, start, end):
    """Intersect a logical [start, end) window with normalized segments.

    Coordinates follow Python slicing against the known logical length, so
    out-of-range windows are clamped rather than reaching past the reference.
    Returns a list of ``(segment, local_start, local_end)`` where the local
    offsets are relative to that segment and ``local_end`` may be None for a
    whole-file segment that extends to the end of the file.
    """
    length = _known_length(segments)
    if length is None:
        if start is None and end is None:
            # Unbounded read: every segment contributes its full extent, whose
            # end is exactly what the remote returns for a whole-file segment.
            return [(segment, 0, _segment_length(segment)) for segment in segments]
        raise ReferenceResolutionError(
            "Cannot compute bounded logical coordinates: a segment has unknown "
            "length"
        )
    if start is None:
        start = 0
    if end is None:
        end = length
    if start < 0:
        start = max(length + start, 0)
    if end < 0:
        end = max(length + end, 0)
    start = min(start, length)
    end = min(end, length)
    if end <= start:
        return []

    plan = []
    cursor = 0
    for segment in segments:
        seg_length = _segment_length(segment)
        seg_end = cursor + seg_length
        if seg_end <= start:
            cursor = seg_end
            continue
        if cursor >= end:
            break
        local_start = max(start - cursor, 0)
        local_end = min(end - cursor, seg_length)
        plan.append((segment, local_start, local_end))
        cursor = seg_end
    return plan


def _prot_in_references(path, references):
    ref = references.get(path)
    if isinstance(ref, (list, tuple)) and isinstance(ref[0], str):
        return split_protocol(ref[0])[0] if ref[0] else ref[0]


def _protocol_groups(paths, references):
    if isinstance(paths, str):
        return {_prot_in_references(paths, references): [paths]}
    out = {}
    for path in paths:
        protocol = _prot_in_references(path, references)
        out.setdefault(protocol, []).append(path)
    return out


class RefsValuesView(collections.abc.ValuesView):
    def __iter__(self):
        for val in self._mapping.zmetadata.values():
            yield json.dumps(val).encode()
        yield from self._mapping._items.values()
        for field in self._mapping.listdir():
            chunk_sizes = self._mapping._get_chunk_sizes(field)
            if len(chunk_sizes) == 0:
                yield self._mapping[field + "/0"]
                continue
            yield from self._mapping._generate_all_records(field)


class RefsItemsView(collections.abc.ItemsView):
    def __iter__(self):
        return zip(self._mapping.keys(), self._mapping.values())


def ravel_multi_index(idx, sizes):
    val = 0
    mult = 1
    for i, s in zip(idx[::-1], sizes[::-1]):
        val += i * mult
        mult *= s
    return val


class LazyReferenceMapper(collections.abc.MutableMapping):
    """This interface can be used to read/write references from Parquet stores.
    It is not intended for other types of references.
    It can be used with Kerchunk's MultiZarrToZarr method to combine
    references into a parquet store.
    Examples of this use-case can be found here:
    https://fsspec.github.io/kerchunk/advanced.html?highlight=parquet#parquet-storage"""

    # import is class level to prevent numpy dep requirement for fsspec
    @property
    def np(self):
        import numpy as np

        return np

    @property
    def pd(self):
        import pandas as pd

        return pd

    def __init__(
        self,
        root,
        fs=None,
        out_root=None,
        cache_size=128,
        categorical_threshold=10,
        engine: Literal["fastparquet", "pyarrow"] = "fastparquet",
    ):
        """

        This instance will be writable, storing changes in memory until full partitions
        are accumulated or .flush() is called.

        To create an empty lazy store, use .create()

        Parameters
        ----------
        root : str
            Root of parquet store
        fs : fsspec.AbstractFileSystem
            fsspec filesystem object, default is local filesystem.
        cache_size : int, default=128
            Maximum size of LRU cache, where cache_size*record_size denotes
            the total number of references that can be loaded in memory at once.
        categorical_threshold : int
            Encode urls as pandas.Categorical to reduce memory footprint if the ratio
            of the number of unique urls to total number of refs for each variable
            is greater than or equal to this number. (default 10)
        engine: Literal["fastparquet","pyarrow"]
            Engine choice for reading parquet files. (default is "fastparquet")
        """

        self.root = root
        self.chunk_sizes = {}
        self.cat_thresh = categorical_threshold
        self.engine = engine
        self.cache_size = cache_size
        self.url = self.root + "/{field}/refs.{record}.parq"
        # TODO: derive fs from `root`
        self.fs = fsspec.filesystem("file") if fs is None else fs
        self.out_root = self.fs.unstrip_protocol(out_root or self.root)

        from importlib.util import find_spec

        if self.engine == "pyarrow" and find_spec("pyarrow") is None:
            raise ImportError("engine choice `pyarrow` is not installed.")

        # Apply `lru_cache` decorator manually per instance.
        # This way `self` reference is not held on class level.
        # WARNING: However, this means that self and its members are not reflected
        # in the cache key, so we expect they won't be mutated once a value is cached.
        self.listdir = lru_cache()(self.listdir)
        self._key_to_record = lru_cache(maxsize=4096)(self._key_to_record)

    def __getattr__(self, item):
        if item in ("_items", "record_size", "zmetadata"):
            self.setup()
            # avoid possible recursion if setup fails somehow
            return self.__dict__[item]
        raise AttributeError(item)

    def setup(self):
        self._items = {}
        self._items[".zmetadata"] = self.fs.cat_file(
            "/".join([self.root, ".zmetadata"])
        )
        met = json.loads(self._items[".zmetadata"])
        self.record_size = met["record_size"]
        self.zmetadata = met["metadata"]

        # Define function to open and decompress refs
        @lru_cache(maxsize=self.cache_size)
        def open_refs(field, record):
            """cached parquet file loader"""
            path = self.url.format(field=field, record=record)
            data = io.BytesIO(self.fs.cat_file(path))
            try:
                df = self.pd.read_parquet(data, engine=self.engine)
                refs = {c: df[c].to_numpy(copy=True) for c in df.columns}
            except OSError:
                refs = None
            return refs

        self.open_refs = open_refs

    @staticmethod
    def create(root, storage_options=None, fs=None, record_size=10000, **kwargs):
        """Make empty parquet reference set

        First deletes the contents of the given directory, if it exists.

        Parameters
        ----------
        root: str
            Directory to contain the output; will be created
        storage_options: dict | None
            For making the filesystem to use for writing is fs is None
        fs: FileSystem | None
            Filesystem for writing
        record_size: int
            Number of references per parquet file
        kwargs: passed to __init__

        Returns
        -------
        LazyReferenceMapper instance
        """
        met = {"metadata": {}, "record_size": record_size}
        if fs is None:
            fs, root = fsspec.core.url_to_fs(root, **(storage_options or {}))
        if fs.exists(root):
            fs.rm(root, recursive=True)
        fs.makedirs(root, exist_ok=True)
        fs.pipe("/".join([root, ".zmetadata"]), json.dumps(met).encode())
        return LazyReferenceMapper(root, fs, **kwargs)

    def listdir(self):
        """List top-level directories"""
        dirs = (p.rsplit("/", 1)[0] for p in self.zmetadata if not p.startswith(".z"))
        return set(dirs)

    def ls(self, path="", detail=True):
        """Shortcut file listings"""
        path = path.rstrip("/")
        pathdash = path + "/" if path else ""
        dirnames = self.listdir()
        dirs = [
            d
            for d in dirnames
            if d.startswith(pathdash) and "/" not in d.lstrip(pathdash)
        ]
        if dirs:
            others = {
                f
                for f in chain(
                    [".zmetadata"],
                    (name for name in self.zmetadata),
                    (name for name in self._items),
                )
                if f.startswith(pathdash) and "/" not in f.lstrip(pathdash)
            }
            if detail is False:
                others.update(dirs)
                return sorted(others)
            dirinfo = [{"name": name, "type": "directory", "size": 0} for name in dirs]
            fileinfo = [
                {
                    "name": name,
                    "type": "file",
                    "size": len(
                        json.dumps(self.zmetadata[name])
                        if name in self.zmetadata
                        else self._items[name]
                    ),
                }
                for name in others
            ]
            return sorted(dirinfo + fileinfo, key=lambda s: s["name"])
        field = path
        others = set(
            [name for name in self.zmetadata if name.startswith(f"{path}/")]
            + [name for name in self._items if name.startswith(f"{path}/")]
        )
        fileinfo = [
            {
                "name": name,
                "type": "file",
                "size": len(
                    json.dumps(self.zmetadata[name])
                    if name in self.zmetadata
                    else self._items[name]
                ),
            }
            for name in others
        ]
        keys = self._keys_in_field(field)

        if detail is False:
            return list(others) + list(keys)
        recs = self._generate_all_records(field)
        recinfo = [
            {"name": name, "type": "file", "size": rec[-1]}
            for name, rec in zip(keys, recs)
            if rec[0]  # filters out path==None, deleted/missing
        ]
        return fileinfo + recinfo

    def _load_one_key(self, key):
        """Get the reference for one key

        Returns bytes, one-element list or three-element list.
        """
        if key in self._items:
            return self._items[key]
        elif key in self.zmetadata:
            return json.dumps(self.zmetadata[key]).encode()
        elif "/" not in key or self._is_meta(key):
            raise KeyError(key)
        field, _ = key.rsplit("/", 1)
        record, ri, chunk_size = self._key_to_record(key)
        maybe = self._items.get((field, record), {}).get(ri, False)
        if maybe is None:
            # explicitly deleted
            raise KeyError
        elif maybe:
            return maybe
        elif chunk_size == 0:
            return b""

        # Chunk keys can be loaded from row group and cached in LRU cache
        try:
            refs = self.open_refs(field, record)
        except (ValueError, TypeError, FileNotFoundError) as exc:
            raise KeyError(key) from exc
        columns = ["path", "offset", "size", "raw"]
        selection = [refs[c][ri] if c in refs else None for c in columns]
        raw = selection[-1]
        if raw is not None:
            return raw
        if selection[0] is None:
            raise KeyError("This reference does not exist or has been deleted")
        if selection[1:3] == [0, 0]:
            # URL only
            return selection[:1]
        # URL, offset, size
        return selection[:3]

    def _key_to_record(self, key):
        """Details needed to construct a reference for one key"""
        field, chunk = key.rsplit("/", 1)
        chunk_sizes = self._get_chunk_sizes(field)
        if len(chunk_sizes) == 0:
            return 0, 0, 0
        chunk_idx = [int(c) for c in chunk.split(".")]
        chunk_number = ravel_multi_index(chunk_idx, chunk_sizes)
        record = chunk_number // self.record_size
        ri = chunk_number % self.record_size
        return record, ri, len(chunk_sizes)

    def _get_chunk_sizes(self, field):
        """The number of chunks along each axis for a given field"""
        if field not in self.chunk_sizes:
            zarray = self.zmetadata[f"{field}/.zarray"]
            size_ratio = [
                math.ceil(s / c) for s, c in zip(zarray["shape"], zarray["chunks"])
            ]
            self.chunk_sizes[field] = size_ratio or [1]
        return self.chunk_sizes[field]

    def _generate_record(self, field, record):
        """The references for a given parquet file of a given field"""
        refs = self.open_refs(field, record)
        it = iter(zip(*refs.values()))
        if len(refs) == 3:
            # All urls
            return (list(t) for t in it)
        elif len(refs) == 1:
            # All raws
            return refs["raw"]
        else:
            # Mix of urls and raws
            return (list(t[:3]) if not t[3] else t[3] for t in it)

    def _generate_all_records(self, field):
        """Load all the references within a field by iterating over the parquet files"""
        nrec = 1
        for ch in self._get_chunk_sizes(field):
            nrec *= ch
        nrec = math.ceil(nrec / self.record_size)
        for record in range(nrec):
            yield from self._generate_record(field, record)

    def values(self):
        return RefsValuesView(self)

    def items(self):
        return RefsItemsView(self)

    def __hash__(self):
        return id(self)

    def __getitem__(self, key):
        return self._load_one_key(key)

    def __setitem__(self, key, value):
        if "/" in key and not self._is_meta(key):
            field, chunk = key.rsplit("/", 1)
            record, i, _ = self._key_to_record(key)
            subdict = self._items.setdefault((field, record), {})
            subdict[i] = value
            if len(subdict) == self.record_size:
                self.write(field, record)
        else:
            # metadata or top-level
            if hasattr(value, "to_bytes"):
                val = value.to_bytes().decode()
            elif isinstance(value, bytes):
                val = value.decode()
            else:
                val = value
            self._items[key] = val
            new_value = json.loads(val)
            self.zmetadata[key] = {**self.zmetadata.get(key, {}), **new_value}

    @staticmethod
    def _is_meta(key):
        return key.startswith(".z") or "/.z" in key

    def __delitem__(self, key):
        if key in self._items:
            del self._items[key]
        elif key in self.zmetadata:
            del self.zmetadata[key]
        else:
            if "/" in key and not self._is_meta(key):
                field, _ = key.rsplit("/", 1)
                record, i, _ = self._key_to_record(key)
                subdict = self._items.setdefault((field, record), {})
                subdict[i] = None
                if len(subdict) == self.record_size:
                    self.write(field, record)
            else:
                # metadata or top-level
                self._items[key] = None

    def write(self, field, record, base_url=None, storage_options=None):
        # extra requirements if writing
        import kerchunk.df
        import numpy as np
        import pandas as pd

        partition = self._items[(field, record)]
        original = False
        if len(partition) < self.record_size:
            try:
                original = self.open_refs(field, record)
            except OSError:
                pass

        if original:
            paths = original["path"]
            offsets = original["offset"]
            sizes = original["size"]
            raws = original["raw"]
        else:
            paths = np.full(self.record_size, np.nan, dtype="O")
            offsets = np.zeros(self.record_size, dtype="int64")
            sizes = np.zeros(self.record_size, dtype="int64")
            raws = np.full(self.record_size, np.nan, dtype="O")
        for j, data in partition.items():
            if isinstance(data, list):
                if (
                    str(paths.dtype) == "category"
                    and data[0] not in paths.dtype.categories
                ):
                    paths = paths.add_categories(data[0])
                paths[j] = data[0]
                if len(data) > 1:
                    offsets[j] = data[1]
                    sizes[j] = data[2]
            elif data is None:
                # delete
                paths[j] = None
                offsets[j] = 0
                sizes[j] = 0
                raws[j] = None
            else:
                # this is the only call into kerchunk, could remove
                raws[j] = kerchunk.df._proc_raw(data)
        # TODO: only save needed columns
        df = pd.DataFrame(
            {
                "path": paths,
                "offset": offsets,
                "size": sizes,
                "raw": raws,
            },
            copy=False,
        )
        if df.path.count() / (df.path.nunique() or 1) > self.cat_thresh:
            df["path"] = df["path"].astype("category")
        object_encoding = {"raw": "bytes", "path": "utf8"}
        has_nulls = ["path", "raw"]

        fn = f"{base_url or self.out_root}/{field}/refs.{record}.parq"
        self.fs.mkdirs(f"{base_url or self.out_root}/{field}", exist_ok=True)

        if self.engine == "pyarrow":
            df_backend_kwargs = {"write_statistics": False}
        elif self.engine == "fastparquet":
            df_backend_kwargs = {
                "stats": False,
                "object_encoding": object_encoding,
                "has_nulls": has_nulls,
            }
        else:
            raise NotImplementedError(f"{self.engine} not supported")
        df.to_parquet(
            fn,
            engine=self.engine,
            storage_options=storage_options
            or getattr(self.fs, "storage_options", None),
            compression="zstd",
            index=False,
            **df_backend_kwargs,
        )

        partition.clear()
        self._items.pop((field, record))

    def flush(self, base_url=None, storage_options=None):
        """Output any modified or deleted keys

        Parameters
        ----------
        base_url: str
            Location of the output
        """

        # write what we have so far and clear sub chunks
        for thing in list(self._items):
            if isinstance(thing, tuple):
                field, record = thing
                self.write(
                    field,
                    record,
                    base_url=base_url,
                    storage_options=storage_options,
                )

        # gather .zmetadata from self._items and write that too
        for k in list(self._items):
            if k != ".zmetadata" and ".z" in k:
                self.zmetadata[k] = json.loads(self._items.pop(k))
        met = {"metadata": self.zmetadata, "record_size": self.record_size}
        self._items.clear()
        self._items[".zmetadata"] = json.dumps(met).encode()
        self.fs.pipe(
            "/".join([base_url or self.out_root, ".zmetadata"]),
            self._items[".zmetadata"],
        )

        # TODO: only clear those that we wrote to?
        self.open_refs.cache_clear()

    def __len__(self):
        # Caveat: This counts expected references, not actual - but is fast
        count = 0
        for field in self.listdir():
            if field.startswith("."):
                count += 1
            else:
                count += math.prod(self._get_chunk_sizes(field))
        count += len(self.zmetadata)  # all metadata keys
        # any other files not in reference partitions
        count += sum(1 for _ in self._items if not isinstance(_, tuple))
        return count

    def __iter__(self):
        # Caveat: returns only existing keys, so the number of these does not
        #  match len(self)
        metas = set(self.zmetadata)
        metas.update(self._items)
        for bit in metas:
            if isinstance(bit, str):
                yield bit
        for field in self.listdir():
            for k in self._keys_in_field(field):
                if k in self:
                    yield k

    def __contains__(self, item):
        try:
            self._load_one_key(item)
            return True
        except KeyError:
            return False

    def _keys_in_field(self, field):
        """List key names in given field

        Produces strings like "field/x.y" appropriate from the chunking of the array
        """
        chunk_sizes = self._get_chunk_sizes(field)
        if len(chunk_sizes) == 0:
            yield field + "/0"
            return
        inds = itertools.product(*(range(i) for i in chunk_sizes))
        for ind in inds:
            yield field + "/" + ".".join([str(c) for c in ind])


class ReferenceFileSystem(AsyncFileSystem):
    """View byte ranges of some other file as a file system
    Initial version: single file system target, which must support
    async, and must allow start and end args in _cat_file. Later versions
    may allow multiple arbitrary URLs for the targets.
    This FileSystem is read-only. It is designed to be used with async
    targets (for now). We do not get original file details from the target FS.
    Configuration is by passing a dict of references at init, or a URL to
    a JSON file containing the same; this dict
    can also contain concrete data for some set of paths.
    Reference dict format:
    {path0: bytes_data, path1: (target_url, offset, size)}
    https://github.com/fsspec/kerchunk/blob/main/README.md

    A path may also be described as an ordered list of segments, forming one
    logical file by concatenation. Each segment is one of:

    - ``[url]`` or ``[url, offset, size]``: bytes from a remote file (a
      whole-file segment has unknown length until read, so such a file
      cannot be opened for random access or sized)
    - ``{"inline": data}``: literal bytes (str, bytes or ``base64:``-encoded)
    - ``{"zeros": n}``: ``n`` zero bytes

    A remote URL of the form ``reference://path`` resolves to another
    reference in this same set; cycles are rejected. The legacy flat forms
    (inline data, ``[url]``, ``[url, offset, size]``) are unchanged.

    simple_references: if True (default), no jinja interpreting is done,
        which is the safe option.
    """

    protocol = "reference"
    cachable = False

    def __init__(
        self,
        fo,
        target=None,
        ref_storage_args=None,
        target_protocol=None,
        target_options=None,
        remote_protocol=None,
        remote_options=None,
        fs=None,
        template_overrides=None,
        simple_templates=True,
        max_gap=64_000,
        max_block=256_000_000,
        cache_size=128,
        **kwargs,
    ):
        """
        Parameters
        ----------
        fo : dict or str
            The set of references to use for this instance, with a structure as above.
            If str referencing a JSON file, will use fsspec.open, in conjunction
            with target_options and target_protocol to open and parse JSON at this
            location. If a directory, then assume references are a set of parquet
            files to be loaded lazily.
        target : str
            For any references having target_url as None, this is the default file
            target to use
        ref_storage_args : dict
            If references is a str, use these kwargs for loading the JSON file.
            Deprecated: use target_options instead.
        target_protocol : str
            Used for loading the reference file, if it is a path. If None, protocol
            will be derived from the given path
        target_options : dict
            Extra FS options for loading the reference file ``fo``, if given as a path
        remote_protocol : str
            The protocol of the filesystem on which the references will be evaluated
            (unless fs is provided). If not given, will be derived from the first
            URL that has a protocol in the templates or in the references, in that
            order.
        remote_options : dict
            kwargs to go with remote_protocol
        fs : AbstractFileSystem | dict(str, (AbstractFileSystem | dict))
            Directly provide a file system(s):
                - a single filesystem instance
                - a dict of protocol:filesystem, where each value is either a filesystem
                  instance, or a dict of kwargs that can be used to create in
                  instance for the given protocol

            If this is given, remote_options and remote_protocol are ignored.
        template_overrides : dict
            Swap out any templates in the references file with these - useful for
            testing.
        simple_templates: bool
            Whether templates can be processed with simple replace (True) or if
            jinja  is needed (False, much slower). All reference sets produced by
            ``kerchunk`` are simple in this sense, but the spec allows for complex.
        max_gap, max_block: int
            For merging multiple concurrent requests to the same remote file.
            Neighboring byte ranges will only be merged when their
            inter-range gap is <= ``max_gap``. Default is 64KB. Set to 0
            to only merge when it requires no extra bytes. Pass a negative
            number to disable merging, appropriate for local target files.
            Neighboring byte ranges will only be merged when the size of
            the aggregated range is <= ``max_block``. Default is 256MB.
        cache_size : int
            Maximum size of LRU cache, where cache_size*record_size denotes
            the total number of references that can be loaded in memory at once.
            Only used for lazily loaded references.
        kwargs : passed to parent class
        """
        super().__init__(**kwargs)
        self.target = target
        self.template_overrides = template_overrides
        self.simple_templates = simple_templates
        self.templates = {}
        self.fss = {}
        self._dircache = {}
        self.max_gap = max_gap
        self.max_block = max_block
        if isinstance(fo, str):
            dic = dict(
                **(ref_storage_args or target_options or {}), protocol=target_protocol
            )
            ref_fs, fo2 = fsspec.core.url_to_fs(fo, **dic)
            if ".json" not in fo2 and (
                fo.endswith(("parq", "parquet", "/")) or ref_fs.isdir(fo2)
            ):
                # Lazy parquet refs
                logger.info("Open lazy reference dict from URL %s", fo)
                self.references = LazyReferenceMapper(
                    fo2,
                    fs=ref_fs,
                    cache_size=cache_size,
                )
            else:
                # text JSON
                with fsspec.open(fo, "rb", **dic) as f:
                    logger.info("Read reference from URL %s", fo)
                    text = json.load(f)
                self._process_references(text, template_overrides)
        else:
            # dictionaries
            self._process_references(fo, template_overrides)
        if isinstance(fs, dict):
            self.fss = {
                k: (
                    fsspec.filesystem(k.split(":", 1)[0], **opts)
                    if isinstance(opts, dict)
                    else opts
                )
                for k, opts in fs.items()
            }
            if None not in self.fss:
                self.fss[None] = filesystem("file")
            return
        if fs is not None:
            # single remote FS
            remote_protocol = (
                fs.protocol[0] if isinstance(fs.protocol, tuple) else fs.protocol
            )
            self.fss[remote_protocol] = fs

        if remote_protocol is None:
            # get single protocol from any templates
            for ref in self.templates.values():
                if callable(ref):
                    ref = ref()
                protocol, _ = fsspec.core.split_protocol(ref)
                if protocol and protocol not in self.fss:
                    fs = filesystem(protocol, **(remote_options or {}))
                    self.fss[protocol] = fs
        if remote_protocol is None:
            # get single protocol from references
            # TODO: warning here, since this can be very expensive?
            for ref in self.references.values():
                if callable(ref):
                    ref = ref()
                if isinstance(ref, list) and ref[0]:
                    protocol, _ = fsspec.core.split_protocol(ref[0])
                    if protocol not in self.fss:
                        fs = filesystem(protocol, **(remote_options or {}))
                        self.fss[protocol] = fs
                        # only use first remote URL
                        break

        if remote_protocol and remote_protocol not in self.fss:
            fs = filesystem(remote_protocol, **(remote_options or {}))
            self.fss[remote_protocol] = fs

        self.fss[None] = fs or filesystem("file")  # default one
        # Wrap any non-async filesystems to ensure async methods are available below
        for k, f in self.fss.items():
            if not f.async_impl:
                self.fss[k] = AsyncFileSystemWrapper(f, asynchronous=self.asynchronous)
            elif self.asynchronous ^ f.asynchronous:
                raise ValueError(
                    "Reference-FS's target filesystem must have same value "
                    "of asynchronous"
                )

    def _resolve_segments(self, path, _chain=()):
        """Resolve a path to normalized, non-nested segments.

        Returns a list of tagged segments (see ``_normalize_segment``) in
        which every remote URL points at a real target: ``reference://``
        URLs are followed into this same reference set, and legacy
        single-range references are rewritten as one remote segment.
        """
        path = self._strip_protocol(path)
        if path in _chain:
            raise ReferenceResolutionError(
                f"Reference cycle: {' -> '.join([*_chain, path])}"
            )
        if len(_chain) >= MAX_REFERENCE_HOPS:
            raise ReferenceResolutionError(
                f"Reference chain longer than {MAX_REFERENCE_HOPS} hops at {path!r}"
            )
        try:
            part = self.references[path]
        except KeyError as exc:
            raise FileNotFoundError(path) from exc
        if isinstance(part, str):
            part = part.encode()
        if hasattr(part, "to_bytes"):
            part = part.to_bytes()
        if isinstance(part, bytes):
            return [("inline", _inline_data(part))]
        if _is_multi_segment(part):
            out = []
            for segment in _normalize_segments(part, path):
                out.extend(self._resolve_segment(segment, path, _chain))
            return out
        if len(part) == 1:
            segment = ("range", part[0], 0, None)
        else:
            segment = ("range", part[0], part[1], part[2])
        return self._resolve_segment(segment, path, _chain)

    def _resolve_segment(self, segment, path, _chain):
        """Expand one segment, following ``reference://`` URLs in place."""
        if segment[0] != "range":
            return [segment]
        _, url, offset, length = segment
        protocol, inner = split_protocol(url) if url else (None, None)
        if protocol != "reference":
            return [segment]
        nested = self._resolve_segments(inner, (*_chain, path))
        if length is None and offset == 0:
            return nested
        nested_length = _known_length(nested)
        if nested_length is None:
            raise ReferenceResolutionError(
                f"Nested reference {url!r} in {path!r} has unknown length; "
                "only whole nested references may be unbounded"
            )
        if offset > nested_length or offset + length > nested_length:
            raise ReferenceResolutionError(
                f"Nested range [{offset}, {offset + length}) of {url!r} in "
                f"{path!r} exceeds its length {nested_length}"
            )
        return [
            self._shift_segment(seg, local_start, local_end)
            for seg, local_start, local_end in _slice_segments(
                nested, offset, offset + length
            )
        ]

    @staticmethod
    def _shift_segment(segment, local_start, local_end):
        """A copy of a normalized segment trimmed to a local window."""
        kind = segment[0]
        if kind == "inline":
            return ("inline", segment[1][local_start:local_end])
        if kind == "zeros":
            return ("zeros", local_end - local_start)
        _, url, offset, length = segment
        if local_end is None:
            return ("range", url, offset + local_start, None)
        return ("range", url, offset + local_start, local_end - local_start)

    def _segments_size(self, path):
        """Logical length of a path, or None when it cannot be known."""
        return _known_length(self._resolve_segments(path))

    def _segment_requests(self, segments):
        """Split segments into local pieces and grouped remote requests."""
        pieces = [None] * len(segments)
        by_protocol = {}
        for i, segment in enumerate(segments):
            kind = segment[0]
            if kind == "inline":
                pieces[i] = segment[1]
            elif kind == "zeros":
                pieces[i] = b"\x00" * segment[1]
            else:
                _, url, offset, length = segment
                if url is None:
                    url = self.target
                protocol, _ = split_protocol(url)
                by_protocol.setdefault(protocol, []).append(
                    (i, url, offset, length)
                )
        return pieces, by_protocol

    @staticmethod
    def _place_fetched(pieces, entries, fetched):
        """Validate fetched ranges and slot them into their piece positions.

        A remote range that comes back short raises instead of silently
        shifting the coordinates of later segments.
        """
        for (i, url, offset, length), data in zip(entries, fetched):
            if isinstance(data, Exception):
                raise ReferenceNotReachable(url, url) from data
            if length is not None and len(data) != length:
                raise ReferenceNotReachable(
                    url,
                    url,
                    f"short read: expected {length} bytes, got {len(data)}",
                )
            pieces[i] = data

    def _fetch_segments(self, segments, fetch):
        """Fetch remote segments through a sync ``fetch`` and concatenate.

        ``fetch`` takes ``(protocol, urls, starts, ends)`` and returns the
        bytes for each range in order. Inline and zero-fill segments are
        produced locally.
        """
        pieces, by_protocol = self._segment_requests(segments)
        for protocol, entries in by_protocol.items():
            urls = [entry[1] for entry in entries]
            starts = [entry[2] for entry in entries]
            ends = [
                None if entry[3] is None else entry[2] + entry[3]
                for entry in entries
            ]
            fetched = fetch(protocol, urls, starts, ends)
            self._place_fetched(pieces, entries, fetched)
        return b"".join(pieces)

    async def _fetch_segments_async(self, segments, fetch):
        """Async counterpart of :meth:`_fetch_segments`."""
        pieces, by_protocol = self._segment_requests(segments)
        for protocol, entries in by_protocol.items():
            urls = [entry[1] for entry in entries]
            starts = [entry[2] for entry in entries]
            ends = [
                None if entry[3] is None else entry[2] + entry[3]
                for entry in entries
            ]
            fetched = await fetch(protocol, urls, starts, ends)
            self._place_fetched(pieces, entries, fetched)
        return b"".join(pieces)

    def _merged_ranges(self, urls, starts, ends):
        """Merge neighbouring ranges on the same underlying URL."""
        return merge_offset_ranges(
            list(urls),
            list(starts),
            list(ends),
            sort=True,
            max_gap=self.max_gap,
            max_block=self.max_block,
        )

    def _fetch_ranges(self, protocol, urls, starts, ends):
        """Fetch byte ranges from one underlying filesystem, merging neighbours."""
        fs = self.fss[protocol]
        merged = self._merged_ranges(urls, starts, ends)
        data = fs.cat_ranges(*merged)
        return _extract_ranges(urls, starts, ends, *merged, data)

    async def _fetch_ranges_async(self, protocol, urls, starts, ends):
        fs = self.fss[protocol]
        merged = self._merged_ranges(urls, starts, ends)
        data = await fs._cat_ranges(*merged)
        return _extract_ranges(urls, starts, ends, *merged, data)

    def _cat_common(self, path, start=None, end=None):
        path = self._strip_protocol(path)
        logger.debug(f"cat: {path}")
        try:
            part = self.references[path]
        except KeyError as exc:
            raise FileNotFoundError(path) from exc
        if isinstance(part, str):
            part = part.encode()
        if hasattr(part, "to_bytes"):
            part = part.to_bytes()
        if isinstance(part, bytes):
            logger.debug(f"Reference: {path}, type bytes")
            return _inline_data(part), None, None

        if len(part) == 1:
            logger.debug(f"Reference: {path}, whole file => {part}")
            url = part[0]
            start1, end1 = start, end
        else:
            url, start0, size = part
            logger.debug(f"Reference: {path} => {url}, offset {start0}, size {size}")
            end0 = start0 + size

            # Resolve offsets like slicing the part, so they never reach target
            # bytes outside the referenced range.
            if start is not None:
                if start >= 0:
                    start1 = min(start0 + start, end0)
                else:
                    start1 = max(end0 + start, start0)
            else:
                start1 = start0
            if end is not None:
                if end >= 0:
                    end1 = min(start0 + end, end0)
                else:
                    end1 = max(end0 + end, start0)
            else:
                end1 = end0
        if url is None:
            url = self.target
        return url, start1, end1

    def _cat_multi(self, path, start, end, fetch, segments=None):
        """Assemble a multi-segment logical file over a logical window."""
        if segments is None:
            segments = self._resolve_segments(path)
        plan = _slice_segments(segments, start, end)
        shifted = [self._shift_segment(seg, s, e) for seg, s, e in plan]
        try:
            return self._fetch_segments(shifted, fetch)
        except ReferenceNotReachable as exc:
            raise ReferenceNotReachable(path, exc.target, *exc.args[2:]) from exc

    async def _cat_multi_async(self, path, start, end, fetch, segments=None):
        """Async counterpart of :meth:`_cat_multi`."""
        if segments is None:
            segments = self._resolve_segments(path)
        plan = _slice_segments(segments, start, end)
        shifted = [self._shift_segment(seg, s, e) for seg, s, e in plan]
        try:
            return await self._fetch_segments_async(shifted, fetch)
        except ReferenceNotReachable as exc:
            raise ReferenceNotReachable(path, exc.target, *exc.args[2:]) from exc

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        path = self._strip_protocol(path)
        if _is_multi_segment(self.references.get(path)):
            return await self._cat_multi_async(
                path, start, end, self._fetch_ranges_async
            )
        part_or_url, start0, end0 = self._cat_common(path, start=start, end=end)
        if isinstance(part_or_url, bytes):
            return part_or_url[start:end]
        protocol, _ = split_protocol(part_or_url)
        try:
            return await self.fss[protocol]._cat_file(
                part_or_url, start=start0, end=end0
            )
        except Exception as e:
            raise ReferenceNotReachable(path, part_or_url) from e

    def cat_file(self, path, start=None, end=None, **kwargs):
        path = self._strip_protocol(path)
        if _is_multi_segment(self.references.get(path)):
            return self._cat_multi(path, start, end, self._fetch_ranges)
        part_or_url, start0, end0 = self._cat_common(path, start=start, end=end)
        if isinstance(part_or_url, bytes):
            return part_or_url[start:end]
        protocol, _ = split_protocol(part_or_url)
        try:
            return self.fss[protocol].cat_file(part_or_url, start=start0, end=end0)
        except Exception as e:
            raise ReferenceNotReachable(path, part_or_url) from e

    def pipe_file(self, path, value, **_):
        """Temporarily add binary data or reference as a file"""
        self.references[path] = value

    async def _get_file(self, rpath, lpath, **kwargs):
        if self.isdir(rpath):
            return os.makedirs(lpath, exist_ok=True)
        data = await self._cat_file(rpath)
        with open(lpath, "wb") as f:
            f.write(data)

    def get_file(self, rpath, lpath, callback=DEFAULT_CALLBACK, **kwargs):
        if self.isdir(rpath):
            return os.makedirs(lpath, exist_ok=True)
        data = self.cat_file(rpath, **kwargs)
        callback.set_size(len(data))
        if isfilelike(lpath):
            lpath.write(data)
        else:
            with open(lpath, "wb") as f:
                f.write(data)
        callback.absolute_update(len(data))

    def get(self, rpath, lpath, recursive=False, maxdepth=None, **kwargs):
        if recursive:
            # trigger directory build
            self.ls("")
        rpath = self.expand_path(rpath, recursive=recursive, maxdepth=maxdepth)
        fs = fsspec.filesystem("file", auto_mkdir=True)
        targets = other_paths(rpath, lpath)
        if isinstance(lpath, str):
            # The names came from the source listing; ".." in one of them
            # would otherwise place the copy above the destination. When
            # lpath is a list the caller named every destination itself.
            check_contained(lpath, targets)
        if recursive:
            data = self.cat([r for r in rpath if not self.isdir(r)])
        else:
            data = self.cat(rpath)
        for remote, local in zip(rpath, targets):
            if remote in data:
                fs.pipe_file(local, data[remote])

    def _cat_many_multi(self, paths, on_error):
        """Batch-read multi-segment logical files with cross-file merging.

        Remote segments from every logical file are collected per underlying
        filesystem and fetched in one merged round trip, while inline and
        zero-fill segments are produced locally. Returns ``(out, done)`` where
        ``done`` names the paths already handled here (including any failures
        recorded under ``on_error``).
        """
        out = {}
        done = set()
        per_file = {}
        requests = {}
        for path in paths:
            path = self._strip_protocol(path)
            done.add(path)
            try:
                segments = self._resolve_segments(path)
                shifted = [
                    self._shift_segment(seg, s, e)
                    for seg, s, e in _slice_segments(segments, None, None)
                ]
            except (FileNotFoundError, ReferenceResolutionError) as exc:
                if on_error == "raise":
                    raise
                if on_error != "omit":
                    out[path] = exc
                continue
            pieces = [None] * len(shifted)
            per_file[path] = pieces
            for i, segment in enumerate(shifted):
                kind = segment[0]
                if kind == "inline":
                    pieces[i] = segment[1]
                elif kind == "zeros":
                    pieces[i] = b"\x00" * segment[1]
                else:
                    _, url, offset, length = segment
                    if url is None:
                        url = self.target
                    protocol, _ = split_protocol(url)
                    requests.setdefault(protocol, []).append(
                        (path, i, url, offset, length)
                    )
        for protocol, entries in requests.items():
            urls = [entry[2] for entry in entries]
            starts = [entry[3] for entry in entries]
            ends = [
                None if entry[4] is None else entry[3] + entry[4]
                for entry in entries
            ]
            try:
                fetched = self._fetch_ranges(protocol, urls, starts, ends)
            except Exception as exc:
                for path, _, _, _, _ in entries:
                    wrapped = ReferenceNotReachable(path, protocol)
                    wrapped.__cause__ = exc
                    if on_error == "raise":
                        raise wrapped
                    if on_error != "omit":
                        out.setdefault(path, wrapped)
                continue
            for (path, i, url, offset, length), data in zip(entries, fetched):
                if path in out:
                    continue
                if isinstance(data, Exception):
                    wrapped = ReferenceNotReachable(path, url)
                    wrapped.__cause__ = data
                    if on_error == "raise":
                        raise wrapped
                    if on_error != "omit":
                        out[path] = wrapped
                    continue
                if length is not None and len(data) != length:
                    wrapped = ReferenceNotReachable(
                        path,
                        url,
                        f"short read: expected {length} bytes, got {len(data)}",
                    )
                    if on_error == "raise":
                        raise wrapped
                    if on_error != "omit":
                        out[path] = wrapped
                    continue
                per_file[path][i] = data
        for path, pieces in per_file.items():
            if path not in out:
                out[path] = b"".join(pieces)
        return out, done

    def cat(self, path, recursive=False, on_error="raise", **kwargs):
        if isinstance(path, str) and recursive:
            raise NotImplementedError
        if isinstance(path, list) and (recursive or any("*" in p for p in path)):
            raise NotImplementedError
        # Multi-segment references flatten into per-segment remote requests and
        # are assembled here, so they never reach the legacy single-range path.
        if isinstance(path, str):
            multi_paths = (
                [path] if _is_multi_segment(self.references.get(path)) else []
            )
            single_paths = [] if multi_paths else [path]
        else:
            multi_paths = [
                p for p in path if _is_multi_segment(self.references.get(p))
            ]
            single_paths = [p for p in path if p not in multi_paths]
        out, multi_done = self._cat_many_multi(multi_paths, on_error)
        if single_paths:
            # TODO: if references is lazy, pre-fetch all paths in batch before access
            proto_dict = _protocol_groups(single_paths, self.references)
        else:
            proto_dict = {}
        for proto, paths in proto_dict.items():
            fs = self.fss[proto]
            urls, starts, ends, valid_paths = [], [], [], []
            for p in paths:
                # find references or label not-found. Early exit if any not
                # found and on_error is "raise"
                try:
                    u, s, e = self._cat_common(p)
                    if not isinstance(u, (bytes, str)):
                        # nan/None from parquet
                        continue
                except FileNotFoundError as err:
                    if on_error == "raise":
                        raise
                    if on_error != "omit":
                        out[p] = err
                else:
                    urls.append(u)
                    starts.append(s)
                    ends.append(e)
                    valid_paths.append(p)

            # process references into form for merging
            urls2 = []
            starts2 = []
            ends2 = []
            paths2 = []
            whole_files = set()
            for u, s, e, p in zip(urls, starts, ends, valid_paths):
                if isinstance(u, bytes):
                    # data
                    out[p] = u
                elif s is None:
                    # whole file - limits are None, None, but no further
                    # entries take for this file
                    whole_files.add(u)
                    urls2.append(u)
                    starts2.append(s)
                    ends2.append(e)
                    paths2.append(p)
            for u, s, e, p in zip(urls, starts, ends, valid_paths):
                # second run to account for files that are to be loaded whole
                if s is not None and u not in whole_files:
                    urls2.append(u)
                    starts2.append(s)
                    ends2.append(e)
                    paths2.append(p)

            # merge and fetch consolidated ranges
            new_paths, new_starts, new_ends = merge_offset_ranges(
                list(urls2),
                list(starts2),
                list(ends2),
                sort=True,
                max_gap=self.max_gap,
                max_block=self.max_block,
            )
            bytes_out = fs.cat_ranges(new_paths, new_starts, new_ends)

            # unbundle from merged bytes - simple approach
            for u, s, e, p in zip(urls, starts, ends, valid_paths):
                if p in out:
                    continue  # was bytes, already handled
                for np, ns, ne, b in zip(new_paths, new_starts, new_ends, bytes_out):
                    if np == u and (ns is None or ne is None):
                        if isinstance(b, Exception):
                            out[p] = b
                        else:
                            out[p] = b[s:e]
                    elif np == u and s >= ns and e <= ne:
                        if isinstance(b, Exception):
                            out[p] = b
                        else:
                            out[p] = b[s - ns : (e - ne) or None]

        for k, v in out.copy().items():
            # these were valid references, but fetch failed, so transform exc.
            # Multi-segment failures were already wrapped by _cat_many_multi.
            if (
                isinstance(v, Exception)
                and k in self.references
                and k not in multi_done
            ):
                ex = out[k]
                new_ex = ReferenceNotReachable(k, self.references[k])
                new_ex.__cause__ = ex
                if on_error == "raise":
                    raise new_ex
                elif on_error != "omit":
                    out[k] = new_ex
                else:
                    del out[k]

        if (
            len(out) == 1
            and isinstance(path, str)
            and "*" not in path
            and not (on_error == "omit" and not out)
        ):
            return _first(out)
        return out

    def _process_references(self, references, template_overrides=None):
        vers = references.get("version", None)
        if vers is None:
            self._process_references0(references)
        elif vers == 1:
            self._process_references1(references, template_overrides=template_overrides)
        else:
            raise ValueError(f"Unknown reference spec version: {vers}")
        # TODO: we make dircache by iterating over all entries, but for Spec >= 1,
        #  can replace with programmatic. Is it even needed for mapper interface?

    def _process_references0(self, references):
        """Make reference dict for Spec Version 0"""
        if isinstance(references, dict):
            # do not do this for lazy/parquet backend, which will not make dicts,
            # but must remain writable in the original object
            references = {
                key: json.dumps(val) if isinstance(val, dict) else val
                for key, val in references.items()
            }
        self.references = references

    def _process_references1(self, references, template_overrides=None):
        if not self.simple_templates or self.templates:
            import jinja2.sandbox
        self.references = {}
        self._process_templates(references.get("templates", {}))

        @lru_cache(1000)
        def _render_jinja(u):
            return (
                jinja2.sandbox.SandboxedEnvironment()
                .from_string(u)
                .render(**self.templates)
            )

        for k, v in references.get("refs", {}).items():
            if isinstance(v, str):
                if v.startswith("base64:"):
                    self.references[k] = base64.b64decode(v[7:])
                self.references[k] = v
            elif isinstance(v, dict):
                self.references[k] = json.dumps(v)
            elif self.templates and _is_multi_segment(v):
                self.references[k] = [
                    self._render_segment_template(segment, _render_jinja)
                    for segment in v
                ]
            elif self.templates and isinstance(v[0], str):
                u = v[0]
                if "{{" in u:
                    if self.simple_templates:
                        u = (
                            u.replace("{{", "{")
                            .replace("}}", "}")
                            .format(**self.templates)
                        )
                    else:
                        u = _render_jinja(u)
                self.references[k] = [u] if len(v) == 1 else [u, v[1], v[2]]
            else:
                self.references[k] = v
        self.references.update(self._process_gen(references.get("gen", [])))

    def _render_segment_template(self, segment, _render_jinja):
        """Expand URL templates inside one segment; inline/zeros pass through."""
        if not isinstance(segment, (list, tuple)) or not segment:
            return segment
        url = segment[0]
        if not isinstance(url, str) or "{{" not in url:
            return segment
        if self.simple_templates:
            url = url.replace("{{", "{").replace("}}", "}").format(
                **self.templates
            )
        else:
            url = _render_jinja(url)
        return [url, *segment[1:]]

    def _process_templates(self, tmp):
        self.templates = {}
        if self.template_overrides is not None:
            tmp.update(self.template_overrides)
        for k, v in tmp.items():
            if "{{" in v:
                import jinja2.sandbox

                self.templates[k] = (
                    lambda temp=v, **kwargs: jinja2.sandbox.SandboxedEnvironment()
                    .from_string(temp)
                    .render(**kwargs)
                )
            else:
                self.templates[k] = v

    def _process_gen(self, gens):
        out = {}
        if self.simple_templates:
            return out
        for gen in gens:
            dimension = {
                k: (
                    v
                    if isinstance(v, list)
                    else range(v.get("start", 0), v["stop"], v.get("step", 1))
                )
                for k, v in gen["dimensions"].items()
            }
            products = (
                dict(zip(dimension.keys(), values))
                for values in itertools.product(*dimension.values())
            )
            for pr in products:
                import jinja2.sandbox

                key = (
                    jinja2.sandbox.SandboxedEnvironment()
                    .from_string(gen["key"])
                    .render(**pr, **self.templates)
                )
                url = (
                    jinja2.sandbox.SandboxedEnvironment()
                    .from_string(gen["url"])
                    .render(**pr, **self.templates)
                )
                if ("offset" in gen) and ("length" in gen):
                    offset = int(
                        jinja2.sandbox.SandboxedEnvironment()
                        .from_string(gen["offset"])
                        .render(**pr, **self.templates)
                    )
                    length = int(
                        jinja2.sandbox.SandboxedEnvironment()
                        .from_string(gen["length"])
                        .render(**pr, **self.templates)
                    )
                    out[key] = [url, offset, length]
                elif ("offset" in gen) ^ ("length" in gen):
                    raise ValueError(
                        "Both 'offset' and 'length' are required for a "
                        "reference generator entry if either is provided."
                    )
                else:
                    out[key] = [url]
        return out

    def _dircache_from_items(self):
        self.dircache = {"": []}
        it = self.references.items()
        for path, part in it:
            if isinstance(part, (bytes, str)) or hasattr(part, "to_bytes"):
                size = len(_inline_data(part))
            elif _is_multi_segment(part):
                size = _known_length(_normalize_segments(part, path))
            elif len(part) == 1:
                size = None
            else:
                _, _, size = part
            par = path.rsplit("/", 1)[0] if "/" in path else ""
            par0 = par
            subdirs = [par0]
            while par0 and par0 not in self.dircache:
                # collect parent directories
                par0 = self._parent(par0)
                subdirs.append(par0)

            subdirs.reverse()
            for parent, child in zip(subdirs, subdirs[1:]):
                # register newly discovered directories
                assert child not in self.dircache
                assert parent in self.dircache
                self.dircache[parent].append(
                    {"name": child, "type": "directory", "size": 0}
                )
                self.dircache[child] = []

            self.dircache[par].append({"name": path, "type": "file", "size": size})

    def _open(self, path, mode="rb", block_size=None, cache_options=None, **kwargs):
        if _is_multi_segment(self.references.get(self._strip_protocol(path))):
            return MultiSegmentFile(self, path, mode, block_size=block_size)
        part_or_url, start0, end0 = self._cat_common(path)
        # This logic is kept outside `ReferenceFile` to avoid unnecessary redirection.
        # That does mean `_cat_common` gets called twice if it eventually reaches `ReferenceFile`.
        if isinstance(part_or_url, bytes):
            return io.BytesIO(part_or_url[start0:end0])

        protocol, _ = split_protocol(part_or_url)
        if start0 is None and end0 is None:
            return self.fss[protocol]._open(
                part_or_url,
                mode,
                block_size=block_size,
                cache_options=cache_options,
                **kwargs,
            )

        return ReferenceFile(
            self,
            path,
            mode,
            block_size=block_size,
            cache_options=cache_options,
            **kwargs,
        )

    def ls(self, path, detail=True, **kwargs):
        logger.debug("list %s", path)
        path = self._strip_protocol(path)
        if isinstance(self.references, LazyReferenceMapper):
            try:
                return self.references.ls(path, detail)
            except KeyError:
                pass
            raise FileNotFoundError(f"'{path}' is not a known key")
        if not self.dircache:
            self._dircache_from_items()
        out = self._ls_from_cache(path)
        if out is None:
            raise FileNotFoundError(path)
        if detail:
            return out
        return [o["name"] for o in out]

    def exists(self, path, **kwargs):  # overwrite auto-sync version
        return self.isdir(path) or self.isfile(path)

    def isdir(self, path):  # overwrite auto-sync version
        if self.dircache:
            return path in self.dircache
        elif isinstance(self.references, LazyReferenceMapper):
            return path in self.references.listdir()
        else:
            # this may be faster than building dircache for single calls, but
            # by looping will be slow for many calls; could cache it?
            return any(_.startswith(f"{path}/") for _ in self.references)

    def isfile(self, path):  # overwrite auto-sync version
        return path in self.references

    async def _ls(self, path, detail=True, **kwargs):  # calls fast sync code
        return self.ls(path, detail, **kwargs)

    def find(self, path, maxdepth=None, withdirs=False, detail=False, **kwargs):
        if withdirs:
            return super().find(
                path, maxdepth=maxdepth, withdirs=withdirs, detail=detail, **kwargs
            )
        if path:
            path = self._strip_protocol(path)
            r = sorted(k for k in self.references if k.startswith(path))
        else:
            r = sorted(self.references)
        if detail:
            if not self.dircache:
                self._dircache_from_items()
            return {k: self._ls_from_cache(k)[0] for k in r}
        else:
            return r

    def info(self, path, **kwargs):
        out = self.references.get(path)
        if out is not None:
            if isinstance(out, (str, bytes)):
                return {"name": path, "type": "file", "size": len(_inline_data(out))}
            elif _is_multi_segment(out):
                size = self._segments_size(path)
                if size is None:
                    raise ReferenceResolutionError(
                        f"Size of {path!r} is unknown: it contains a whole-file "
                        "segment with no bounded length"
                    )
                return {"name": path, "type": "file", "size": size}
            elif len(out) > 1:
                return {"name": path, "type": "file", "size": out[2]}
            else:
                out0 = [{"name": path, "type": "file", "size": None}]
        else:
            out = self.ls(path, True)
            out0 = [o for o in out if o["name"] == path]
            if not out0:
                return {"name": path, "type": "directory", "size": 0}
        if out0[0]["size"] is None:
            # if this is a whole remote file, update size using remote FS
            prot, _ = split_protocol(self.references[path][0])
            out0[0]["size"] = self.fss[prot].size(self.references[path][0])
        return out0[0]

    async def _info(self, path, **kwargs):
        out = self.references.get(path)
        if _is_multi_segment(out):
            size = self._segments_size(path)
            if size is None:
                raise ReferenceResolutionError(
                    f"Size of {path!r} is unknown: it contains a whole-file "
                    "segment with no bounded length"
                )
            return {"name": path, "type": "file", "size": size}
        if isinstance(out, (list, tuple)) and len(out) == 1:
            # whole remote file: the size must come from the remote FS, which
            # cannot be called synchronously from inside the event loop
            prot, _ = split_protocol(out[0])
            fs = self.fss[prot]
            if fs.async_impl:
                size = await fs._size(out[0])
            else:
                size = fs.size(out[0])
            return {"name": path, "type": "file", "size": size}
        return self.info(path)  # calls fast sync code

    async def _rm_file(self, path, **kwargs):
        self.references.pop(
            path, None
        )  # ignores FileNotFound, just as well for directories
        self.dircache.clear()  # this is a bit heavy handed

    async def _pipe_file(self, path, data, mode="overwrite", **kwargs):
        if mode == "create" and self.exists(path):
            raise FileExistsError
        # can be str or bytes
        self.references[path] = data
        self.dircache.clear()  # this is a bit heavy handed

    async def _put_file(self, lpath, rpath, mode="overwrite", **kwargs):
        # puts binary
        if mode == "create" and self.exists(rpath):
            raise FileExistsError
        with open(lpath, "rb") as f:
            self.references[rpath] = f.read()
        self.dircache.clear()  # this is a bit heavy handed

    def save_json(self, url, **storage_options):
        """Write modified references into new location"""
        out = {}
        for k, v in self.references.items():
            if isinstance(v, bytes):
                try:
                    out[k] = v.decode("ascii")
                except UnicodeDecodeError:
                    out[k] = (b"base64:" + base64.b64encode(v)).decode()
            elif _is_multi_segment(v):
                out[k] = [self._json_safe_segment(seg) for seg in v]
            else:
                out[k] = v
        with fsspec.open(url, "wb", **storage_options) as f:
            f.write(json.dumps({"version": 1, "refs": out}).encode())

    @staticmethod
    def _json_safe_segment(segment):
        """Make one segment JSON serializable, base64-encoding raw bytes."""
        if isinstance(segment, (list, tuple)):
            return list(segment)
        if isinstance(segment, dict) and isinstance(
            segment.get("inline"), (bytes, bytearray)
        ):
            return {
                **segment,
                "inline": (
                    b"base64:" + base64.b64encode(bytes(segment["inline"]))
                ).decode(),
            }
        return segment


class ReferenceFile(AbstractBufferedFile):
    def __init__(
        self,
        fs,
        path,
        mode="rb",
        block_size="default",
        autocommit=True,
        cache_type="readahead",
        cache_options=None,
        size=None,
        **kwargs,
    ):
        super().__init__(
            fs,
            path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            size=size,
            cache_type=cache_type,
            cache_options=cache_options,
            **kwargs,
        )
        part_or_url, self.start, self.end = self.fs._cat_common(self.path)
        protocol, _ = split_protocol(part_or_url)
        self.src_fs = self.fs.fss[protocol]
        self.src_path = part_or_url
        self._f = None

    @property
    def f(self):
        if self._f is None or self._f.closed:
            self._f = self.src_fs._open(
                self.src_path,
                mode=self.mode,
                block_size=self.blocksize,
                autocommit=self.autocommit,
                cache_type="none",
                **self.kwargs,
            )
        return self._f

    def close(self):
        if self._f is not None:
            self._f.close()
        return super().close()

    def _fetch_range(self, start, end):
        start = start + self.start
        end = min(end + self.start, self.end)
        self.f.seek(start)
        return self.f.read(end - start)


class MultiSegmentFile(io.RawIOBase):
    """Random-access view over an ordered-segment logical file.

    Positions are logical: ``seek``/``read`` operate on the concatenated
    segment coordinates, and each read is resolved to the segments it
    overlaps, fetching only the underlying ranges it needs.
    """

    def __init__(self, fs, path, mode="rb", block_size=None, **kwargs):
        if mode != "rb":
            raise NotImplementedError("Multi-segment references are read-only")
        self.fs = fs
        self.path = fs._strip_protocol(path)
        self._segments = fs._resolve_segments(self.path)
        self._size = _known_length(self._segments)
        if self._size is None:
            raise ReferenceResolutionError(
                f"Cannot open {self.path!r} for random access: it contains a "
                "whole-file segment with no bounded length"
            )
        self._loc = 0
        super().__init__()

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self._loc

    def seek(self, loc, whence=0):
        if whence == 0:
            nloc = loc
        elif whence == 1:
            nloc = self._loc + loc
        elif whence == 2:
            nloc = self._size + loc
        else:
            raise ValueError(f"invalid whence ({whence}, should be 0, 1 or 2)")
        if nloc < 0:
            raise ValueError("Seek before start of file")
        self._loc = nloc
        return self._loc

    def read(self, size=-1):
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if size is None or size < 0:
            size = self._size - self._loc
        end = min(self._loc + size, self._size)
        if end <= self._loc:
            return b""
        data = self.fs._cat_multi(
            self.path,
            self._loc,
            end,
            self.fs._fetch_ranges,
            segments=self._segments,
        )
        self._loc += len(data)
        return data

    def readinto(self, b):
        out = memoryview(b).cast("B")
        data = self.read(out.nbytes)
        out[: len(data)] = data
        return len(data)
