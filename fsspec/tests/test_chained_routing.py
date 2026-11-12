"""Routing tests for chained URLs (``::``) and ``open_files``.

For every hop of a chain these tests answer a narrow question: *which*
filesystem instance receives *which* path, *which* options, and *which*
target.  They do not judge correctness by the bytes that happen to be
readable at the end of the chain - a successful read cannot tell you whether
an option was routed to the wrong layer.

Routing is made observable with recording stand-in backends: each
constructor appends its exact ``__init__`` kwargs to a shared log, and
``_open``/``glob``/``isdir`` record the path they were handed.  Assertions
inspect that log and the identity of the constructed instances, never the
file contents.
"""

import io
import zipfile

import pytest

import fsspec
from fsspec import AbstractFileSystem
from fsspec.core import _un_chain, get_fs_token_paths, open_files, url_to_fs
from fsspec.implementations.chained import ChainedFileSystem
from fsspec.implementations.memory import MemoryFileSystem
from fsspec.registry import register_implementation

# Shared call log: ("init", instance, kwargs) or ("open"/"glob"/..., instance, path)
ROUTING_LOG = []


class RecordingFS(AbstractFileSystem):
    """Base class for backends that only record construction and calls.

    Subclasses set ``protocol``; every instance records the kwargs passed by
    the resolver, and - when given ``target_protocol``/``fs`` - builds the
    nested target like a real chained filesystem would.
    """

    cachable = True
    root_marker = ""
    # path -> bytes served by _open in read mode
    files: dict = {}

    def __init__(
        self,
        target_protocol=None,
        target_options=None,
        fo=None,
        fs=None,
        **kwargs,
    ):
        ROUTING_LOG.append(
            (
                "init",
                self,
                {
                    "target_protocol": target_protocol,
                    "target_options": target_options,
                    "fo": fo,
                    "fs": fs,
                    **kwargs,
                },
            )
        )
        super().__init__()
        self.fo = fo
        if fs is not None:
            self.target = fs
        elif target_protocol is not None:
            self.target = fsspec.filesystem(target_protocol, **(target_options or {}))
        else:
            self.target = None

    def _open(self, path, mode="rb", block_size=None, **kwargs):
        ROUTING_LOG.append(("open", self, path))
        if "w" in mode:
            return io.BytesIO()
        return io.BytesIO(self.files[path])

    def glob(self, path, **kwargs):
        ROUTING_LOG.append(("glob", self, path))
        if path == "data/*.csv":
            return ["data/1.csv", "data/2.csv"]
        return []

    def isdir(self, path):
        ROUTING_LOG.append(("isdir", self, path))
        return False

    def info(self, path, **kwargs):
        ROUTING_LOG.append(("info", self, path))
        return {"name": path, "type": "file", "size": 0}

    @classmethod
    def _strip_protocol(cls, path):
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        prefix = cls.protocol + "://"
        return path.removeprefix(prefix).rstrip("/")


class RecordingInner(RecordingFS):
    protocol = "recin"


class RecordingMid(RecordingFS):
    protocol = "recmid"


class RecordingOuter(RecordingFS):
    protocol = "recout"


class RecordingChained(ChainedFileSystem, RecordingFS):
    """Chained stand-in that delegates ``_open`` to its target via ``fo``."""

    protocol = "recchain"

    def _open(self, path, mode="rb", block_size=None, **kwargs):
        ROUTING_LOG.append(("open", self, path))
        return self.target._open(self.fo, mode=mode, block_size=block_size, **kwargs)


class UrlOptionFS(RecordingFS):
    """Backend that extracts instance options out of the URL itself."""

    protocol = "recurl"

    @staticmethod
    def _get_kwargs_from_urls(path):
        return {"url_option": "from-url", "shared": "from-url"}


RECORDING_CLASSES = (RecordingInner, RecordingMid, RecordingOuter, RecordingChained)


@pytest.fixture
def routing():
    """Register the stand-ins with a clean instance cache and empty log."""
    register_implementation("recin", RecordingInner, clobber=True)
    register_implementation("recmid", RecordingMid, clobber=True)
    register_implementation("recout", RecordingOuter, clobber=True)
    register_implementation("recchain", RecordingChained, clobber=True)
    register_implementation("recurl", UrlOptionFS, clobber=True)
    for cls in (*RECORDING_CLASSES, UrlOptionFS):
        cls.clear_instance_cache()
    RecordingFS.files = {}
    ROUTING_LOG.clear()
    yield ROUTING_LOG
    for cls in (*RECORDING_CLASSES, UrlOptionFS):
        cls.clear_instance_cache()


def inits(log):
    """All constructor events as ``(protocol, kwargs)`` pairs."""
    return [(fs.protocol, kw) for event, fs, kw in log if event == "init"]


def calls(log, kind):
    """All method-call events as ``(protocol, path)`` pairs."""
    return [(fs.protocol, path) for event, fs, path in log if event == kind]


def call_instances(log, kind):
    """The filesystem instances that received method calls of ``kind``."""
    return [fs for event, fs, _ in log if event == kind]


def test_un_chain_maps_each_layer_to_path_protocol_and_scoped_options(routing):
    # The chain is ordered outer -> inner; each tuple carries the layer's
    # stripped path, its protocol, and only its own scoped options.
    chain = _un_chain(
        "recout://outer/name::recmid://mid/name::recin://inner/file",
        {"recout": {"outer_opt": 1}, "recin": {"inner_opt": 9}},
    )
    assert chain == [
        ("outer/name", "recout", {"outer_opt": 1}),
        ("mid/name", "recmid", {}),
        ("inner/file", "recin", {"inner_opt": 9}),
    ]


def test_url_to_fs_nests_target_options_and_fo_layer_by_layer(routing):
    url = "recout://outer/name::recmid://mid/name::recin://inner/file"
    fs, path = url_to_fs(url, recout={"outer_opt": 1}, recin={"inner_opt": 9})

    # The outermost layer is the returned filesystem and owns the outer path.
    assert isinstance(fs, RecordingOuter)
    assert path == "outer/name"

    assert inits(routing) == [
        (
            "recout",
            {
                "target_protocol": "recmid",
                "target_options": {
                    "target_protocol": "recin",
                    "target_options": {"inner_opt": 9},
                    "fo": "inner/file",
                },
                "fo": "mid/name",
                "fs": None,
                "outer_opt": 1,
            },
        ),
        (
            "recmid",
            {
                "target_protocol": "recin",
                "target_options": {"inner_opt": 9},
                "fo": "inner/file",
                "fs": None,
            },
        ),
        (
            "recin",
            {
                "target_protocol": None,
                "target_options": None,
                "fo": None,
                "fs": None,
                "inner_opt": 9,
            },
        ),
    ]

    # The instances really are nested, each pointing at the layer below.
    assert isinstance(fs.target, RecordingMid)
    assert isinstance(fs.target.target, RecordingInner)
    assert fs.target.target.storage_options == {"inner_opt": 9}


def test_bare_protocol_bit_borrows_the_inner_path_as_its_fo(routing):
    # A chained layer written without a path (``simplecache``) is handed the
    # next layer's full path as its own ``fo`` instead of an empty string.
    chain = _un_chain("simplecache::memory://data/file", {})
    assert chain == [
        ("/data/file", "simplecache", {}),
        ("/data/file", "memory", {}),
    ]

    # The same bare word without a ``::`` is just a local relative path.
    local_chain = _un_chain("simplecache", {})
    assert local_chain[0][1] == "file"
    assert local_chain[0][0].endswith("simplecache")


def test_repeated_protocol_layer_keeps_separate_instances_and_routes_options(
    routing,
):
    # Two ``recmid`` hops: the scoped dict is popped once, so it lands on the
    # innermost recmid hop; the outer hop only sees it nested inside
    # target_options.  Both hops still produce distinct instances.
    url = "recmid://a::recmid://b::recin://c"
    chain = _un_chain(url, {"recmid": {"mid_opt": 1}})
    assert [(p, proto, kw) for p, proto, kw in chain] == [
        ("a", "recmid", {}),
        ("b", "recmid", {"mid_opt": 1}),
        ("c", "recin", {}),
    ]

    fs, path = url_to_fs(url, recmid={"mid_opt": 1})
    assert path == "a"
    assert isinstance(fs, RecordingMid) and isinstance(fs.target, RecordingMid)
    assert fs is not fs.target

    init_kws = [kw for _, kw in inits(routing)]
    outer_kw, inner_kw = init_kws[0], init_kws[1]
    assert outer_kw["fo"] == "b"
    # The outer hop has no top-level ``mid_opt``; the option only survives
    # inside the nested target_options handed to the layer below.
    assert "mid_opt" not in outer_kw
    assert outer_kw["target_options"] == {
        "mid_opt": 1,
        "target_protocol": "recin",
        "target_options": {},
        "fo": "c",
    }
    assert inner_kw["fo"] == "c"
    assert inner_kw["mid_opt"] == 1


def test_protocol_override_is_innermost_only_and_keeps_foreign_prefix(routing):
    # A ``protocol=`` storage option is consumed by the *innermost* layer
    # (the chain is reversed when processed).  For a single-layer URL the
    # overriding class strips only its own prefix, so the original
    # ``recin://`` text survives inside the path - which is why such
    # overrides must be used deliberately.
    single = _un_chain("recin://some/path", {"protocol": "recmid"})
    assert single == [("recin://some/path", "recmid", {})]

    chained = _un_chain("recmid://mid::recin://c", {"protocol": "recout"})
    assert chained == [
        ("mid", "recmid", {}),
        ("recin://c", "recout", {}),
    ]

    fs, path = url_to_fs("recin://some/path", protocol="recmid")
    assert isinstance(fs, RecordingMid)
    assert path == "recin://some/path"
    # The override keyword is popped and never reaches a constructor.
    assert all("protocol" not in kw for _, kw in inits(routing))


def test_url_encoded_option_and_caller_option_with_same_value_deduplicate(routing):
    chain = _un_chain("recurl://x", {"recurl": {"shared": "from-url"}})
    assert chain[0][2] == {
        "url_option": "from-url",
        "shared": "from-url",
    }


def test_conflicting_url_encoded_and_caller_option_raises_type_error(routing):
    # Established compatibility rule (also codified upstream via adlfs): when
    # the same option is encoded in the URL and passed explicitly with a
    # different value, the resolver raises rather than silently picking one.
    with pytest.raises(TypeError, match="multiple values"):
        _un_chain("recurl://x", {"recurl": {"shared": "from-caller"}})

    with pytest.raises(TypeError, match="multiple values"):
        _un_chain("recurl://x", {"shared": "from-caller"})


@pytest.fixture
def zip_in_memory():
    """A real ZipFileSystem over an in-memory archive with tricky members."""
    members = [
        "data/a 1.csv",
        "data/b#[2].txt",
        "ünï/файл(1).bin",
        "plain.txt",
    ]
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        for member in members:
            archive.writestr(member, b"payload")
    raw = buf.getvalue()

    def fresh_store():
        # The archive layer builds its own cached ``memory`` instance, which
        # only sees the shared class-level store; a stored MemoryFile cannot
        # be reopened once closed, so rebuild the BytesIO before each open.
        MemoryFileSystem.clear_instance_cache()
        memory = MemoryFileSystem()
        memory.store.clear()
        memory.pseudo_dirs = [""]
        memory.store["/arch.zip"] = io.BytesIO(raw)
        return memory

    yield members, fresh_store
    memory = MemoryFileSystem()
    memory.store.clear()
    MemoryFileSystem.clear_instance_cache()


def test_archive_member_special_characters_survive_un_chaining(routing, zip_in_memory):
    members, fresh_store = zip_in_memory
    tricky = [
        "data/a 1.csv",  # space
        "data/b#[2].txt",  # hash and glob-like brackets
        "ünï/файл(1).bin",  # unicode and parentheses
    ]
    for member in tricky:
        assert member in members
        url = f"zip://{member}::memory://arch.zip"
        chain = _un_chain(url, {})
        # The member name is delivered verbatim to the archive layer; the
        # archive location, including its leading slash, goes to memory.
        assert chain == [
            (member, "zip", {}),
            ("/arch.zip", "memory", {}),
        ]

        fresh_store()
        fs, path = url_to_fs(url)
        assert path == member
        assert type(fs).__name__ == "ZipFileSystem"
        # ``of`` is the OpenFile descriptor (``fo`` is the entered file
        # object), so the target path lives on ``of.path``.
        assert fs.of.path == "/arch.zip"
        # The archive target was resolved on a memory instance via its
        # OpenFile; the check uses instance identity rather than bytes.
        assert isinstance(fs.of.fs, MemoryFileSystem)
        # Routing check via the member name the archive layer resolves,
        # without reading any bytes.
        assert fs.info(member)["name"] == member


def test_archive_glob_is_routed_to_the_archive_layer(routing, zip_in_memory):
    members, fresh_store = zip_in_memory
    url = "zip://data/*.csv::memory://arch.zip"
    assert _un_chain(url, {}) == [
        ("data/*.csv", "zip", {}),
        ("/arch.zip", "memory", {}),
    ]

    fresh_store()
    files = open_files(url)
    assert type(files.fs).__name__ == "ZipFileSystem"
    # The pattern was expanded on the archive layer into exact member names,
    # keeping the space character intact.
    assert [f.path for f in files] == ["data/a 1.csv"]


def test_archive_member_containing_chain_separator_is_the_minimal_counterexample(
    routing,
):
    # ``::`` is the chain separator and splitting happens before any backend
    # interprets the path, so an archive member whose name contains ``::``
    # cannot be addressed in a chained URL.  The middle fragment is parsed as
    # its own layer instead of remaining part of the member name.
    chain = _un_chain("zip://dir/with::inside.txt::memory://arch.zip", {})
    assert chain[0] == ("dir/with", "zip", {})
    # The middle fragment is mistaken for a relative local path and turned
    # into an absolute path by LocalFileSystem._strip_protocol.
    assert chain[1][1] == "file"
    assert chain[1][0].endswith("inside.txt")
    assert chain[2] == ("/arch.zip", "memory", {})


def test_archive_member_with_chain_separator_reachable_via_explicit_fs(
    routing,
):
    # The workaround for the limitation above: build the archive filesystem
    # directly, so the member name never passes through the chain parser.
    buf = io.BytesIO()
    member = "dir/with::inside.txt"
    with zipfile.ZipFile(buf, "w") as archive:
        archive.writestr(member, b"payload")
    MemoryFileSystem.clear_instance_cache()
    memory = MemoryFileSystem()
    memory.store.clear()
    memory.pseudo_dirs = [""]
    memory.store["/arch.zip"] = io.BytesIO(buf.getvalue())

    archive_fs = fsspec.filesystem("zip", fo="/arch.zip", target_protocol="memory")
    assert archive_fs.ls("dir", detail=False) == [member]
    assert archive_fs.info(member)["name"] == member
    memory.store.clear()
    MemoryFileSystem.clear_instance_cache()


def test_glob_expansion_happens_on_the_outermost_filesystem(routing):
    # ``open_files`` asks the outermost filesystem (the one it actually
    # returns) to expand the pattern and filter directories itself.
    files = open_files("recout://data/*.csv")
    assert [f.path for f in files] == ["data/1.csv", "data/2.csv"]
    assert call_instances(routing, "glob") == [files.fs]
    assert calls(routing, "glob") == [("recout", "data/*.csv")]
    assert calls(routing, "isdir") == [
        ("recout", "data/1.csv"),
        ("recout", "data/2.csv"),
    ]


def test_glob_through_a_chain_does_not_reach_the_inner_layer(routing):
    # With a wrapper on top, glob/isdir still target the wrapper instance;
    # the inner layer only receives its construction options, not the glob.
    fs, _, paths = get_fs_token_paths(
        "recout://data/*.csv::recin://root",
        storage_options={},
    )
    assert paths == ["data/1.csv", "data/2.csv"]
    assert isinstance(fs, RecordingOuter)
    assert call_instances(routing, "glob") == [fs]
    assert all(
        isinstance(instance, RecordingOuter)
        for instance in call_instances(routing, "isdir")
    )
    assert not any(
        isinstance(instance, RecordingInner)
        for instance in call_instances(routing, "glob")
    )
    outer_init = inits(routing)[0][1]
    assert outer_init["fo"] == "root"
    assert outer_init["target_protocol"] == "recin"


def test_list_of_chained_urls_must_share_the_outermost_protocol(routing):
    # Protocol-mismatch detection compares the first tuple of each element's
    # chain, i.e. the outer protocol, not the inner one.
    with pytest.raises(ValueError, match="Protocol mismatch"):
        get_fs_token_paths(
            ["recout://a::recin://x", "recmid://b::recin://y"],
            storage_options={},
        )

    fs, _, paths = get_fs_token_paths(
        ["recout://a::recin://x", "recout://b::recin://y"],
        storage_options={},
    )
    assert isinstance(fs, RecordingOuter)
    # Each element's outer path is extracted independently.
    assert paths == ["a", "b"]


def test_explicit_filesystem_instance_replaces_the_inner_layer(routing):
    # ``fs=`` supplies the target directly: the outer layer receives it as a
    # constructor argument, and no inner instance is constructed at all.
    supplied = RecordingInner()
    routing.clear()

    fs, path = url_to_fs("recout://outer/name", fs=supplied)
    assert path == "outer/name"
    assert isinstance(fs, RecordingOuter)
    assert fs.target is supplied
    (init_kw,) = [kw for _, kw in inits(routing)]
    assert init_kw["fs"] is supplied
    assert init_kw["target_protocol"] is None
    assert init_kw["target_options"] is None


def test_explicit_filesystem_instance_through_open_files(routing):
    supplied = RecordingInner()
    routing.clear()

    files = open_files("recout://outer/name", fs=supplied)
    assert isinstance(files.fs, RecordingOuter)
    assert files.fs.target is supplied
    assert [f.path for f in files] == ["outer/name"]


def test_explicit_filesystem_cannot_coexist_with_an_inner_url_layer(routing):
    # A chained URL always derives ``target_protocol`` for the outer layer,
    # so a scoped ``fs`` on the same layer conflicts and the chained
    # filesystem raises instead of silently dropping one of them.
    supplied = RecordingInner()
    routing.clear()

    with pytest.raises(ValueError, match="may not be both given"):
        url_to_fs(
            "simplecache::recin://inner/name",
            simplecache={"fs": supplied, "cache_storage": "/tmp/fs-test"},
        )


def test_archive_layer_ignores_explicit_filesystem_option(routing, zip_in_memory):
    # ``fs`` is only meaningful to ChainedFileSystem subclasses; an archive
    # layer stores it as an ordinary option and still resolves its target
    # from ``target_protocol``/``fo``.
    members, fresh_store = zip_in_memory
    memory = fresh_store()
    supplied = RecordingInner()

    fs, member = url_to_fs("zip://plain.txt::memory://arch.zip", zip={"fs": supplied})
    assert member == "plain.txt"
    assert fs.storage_options["fs"] is supplied
    # The archive target is a memory instance from the chain, not ``supplied``.
    assert isinstance(fs.of.fs, MemoryFileSystem)
    assert fs.of.fs is not supplied
    assert fs.of.fs is memory


def test_same_url_and_options_reuse_the_whole_cached_chain(routing):
    url = "recout://o::recmid://m::recin://i/file"
    fs1, _ = url_to_fs(url, recout={"outer_opt": 1}, recin={"inner_opt": 7})
    fs2, _ = url_to_fs(url, recout={"outer_opt": 1}, recin={"inner_opt": 7})
    assert fs1 is fs2
    assert fs1.target is fs2.target
    assert fs1.target.target is fs2.target.target


def test_outer_option_change_only_rebuilds_the_outer_layers(routing):
    url = "recout://o::recmid://m::recin://i/file"
    base = {"recin": {"inner_opt": 7}}
    fs1, _ = url_to_fs(url, recout={"outer_opt": 1}, **base)
    fs2, _ = url_to_fs(url, recout={"outer_opt": 2}, **base)

    # Different outer options give a different outer instance ...
    assert fs1 is not fs2
    # ... but the inner layers are keyed by their own options, which did not
    # change, so they are the same cached instances.
    assert fs1.target is fs2.target
    assert fs1.target.target is fs2.target.target


def test_inner_option_change_rebuilds_the_inner_instance(routing):
    url = "recout://o::recmid://m::recin://i/file"
    fs1, _ = url_to_fs(url, recin={"inner_opt": 7})
    fs2, _ = url_to_fs(url, recin={"inner_opt": 8})

    assert fs1 is not fs2
    assert fs1.target.target is not fs2.target.target
    # The new inner instance really carries the new option.
    assert fs2.target.target.storage_options == {"inner_opt": 8}


def test_open_file_route_records_the_path_seen_by_every_layer(routing):
    RecordingFS.files = {"inner/file": b"payload"}
    files = open_files(
        "recchain://outer/name::recin://inner/file",
        recin={"inner_opt": 3},
    )
    assert len(files) == 1
    open_file = files[0]
    assert isinstance(open_file.fs, RecordingChained)
    assert open_file.path == "outer/name"

    with open_file as handle:
        assert handle.read() == b"payload"

    # The outer layer opens its own path; the chained layer then delegates to
    # the inner layer using the inner ``fo`` path, not the outer path.
    assert calls(routing, "open") == [
        ("recchain", "outer/name"),
        ("recin", "inner/file"),
    ]
    (inner_init,) = [kw for proto, kw in inits(routing) if proto == "recin"]
    assert inner_init["inner_opt"] == 3
