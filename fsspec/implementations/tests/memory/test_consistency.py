"""Consistent-read tests on a mutable, versioned memory-backed test double.

The double keeps its bytes in the in-memory store but routes read opens
through :class:`~fsspec.spec.AbstractBufferedFile`, so every read exercises
the same block caching and range-fetch machinery as a real remote backend.
"""

import io
import pickle

import pytest

from fsspec.exceptions import ConsistencyNotSupportedError, ObjectChangedError
from fsspec.implementations.memory import MemoryFileSystem
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem

# 64 bytes: several blocks at the small block sizes used below, and no two
# blocks share content, so a stitched read is easy to recognise.
DATA_A = bytes(range(64))
# Same length as DATA_A, entirely different bytes.
DATA_B = bytes(reversed(range(64)))


class VersionedMemoryFS(MemoryFileSystem):
    """Memory store with remote-style buffered reads and versioned objects."""

    protocol = "versioned-memory"
    cachable = False

    def __init__(self, **kwargs):
        super().__init__(global_store=False, skip_instance_cache=True, **kwargs)
        self.versions = {}

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        path = self._strip_protocol(path)
        if "r" in mode:
            return AbstractBufferedFile(
                self,
                path,
                mode,
                block_size,
                autocommit,
                cache_options=cache_options,
                **kwargs,
            )
        return super()._open(
            path,
            mode,
            block_size,
            autocommit,
            cache_options=cache_options,
            **kwargs,
        )

    def version(self, path):
        path = self._strip_protocol(path)
        return ("version_id", self.versions[path])

    def select_version(self, path, version):
        """Serve the given bytes under a version; used to build the fixture."""
        path = self._strip_protocol(path)
        super().pipe_file(path, version)
        self.versions[path] = self.versions.get(path, 0) + 1
        return ("version_id", self.versions[path])

    def replace(self, path, data):
        """Replace content in place, keeping the same path and length."""
        path = self._strip_protocol(path)
        f = self.store[path]
        f.seek(0)
        f.write(data)
        f.truncate()
        self.versions[path] += 1

    def __reduce__(self):
        factory, args, state = super().__reduce__()
        return factory, args, {**state, "versions": dict(self.versions)}


@pytest.fixture
def vfs():
    fs = VersionedMemoryFS()
    fs.select_version("/file", DATA_A)
    yield fs
    fs.store.clear()
    fs.pseudo_dirs.clear()
    fs.pseudo_dirs.append("")


@pytest.mark.parametrize(
    "cache_type",
    [None, "none", "readahead", "bytes", "blockcache", "first", "background", "mmap"],
)
def test_default_read_mode_has_no_consistency_enforcement(vfs, cache_type):
    """The legacy default keeps reading through an overwrite without error."""
    with vfs.open("/file", "rb", block_size=8, cache_type=cache_type) as f:
        first = f.read(8)
        vfs.select_version("/file", DATA_B)
        f.seek(24)
        later = f.read(8)
    assert first == DATA_A[:8]
    assert later == DATA_B[24:32]


@pytest.mark.parametrize(
    "cache_type", [None, "none", "readahead", "bytes", "blockcache", "background"]
)
def test_consistent_read_never_mixes_versions(vfs, cache_type):
    """Replacing the object between two block fetches fails the read.

    The returned bytes can never be a third value made of blocks from both
    versions: either the snapshot is returned intact, or the read raises.
    """
    vfs.replace("/file", DATA_A)
    with vfs.open(
        "/file", "rb", block_size=8, cache_type=cache_type, consistency=True
    ) as f:
        assert f.read(8) == DATA_A[:8]
        vfs.select_version("/file", DATA_B)
        f.seek(32)
        with pytest.raises(ObjectChangedError):
            f.read(8)


@pytest.mark.parametrize(
    "cache_type", [None, "none", "readahead", "bytes", "blockcache", "background"]
)
def test_never_returns_third_content(vfs, cache_type):
    """Same-length replacement between block fetches never yields a mixture.

    Sweeps every block boundary: after each fetch the object is replaced by
    same-length content. Every read must return bytes from one single version
    or raise; a stitched third value is impossible.
    """
    block = 8
    for boundary in range(block, len(DATA_A), block):
        vfs.select_version("/file", DATA_A)
        try:
            with vfs.open(
                "/file", "rb", block_size=block, cache_type=cache_type, consistency=True
            ) as f:
                pieces = []
                while True:
                    chunk = f.read(block)
                    if not chunk:
                        break
                    pieces.append(chunk)
                    # Overwrite between fetches with same-length new content.
                    vfs.select_version("/file", DATA_B)
        except ObjectChangedError:
            continue  # a change was detected: the guarantee held
        data = b"".join(pieces)
        assert data in (DATA_A, DATA_B), (
            f"boundary {boundary} produced stitched content: {data!r}"
        )


def test_cache_hits_stay_on_snapshot(vfs):
    """Cached blocks keep returning the bound version without raising."""
    vfs.replace("/file", DATA_A)
    f = vfs.open("/file", "rb", block_size=8, consistency=True)
    assert f.read(8) == DATA_A[:8]
    vfs.select_version("/file", DATA_B)
    # Still inside the block fetched before the overwrite: a cache hit, so the
    # snapshot bytes are returned unchanged.
    f.seek(0)
    assert f.read(8) == DATA_A[:8]
    # close() notices the remote moved on and fails instead of pretending the
    # whole handle was consistent.
    with pytest.raises(ObjectChangedError):
        f.close()


def test_seek_back_and_forth_stays_consistent(vfs):
    """Seeks across cached and uncached blocks never cross versions."""
    vfs.replace("/file", DATA_A)
    with vfs.open("/file", "rb", block_size=8, consistency=True) as f:
        assert f.read(16) == DATA_A[:16]
        assert f.seek(40) == 40
        assert f.seek(8) == 8
        assert f.read(16) == DATA_A[8:24]
        assert f.seek(-8, 2) == len(DATA_A) - 8
        assert f.read(8) == DATA_A[-8:]


def test_short_read_past_eof_is_not_an_error(vfs):
    """Reads running past the bound end of file are legitimately short."""
    vfs.replace("/file", DATA_A)
    with vfs.open("/file", "rb", block_size=8, consistency=True) as f:
        assert f.seek(60) == 60
        assert f.read(100) == DATA_A[60:]
        assert f.read() == b""


def test_short_read_inside_bound_size_fails(vfs):
    """A backend returning too few bytes within the bound size is caught."""
    vfs.replace("/file", DATA_A)
    original = vfs._fetch_consistent

    def truncated(path, start, end, version):
        if vfs.version(path) != version:
            raise ObjectChangedError("changed")
        return b"" if end - start > 4 else b"x" * (end - start)

    vfs._fetch_consistent = truncated
    try:
        with vfs.open("/file", "rb", block_size=8, consistency=True) as f:
            with pytest.raises(ObjectChangedError, match="Short read"):
                f.read(8)
    finally:
        vfs._fetch_consistent = original


def test_change_detected_at_close(vfs):
    """A change visible only through cached reads still fails at close."""
    vfs.replace("/file", DATA_A)
    f = vfs.open("/file", "rb", block_size=len(DATA_A), consistency=True)
    assert f.read(8) == DATA_A[:8]
    vfs.select_version("/file", DATA_B)
    with pytest.raises(ObjectChangedError, match="closed"):
        f.close()
    assert f.closed


def test_unchanged_close_succeeds(vfs):
    vfs.replace("/file", DATA_A)
    with vfs.open("/file", "rb", size=len(DATA_A), consistency=True) as f:
        assert f.read() == DATA_A
    # Context-manager exit did not raise for an unchanged object.
    assert f.closed


def test_first_read_after_change_fails(vfs):
    """The first fetch after the overwrite fails, before any bytes return."""
    vfs.replace("/file", DATA_A)
    with vfs.open(
        "/file", "rb", block_size=8, cache_type="none", consistency=True
    ) as f:
        assert f.read(8) == DATA_A[:8]
        vfs.select_version("/file", DATA_B)
        with pytest.raises(ObjectChangedError):
            f.read(8)


def test_truncation_detected_at_seek_position(vfs):
    """A shorter replacement is caught at the offset that used to exist."""
    vfs.replace("/file", DATA_A)
    with vfs.open("/file", "rb", block_size=8, consistency=True) as f:
        assert f.seek(48) == 48
        vfs.select_version("/file", b"tiny")
        with pytest.raises(ObjectChangedError):
            f.read(8)


class NoVersionMemoryFS(MemoryFileSystem):
    """A backend whose info only exposes size and timestamps."""

    protocol = "no-version-memory"
    cachable = False

    def version(self, path):
        # Deliberately use the base implementation, which must not trust
        # size/mtime/created alone.
        return AbstractFileSystem.version(self, path)


def test_consistent_read_reports_missing_capability():
    """Size and mtime alone are refused with an explicit capability error."""
    fs = NoVersionMemoryFS(global_store=False, skip_instance_cache=True)
    fs.pipe_file("/plain", DATA_A)
    with pytest.raises(
        ConsistencyNotSupportedError, match="Size and modification time"
    ):
        fs.version("/plain")
    # Opening a consistent read surfaces the same error, not silent mixing.
    with pytest.raises(ConsistencyNotSupportedError):
        AbstractBufferedFile(fs, "/plain", consistency=True)


@pytest.mark.parametrize("consistency", [True, "snapshot"])
def test_consistency_aliases(vfs, consistency):
    vfs.replace("/file", DATA_A)
    with vfs.open("/file", "rb", consistency=consistency, block_size=8) as f:
        assert f.consistency is True
        assert f.read(8) == DATA_A[:8]


@pytest.mark.parametrize("consistency", [None, False])
def test_default_consistency_off(vfs, consistency):
    vfs.replace("/file", DATA_A)
    with vfs.open("/file", "rb", consistency=consistency, block_size=8) as f:
        assert f.consistency is False
        assert f.read(8) == DATA_A[:8]


def test_invalid_consistency_value(vfs):
    with pytest.raises(ValueError, match="consistency must be"):
        vfs.open("/file", "rb", consistency="eventual")


def test_write_mode_ignores_consistency(vfs):
    with vfs.open("/file2", "wb", consistency=True) as f:
        f.write(b"data")
    with vfs.open("/file2", "rb", block_size=8, cache_type="none") as f:
        assert f.read() == b"data"


def test_consistent_read_readline_and_readinto(vfs):
    lines = b"line one\nline two\nline three\n"
    vfs.select_version("/lines", lines)
    with vfs.open(
        "/lines", "rb", block_size=8, cache_type="none", consistency=True
    ) as f:
        buffer = bytearray(4)
        assert f.readinto(buffer) == 4
        assert bytes(buffer) == lines[:4]
        assert f.seek(0) == 0
        assert f.readline() == b"line one\n"
        assert f.readuntil(b"\n") == b"line two\n"
        assert f.readline() == b"line three\n"
        assert f.readline() == b""


def test_text_wrapper_keeps_consistency(vfs):
    vfs.replace("/file", DATA_A)
    with vfs.open(
        "/file",
        "rt",
        consistency=True,
        block_size=8,
        encoding="latin-1",
        newline="",
    ) as f:
        assert isinstance(f, io.TextIOWrapper)
        assert f.read() == DATA_A.decode("latin-1")


def test_pickle_preserves_consistency(vfs):
    vfs.replace("/file", DATA_A)
    f = vfs.open("/file", "rb", block_size=8, consistency=True)
    assert f.read(8) == DATA_A[:8]
    f2 = pickle.loads(pickle.dumps(f))
    assert f2.consistency is True
    assert f2.read(8) == DATA_A[8:16]
    for handle in (f, f2):
        handle.close()


def test_buffered_file_requires_version_hook(tmp_path):
    """The generic buffered file rejects backends without an identity."""
    from fsspec.implementations.local import LocalFileSystem

    target = tmp_path / "data.bin"
    target.write_bytes(DATA_A)
    fs = LocalFileSystem()
    # Local files only have size/mtime/ino: not a verifiable version.
    with pytest.raises(ConsistencyNotSupportedError):
        AbstractBufferedFile(fs, str(target), consistency=True)


def test_failed_close_still_releases_cache(vfs):
    """The cache is closed even when the final consistency check raises."""
    vfs.replace("/file", DATA_A)
    f = vfs.open(
        "/file", "rb", block_size=8, cache_type="background", consistency=True
    )
    f.read(8)
    cache = f.cache
    executor = cache._thread_executor
    vfs.select_version("/file", DATA_B)
    with pytest.raises(ObjectChangedError):
        f.close()
    assert f.closed
    assert f.cache is None
    with pytest.raises(RuntimeError, match="cannot schedule new futures"):
        executor.submit(lambda: None)


def test_garbage_collection_never_unraisable(vfs):
    """An unclosed, invalidated handle is collected without warnings."""
    import gc
    import warnings

    vfs.replace("/file", DATA_A)
    f = vfs.open("/file", "rb", block_size=8, consistency=True)
    f.read(8)
    vfs.select_version("/file", DATA_B)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        del f
        gc.collect()


def test_memory_versions_are_monotonic_across_overwrites():
    """Same-path overwrites (pipe and copy) never reuse an old version token."""
    fs = MemoryFileSystem(global_store=False, skip_instance_cache=True)
    fs.pipe_file("/a", DATA_A)
    fs.pipe_file("/b", DATA_B)
    first = fs.version("/b")
    # same-length overwrite via copy must not reuse the old token
    fs.cp_file("/a", "/b")
    copied = fs.version("/b")
    fs.pipe_file("/b", DATA_A)
    after_pipe = fs.version("/b")
    assert len({first, copied, after_pipe}) == 3

