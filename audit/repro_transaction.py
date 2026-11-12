#!/usr/bin/env python3
"""fsspec Transaction audit: LocalFileSystem vs MemoryFileSystem.

Every claim below is checked by an observable probe. Faults are injected
explicitly (selective exceptions / monkeypatch), never inferred.

Run:  python audit/repro_transaction.py
Exit: 0 if every probe matches the documented capability, 1 otherwise.
"""
import contextlib
import glob
import os
import shutil
import sys
import tempfile

import fsspec
from fsspec.implementations.local import LocalFileSystem, LocalFileOpener
from fsspec.implementations.memory import MemoryFileSystem, MemoryFile

RESULTS = []


def check(label, cond, detail=""):
    RESULTS.append((label, bool(cond), detail))
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}" + (f"  -- {detail}" if detail else ""))


def section(title):
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def fresh_local(**kw):
    return LocalFileSystem(skip_instance_cache=True, **kw)


def fresh_memory():
    return MemoryFileSystem(global_store=False, skip_instance_cache=True)


@contextlib.contextmanager
def private_tmpdir():
    """Redirect tempfile.mkstemp() into a private dir so we can count temp files."""
    old = tempfile.tempdir
    d = tempfile.mkdtemp(prefix="fsspec-audit-")
    tempfile.tempdir = d
    try:
        yield d
    finally:
        tempfile.tempdir = old
        shutil.rmtree(d, ignore_errors=True)


def fresh_dir(root, name):
    d = os.path.join(root, name)
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d, exist_ok=True)
    return d


def local_scenarios(root):
    fs = fresh_local()
    p = os.path.join(root, "p")
    q = os.path.join(root, "q")
    fs.pipe_file(p, b"OLD-P")

    section("L1  local: writes invisible until commit (overwrite + create)")
    with private_tmpdir() as td:
        before = set(glob.glob(os.path.join(td, "tmp*")))
        with fs.transaction:
            with fs.open(p, "wb") as f:
                f.write(b"NEW-P")
            with fs.open(q, "wb") as f:
                f.write(b"NEW-Q")
            check("overwrite: old object stays visible", fs.cat(p) == b"OLD-P", repr(fs.cat(p)))
            check("new object does not appear", not fs.exists(q))
            check("no temp file in target dir",
                  not os.path.exists(q) and not glob.glob(os.path.join(root, "tmp*")))
            ntmp = len(set(glob.glob(os.path.join(td, "tmp*"))) - before)
            check("payload lives in system temp file(s)", ntmp == 2, f"{ntmp} temp files")
        check("new object appears after commit", fs.cat(q) == b"NEW-Q")
        check("overwrite lands after commit", fs.cat(p) == b"NEW-P")

    section("L2  local: already-open reader keeps old inode across rename")
    fs.pipe_file(p, b"OLD-P")
    reader = fs.open(p, "rb")
    check("reader sees old content before commit", reader.read() == b"OLD-P")
    with fs.transaction, fs.open(p, "wb") as f:
        f.write(b"NEW-P")
    reader.seek(0)
    check("same fd still sees old content", reader.read() == b"OLD-P")
    check("reader opened after commit sees new content", fs.open(p, "rb").read() == b"NEW-P")
    reader.close()

    section("L3  local: exception inside context -> discard all")
    fs.pipe_file(p, b"OLD-P")
    if os.path.exists(q):
        os.remove(q)
    with private_tmpdir() as td:
        try:
            with fs.transaction:
                fs.open(p, "wb").write(b"NEW-P")
                fs.open(q, "wb").write(b"NEW-Q")
                raise RuntimeError("boom inside transaction")
        except RuntimeError:
            pass
        check("old object untouched", fs.cat(p) == b"OLD-P")
        check("new object never appears", not fs.exists(q))
        check("temp files cleaned", glob.glob(os.path.join(td, "tmp*")) == [])
    check("transaction state reset", fs._intrans is False and fs._transaction is None)

    section("L4  local: commit of file 'b' fails midway -> prefix committed")
    a, b, c = [os.path.join(root, n) for n in ("a", "b", "c")]
    for x in (a, b, c):
        if os.path.exists(x):
            os.remove(x)
    real_commit = LocalFileOpener.commit

    def flaky_commit(self):
        if os.path.basename(self.path) == "b":
            raise PermissionError("injected failure while committing b")
        real_commit(self)

    LocalFileOpener.commit = flaky_commit
    with private_tmpdir() as td:
        try:
            with fs.transaction:
                for name, x in (("a", a), ("b", b), ("c", c)):
                    with fs.open(x, "wb") as f:
                        f.write(name.encode())
        except PermissionError as e:
            check("commit error surfaces to caller", "injected" in str(e), str(e))
        finally:
            LocalFileOpener.commit = real_commit
        check("earlier file a is committed and visible", fs.cat(a) == b"a")
        check("failed file b never appears", not os.path.exists(b))
        check("later file c is discarded", not os.path.exists(c))
        check("temp files all cleaned", glob.glob(os.path.join(td, "tmp*")) == [])
    check("state reset after failed complete", fs._intrans is False and fs._transaction is None)
    later = os.path.join(root, "later")
    fs.pipe_file(later, b"ok")
    check("instance usable afterwards; write not silently deferred", fs.cat(later) == b"ok")

    section("L5  local: what a reader sees BETWEEN two commits (mixed versions)")
    a, b, c = [os.path.join(root, n) for n in ("va", "vb", "vc")]
    for x in (a, b, c):
        fs.pipe_file(x, b"old-" + os.path.basename(x).encode())
    real_commit = LocalFileOpener.commit
    snapshot = {}

    def spying_commit(self):
        real_commit(self)
        if os.path.basename(self.path) == "va":
            # a concurrent reader polling right after the first commit sees:
            snapshot["va"] = fs.cat(a)
            snapshot["vb"] = fs.cat(b)
            snapshot["vc"] = fs.cat(c)

    LocalFileOpener.commit = spying_commit
    try:
        with fs.transaction:
            for x in (a, b, c):
                with fs.open(x, "wb") as f:
                    f.write(b"new-" + os.path.basename(x).encode())
    finally:
        LocalFileOpener.commit = real_commit
    check("mid-commit: first file already new", snapshot.get("va") == b"new-va")
    check("mid-commit: second file still old", snapshot.get("vb") == b"old-vb")
    check("mid-commit: third file still old", snapshot.get("vc") == b"old-vc")
    check("after complete: all new",
          fs.cat(a) == b"new-va" and fs.cat(b) == b"new-vb" and fs.cat(c) == b"new-vc")

    # combined case: preexisting files + injected failure at the second commit
    for x in (a, b, c):
        fs.pipe_file(x, b"old-" + os.path.basename(x).encode())
    real_commit = LocalFileOpener.commit

    def flaky_commit(self):
        if os.path.basename(self.path) == "vb":
            raise PermissionError("injected failure on preexisting vb")
        real_commit(self)

    LocalFileOpener.commit = flaky_commit
    try:
        with fs.transaction:
            for x in (a, b, c):
                with fs.open(x, "wb") as f:
                    f.write(b"new2-" + os.path.basename(x).encode())
    except PermissionError:
        pass
    finally:
        LocalFileOpener.commit = real_commit
    check("preexisting: prefix replaced, failed + later files keep OLD content",
          fs.cat(a) == b"new2-va" and fs.cat(b) == b"old-vb" and fs.cat(c) == b"old-vc")

    section("L6  local: cross-device commit is copy+unlink -> partial file VISIBLE")
    dev_target = os.stat(root).st_dev
    dev_shm = os.stat("/dev/shm").st_dev if os.path.isdir("/dev/shm") else dev_target
    if dev_shm == dev_target:
        print("  [SKIP] /dev/shm on same device; cannot reach shutil.copy fallback")
    else:
        old = tempfile.tempdir
        tempfile.tempdir = "/dev/shm"
        try:
            xdev = os.path.join(root, "xdev")
            fs.pipe_file(xdev, b"OLD-XDEV")
            real_copyfile = shutil.copyfile

            def partial_copy(src, dst, **kw):
                with open(dst, "wb") as f:
                    f.write(b"PARTIAL")  # only part of the payload
                raise RuntimeError("injected failure mid copy")

            shutil.copyfile = partial_copy
            try:
                with fs.transaction, fs.open(xdev, "wb") as f:
                    f.write(b"PARTIAL-COMPLETE-NEW-CONTENT")
            except RuntimeError as e:
                check("copy failure surfaces", "mid copy" in str(e), str(e))
            finally:
                shutil.copyfile = real_copyfile
            content = fs.cat(xdev)
            check("target now holds PARTIAL bytes (old object destroyed)",
                  content == b"PARTIAL", repr(content))
            check("state reset after cross-device failure",
                  fs._intrans is False and fs._transaction is None)
        finally:
            tempfile.tempdir = old

    section("L7  local: rm / cp_file / mv bypass the transaction")
    fs.pipe_file(p, b"OLD-P")
    src = os.path.join(root, "src")
    dst = os.path.join(root, "dst")
    fs.pipe_file(src, b"SRC")
    try:
        with fs.transaction:
            fs.rm(p)
            fs.cp_file(src, dst)
            raise RuntimeError("abort")
    except RuntimeError:
        pass
    check("rm is immediate and NOT rolled back", not fs.exists(p))
    check("cp_file is immediate and NOT rolled back", fs.cat(dst) == b"SRC")
    touched = os.path.join(root, "touched")
    with fs.transaction:
        fs.touch(touched)
        check("local touch bypasses the transaction (visible immediately)", fs.exists(touched))

    section("L8  local: same path written twice in one transaction")
    dup = os.path.join(root, "dup")
    with fs.transaction:
        with fs.open(dup, "wb") as f:
            f.write(b"FIRST")
        with fs.open(dup, "wb") as f:
            f.write(b"SECOND")
    check("no conflict detection; queue order decides (last wins)",
          fs.cat(dup) == b"SECOND", repr(fs.cat(dup)))

    section("L9  local: listings cache is not involved")
    fs2 = fresh_local(use_listings_cache=True)
    l8 = os.path.join(root, "l8")
    with fs2.transaction, fs2.open(l8, "wb") as f:
        f.write(b"data")
    check("ls sees new file immediately after commit",
          any(x.endswith("l8") for x in fs2.ls(root)))
    check("dircache never populated by local ls", len(fs2.dircache) == 0)

    section("L10  local: autocommit=True inside a transaction bypasses it")
    esc = os.path.join(root, "esc")
    with fs.transaction:
        fs.open(esc, "wb", autocommit=True).write(b"ESCAPED")
        check("autocommit write visible immediately, inside transaction", fs.exists(esc))
    check("escape hatch content persisted", fs.cat(esc) == b"ESCAPED")

    section("L11 local: nested transaction drops writes AND crashes on outer exit")
    nest = os.path.join(root, "nest")
    if os.path.exists(nest):
        os.remove(nest)
    with private_tmpdir() as td:
        nested_error = None
        try:
            with fs.transaction:
                fs.open(nest, "wb").write(b"OUTER")
                with fs.transaction:  # same object; start() resets queue, exit nulls fs
                    pass
        except AttributeError as e:
            nested_error = e
        check("outer context exit crashes with AttributeError",
              nested_error is not None, repr(nested_error))
        check("outer write silently lost (never committed)", not os.path.exists(nest))
        leaked = glob.glob(os.path.join(td, "tmp*"))
        check("orphaned temp file leaked on disk", len(leaked) == 1, f"{len(leaked)} leaked")
        check("instance left without a live transaction anyway",
              fs._transaction is None and fs._intrans is False)


def local_mode_matrix(root):
    section("L12 local: only modes containing 'w' are deferred (ab/xb break)")
    cases = [("wb", False), ("xb", False), ("ab", True), ("r+b", True)]
    for mode, preexist in cases:
        d = fresh_dir(root, "mode-" + mode.replace("+", "p"))
        path = os.path.join(d, "f")
        fs = fresh_local()
        if preexist:
            fs.pipe_file(path, b"base")
        err = None
        inside = None
        try:
            with fs.transaction:
                with fs.open(path, mode) as f:
                    f.write(b"Z")
                inside = fs.cat(path) if fs.exists(path) else None
        except AttributeError as e:
            err = e
        final = fs.cat(path) if fs.exists(path) else None
        deferred = inside is None
        print(f"   mode={mode:4} deferred={deferred} exit_error={type(err).__name__ if err else None} final={final!r}")
        if mode == "wb":
            check("wb: deferred, clean commit", deferred and err is None and final == b"Z")
        elif mode == "r+b":
            check("r+b: bypasses transaction silently (immediate mutation, no crash)",
                  not deferred and err is None and inside == b"Zase" and final == b"Zase")
        else:
            check(f"{mode}: writes immediately AND commit raises AttributeError",
                  not deferred and isinstance(err, AttributeError))
        if err is not None:
            # state must still be reset after the crash so the next case is clean
            check(f"{mode}: transaction state reset after error",
                  fs._intrans is False and fs._transaction is None)


def local_discard_failure(root):
    section("L13 local: discard itself fails during rollback")
    fs = fresh_local()
    d = fresh_dir(root, "discard-fail")
    with private_tmpdir() as td:
        def flaky_discard(self):
            raise OSError("injected discard failure")

        LocalFileOpener.discard = flaky_discard
        raised = None
        try:
            with fs.transaction:
                fs.open(os.path.join(d, "a"), "wb").write(b"a")
                fs.open(os.path.join(d, "b"), "wb").write(b"b")
                raise RuntimeError("user exception")
        except OSError as e:
            raised = e
        finally:
            del LocalFileOpener.discard  # restore inherited method
        check("discard error masks the user exception",
              isinstance(raised, OSError) and isinstance(raised.__context__, RuntimeError))
        check("state still resets", fs._intrans is False and fs._transaction is None)
        check("no payload ever becomes visible",
              not fs.exists(os.path.join(d, "a")) and not fs.exists(os.path.join(d, "b")))
        check("temp files leak because cleanup itself failed",
              len(glob.glob(os.path.join(td, "tmp*"))) == 2)


def memory_mode_matrix():
    section("M10  memory: wb/xb/ab deferred; r+b mutates the shared object immediately")
    # wb / xb: new files, invisible until commit
    for mode in ("wb", "xb"):
        fs = fresh_memory()
        path = "/mm-" + mode
        with fs.transaction:
            with fs.open(path, mode) as f:
                f.write(b"Z")
            inside_exists = fs.exists(path)
        check(f"{mode}: deferred inside transaction", not inside_exists and fs.cat(path) == b"Z")
    # ab: preexisting object's CONTENT stays unchanged until commit (copy semantics)
    fs = fresh_memory()
    fs.pipe_file("/mm-ab", b"base")
    with fs.transaction, fs.open("/mm-ab", "ab") as f:
        f.write(b"Z")
        check("ab: store content unchanged inside transaction", fs.cat("/mm-ab") == b"base")
    check("ab commit preserves prefix", fs.cat("/mm-ab") == b"baseZ")
    # r+b: returns the store object itself -> bypass, immediate in-place mutation
    fs = fresh_memory()
    fs.pipe_file("/mm-rb", b"base")
    with fs.transaction, fs.open("/mm-rb", "r+b") as f:
        f.write(b"Z")
        check("r+b: mutation visible immediately (transaction bypassed)",
              fs.cat("/mm-rb") == b"Zase")
    check("r+b: stays mutated after exit", fs.cat("/mm-rb") == b"Zase")


def local_crash_demo(root):
    section("L14 local: process death mid-complete leaves prefix + orphaned temp")
    import subprocess

    crash_dir = fresh_dir(root, "crash")
    crash_tmp = fresh_dir(root, "crash-tmp")
    child = (
        "import os, tempfile, fsspec\n"
        "from fsspec.implementations.local import LocalFileOpener\n"
        "tempfile.tempdir = os.environ['AUDIT_TMP']\n"
        "fs = fsspec.filesystem('file', skip_instance_cache=True)\n"
        "root = os.environ['AUDIT_ROOT']\n"
        "real = LocalFileOpener.commit\n"
        "def boom(self):\n"
        "    if os.path.basename(self.path) == 'b':\n"
        "        if not self.f.closed:\n"
        "            self.f.close()\n"
        "        os._exit(7)  # hard death: no finally, no discard, no journal replay\n"
        "    real(self)\n"
        "LocalFileOpener.commit = boom\n"
        "with fs.transaction:\n"
        "    for n in ('a', 'b', 'c'):\n"
        "        with fs.open(os.path.join(root, n), 'wb') as f:\n"
        "            f.write(n.encode())\n"
    )
    env = dict(os.environ, AUDIT_ROOT=crash_dir, AUDIT_TMP=crash_tmp)
    proc = subprocess.run([sys.executable, "-c", child], env=env, capture_output=True)
    check("child died hard (no exception handling ran)", proc.returncode == 7)
    a = os.path.join(crash_dir, "a")
    b = os.path.join(crash_dir, "b")
    c = os.path.join(crash_dir, "c")
    check("prefix file a stays committed", os.path.exists(a) and open(a, "rb").read() == b"a")
    check("b never appears", not os.path.exists(b))
    check("c never appears", not os.path.exists(c))
    orphans = glob.glob(os.path.join(crash_tmp, "tmp*"))
    check("b's and c's temp files are orphaned on disk (no cleanup at all)",
          len(orphans) == 2, f"{len(orphans)} orphan(s)")
    # no replay exists: a new process just sees this mixed state as-is
    fs2 = fresh_local()
    check("new process sees mixed state, no auto-repair",
          fs2.exists(a) and not fs2.exists(b) and not fs2.exists(c))


def memory_scenarios():
    fs = fresh_memory()
    fs.pipe_file("/p", b"OLD-P")

    section("M1  memory: writes invisible until commit")
    with fs.transaction:
        with fs.open("/p", "wb") as f:
            f.write(b"NEW-P")
        with fs.open("/q", "wb") as f:
            f.write(b"NEW-Q")
        check("overwrite: old object stays visible", fs.cat("/p") == b"OLD-P")
        check("new object does not appear", not fs.exists("/q"))
        check("store untouched mid-transaction", "/q" not in fs.store)
    check("new object appears after commit", fs.cat("/q") == b"NEW-Q")
    check("overwrite lands after commit", fs.cat("/p") == b"NEW-P")

    section("M2  memory: open reader holds the OLD object after commit")
    fs.pipe_file("/p", b"OLD-P")
    reader = fs.open("/p", "rb")
    check("reader sees old content before commit", reader.read() == b"OLD-P")
    with fs.transaction, fs.open("/p", "wb") as f:
        f.write(b"NEW-P")
    reader.seek(0)
    check("same reader object still sees old content", reader.read() == b"OLD-P")
    check("new open sees new content", fs.open("/p", "rb").read() == b"NEW-P")

    section("M3  memory: exception inside context -> nothing visible")
    fs.pipe_file("/p", b"OLD-P")
    if fs.exists("/q"):
        fs.rm("/q")
    try:
        with fs.transaction:
            fs.open("/p", "wb").write(b"NEW-P")
            fs.open("/q", "wb").write(b"NEW-Q")
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    check("old object untouched", fs.cat("/p") == b"OLD-P")
    check("new object never appears", not fs.exists("/q"))
    check("transaction state reset", fs._intrans is False and fs._transaction is None)

    section("M4  memory: commit of 'b' fails midway -> prefix committed")
    for x in ("/a", "/b", "/c"):
        if fs.exists(x):
            fs.rm(x)
    real_commit = MemoryFile.commit

    def flaky_commit(self):
        if self.path == "/b":
            raise RuntimeError("injected failure while committing /b")
        real_commit(self)

    MemoryFile.commit = flaky_commit
    try:
        with fs.transaction:
            for name in ("a", "b", "c"):
                with fs.open("/" + name, "wb") as f:
                    f.write(name.encode())
    except RuntimeError as e:
        check("commit error surfaces", "injected" in str(e), str(e))
    finally:
        MemoryFile.commit = real_commit
    check("earlier file /a committed and visible", fs.cat("/a") == b"a")
    check("failed file /b never entered store", not fs.exists("/b"))
    check("later file /c discarded", not fs.exists("/c"))
    check("state reset after failed complete", fs._intrans is False and fs._transaction is None)

    section("M5  memory: what a reader sees BETWEEN two commits (mixed versions)")
    for name in ("ma", "mb", "mc"):
        fs.pipe_file("/" + name, b"old-" + name.encode())
    real_commit = MemoryFile.commit
    snapshot = {}

    def spying_commit(self):
        real_commit(self)
        if self.path == "/ma":
            snapshot["ma"] = fs.cat("/ma")
            snapshot["mb"] = fs.cat("/mb")
            snapshot["mc"] = fs.cat("/mc")

    MemoryFile.commit = spying_commit
    try:
        with fs.transaction:
            for name in ("ma", "mb", "mc"):
                with fs.open("/" + name, "wb") as f:
                    f.write(b"new-" + name.encode())
    finally:
        MemoryFile.commit = real_commit
    check("mid-commit: first object already new", snapshot.get("ma") == b"new-ma")
    check("mid-commit: second object still old", snapshot.get("mb") == b"old-mb")
    check("mid-commit: third object still old", snapshot.get("mc") == b"old-mc")

    section("M6  memory: readers share ONE BytesIO object (cursor is shared)")
    fs.pipe_file("/shared", b"0123456789")
    r1 = fs.open("/shared", "rb")
    r2 = fs.open("/shared", "rb")
    check("two opens return the SAME object", r1 is r2)
    r1.read(3)
    chunk = r2.read(3)
    check("second reader continues from first reader's cursor",
          chunk == b"345", repr(chunk))
    # non-transactional append mutates the object readers hold
    r3 = fs.open("/shared", "ab")
    r3.write(b"XYZ")
    r1.seek(0)
    check("non-transactional append visible to existing reader immediately",
          r1.read() == b"0123456789XYZ")

    section("M7  memory: rm / cp_file bypass the transaction")
    fs.pipe_file("/p", b"OLD-P")
    fs.pipe_file("/src", b"SRC")
    if fs.exists("/dst"):
        fs.rm("/dst")
    try:
        with fs.transaction:
            fs.rm("/p")
            fs.cp_file("/src", "/dst")
            check("cp_file visible immediately, inside transaction", fs.exists("/dst"))
            raise RuntimeError("abort")
    except RuntimeError:
        pass
    check("rm is immediate and NOT rolled back", not fs.exists("/p"))
    check("cp_file is NOT rolled back", fs.cat("/dst") == b"SRC")
    with fs.transaction:
        fs.touch("/touched")
        check("memory touch IS deferred (invisible until commit)", not fs.exists("/touched"))
    check("memory touch appears after commit", fs.exists("/touched"))

    section("M8  memory: append in transaction works on a private copy")
    fs.pipe_file("/ap", b"base")
    try:
        with fs.transaction:
            with fs.open("/ap", "ab") as f:
                f.write(b"+tail")
            check("store object unchanged before commit", fs.cat("/ap") == b"base")
            raise RuntimeError("abort")
    except RuntimeError:
        pass
    check("discarded append leaves original bytes", fs.cat("/ap") == b"base")
    with fs.transaction, fs.open("/ap", "ab") as f:
        f.write(b"+tail")
    check("committed append appears", fs.cat("/ap") == b"base+tail")

    section("M9  memory: global store is process-wide, transaction is per-instance")
    g1 = MemoryFileSystem()  # cached global instance
    g2 = MemoryFileSystem(skip_instance_cache=True)  # different instance, same store
    with g1.transaction:
        with g1.open("/g", "wb") as f:
            f.write(b"G")
        check("uncommitted write invisible to other instance too", not g2.exists("/g"))
    check("committed write visible to other instance", g2.cat("/g") == b"G")
    check("global store is shared dict", g1.store is g2.store)


def main():
    root = tempfile.mkdtemp(prefix="fsspec-audit-target-")
    print(f"fsspec {fsspec.__version__}  target dir: {root}")
    try:
        local_scenarios(fresh_dir(root, "local"))
        local_mode_matrix(root)
        local_discard_failure(root)
        local_crash_demo(root)
        memory_scenarios()
        memory_mode_matrix()
    finally:
        shutil.rmtree(root, ignore_errors=True)
    failed = [label for label, ok, _ in RESULTS if not ok]
    print("\n" + "=" * 78)
    print(f"SUMMARY: {len(RESULTS) - len(failed)}/{len(RESULTS)} probes matched")
    if failed:
        print("MISMATCHES:")
        for label in failed:
            print("  -", label)
    print("=" * 78)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
