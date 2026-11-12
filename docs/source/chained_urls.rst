How chained URLs are expanded
=============================

A chained URL such as ``zip://member::memory://archive.zip`` is a shorthand
for passing ``fo``, ``target_protocol`` and ``target_options`` to the outer
filesystem. This page describes exactly how the string and the keyword
arguments are attributed to the layers, and which cases cannot be expressed.
The behaviour below is pinned by ``fsspec/tests/test_chained_routing.py``,
which observes routing with recording stand-in backends rather than
inferring parameters from a successful read.

The layer map
-------------

``fsspec.core._un_chain(url, kwargs)`` returns the ordered list
``[(path, protocol, options), ...]`` from outermost to innermost layer:

#. The URL is split on ``::``. A fragment that is exactly a known protocol
   name (``simplecache``) is treated as ``simplecache://``; a fragment
   containing ``://`` is kept as written; any other fragment without
   ``://`` becomes ``protocol://path``.
#. Layers are processed in reverse (innermost first), because each outer
   layer has to know its target first.
#. Each layer is stripped with **its own class's** ``_strip_protocol``.
#. A chained layer with an empty path is handed the next layer's path as its
   ``fo``. Thus ``simplecache::memory://a/file`` gives both layers the path
   ``a/file`` (memory receives ``/a/file`` after its own stripping).

For the three-layer URL
``outer://outer/name::mid://mid/name::inner://inner/file`` the map is::

    [("outer/name", "outer", {}),
     ("mid/name",   "mid",   {}),
     ("inner/file", "inner", {"inner_opt": 9})]

and ``url_to_fs`` nests them as::

    outer(target_protocol="mid",
          fo="mid/name",
          target_options={"target_protocol": "inner",
                          "target_options": {"inner_opt": 9},
                          "fo": "inner/file"})

Each layer receives only the path it owns plus a description of the layer
below; it never receives the outer path.

Ownership of ``storage_options``
--------------------------------

* Options scoped by protocol name (``mid={"opt": 1}``) belong to that
  protocol's layer. With **repeated protocol layers** the dict is popped
  once during the reverse pass, so it is applied to the *innermost*
  occurrence; the outer occurrence only sees it nested inside
  ``target_options``.
* Unscoped keyword arguments are applied to the **outermost** layer only.
* ``protocol=...`` is popped by the innermost layer and is not passed to any
  constructor. For a single-layer URL it overrides the class but keeps the
  original string, so the foreign ``://`` prefix survives in the path (for
  example ``url_to_fs("a://x", protocol="b")`` yields path ``"a://x"``).
* Keyword arguments encoded in the URL itself (``_get_kwargs_from_urls``) are
  merged with explicit ones. The same key with the **same** value is
  deduplicated; the same key with a **different** value raises ``TypeError``
  (the rule codified by ``test_repeated_argument`` for cloud URLs).

Caching
-------

Instances are cached per class on a token built from the constructor
arguments, including the nested ``target_options``. Repeating a URL with the
same options returns the identical instance graph; changing only an outer
option rebuilds the outer layer while reusing the unchanged inner layers;
changing an inner option rebuilds that inner instance. Two hops of the same
protocol are still two distinct instances.

Explicit filesystem instances
-----------------------------

A chained filesystem accepts ``fs=`` in place of ``target_protocol`` /
``target_options``. ``url_to_fs("simplecache://path", fs=instance)`` passes
the instance straight through and constructs no target.

Two boundaries are intentional and should not be read as routing bugs:

* ``simplecache::inner://path`` with ``simplecache={"fs": instance}`` raises
  ``ValueError`` because the chain derives ``target_protocol`` for the same
  layer as well.
* Archive layers (``zip``, ``tar``, ``libarchive``) are not
  ``ChainedFileSystem`` subclasses: they keep ``fs`` only as an ordinary
  option and still resolve their target from ``target_protocol`` / ``fo``.

Glob and ``open_files``
------------------------

``open_files`` resolves one filesystem from the first URL and expands the
pattern on it, then filters results with ``fs.isdir``. For a chain this is
the **outermost** instance; a delegating wrapper may forward the call, but
the resolver never globs on behalf of an inner layer. A list of URLs is
checked for protocol mismatch using each element's **outer** protocol, and
each element's outer path is extracted independently.

Minimal counterexamples
-----------------------

Archive members containing the chain separator cannot be addressed::

    zip://dir/with::inside.txt::memory://arch.zip

splits into ``("dir/with", "zip")``, a local file layer for
``inside.txt``, and ``("/arch.zip", "memory")`` - the member name is lost.
There is no escape for ``::`` inside a chained URL. Construct the archive
filesystem explicitly instead, so the member name never reaches the chain
parser::

    fs = fsspec.filesystem(
        "zip", fo="/arch.zip", target_protocol="memory"
    )
    fs.cat("dir/with::inside.txt")

A bare protocol-like member name is ambiguous: ``zip://simplecache::...``
addresses a member called ``simplecache`` rather than adding a cache layer;
write the cache layer without a member path instead
(``zip://member::simplecache``).

Other member characters - spaces, ``#``, brackets, unicode, parentheses -
are preserved verbatim, and archive globs such as
``zip://data/*.csv::memory://arch.zip`` expand on the archive layer.

