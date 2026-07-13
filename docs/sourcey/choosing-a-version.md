---
title: Choosing a version
description: Decide whether to use the go-car v2 module or the original v1 module.
---

## Use v2 for new work

Import packages beneath `github.com/ipld/go-car/v2` when starting a new
integration. The v2 module exposes CARv1 and CARv2 reading and writing,
indexing, blockstore integration, and random-access storage APIs.

Pin a released module version in `go.mod`; do not infer compatibility from this
documentation site's build date.

## Keep v1 where compatibility requires it

The original module path is `github.com/ipld/go-car`. It remains useful when an
existing dependency graph or API contract is based on v1. Its API reference is
kept in a separate tab so similarly named symbols cannot be confused with v2.

## Verify behavior at the source

The generated reference reflects one pinned upstream commit. Before upgrading,
read the upstream release notes and run your application's tests against the
specific module version selected in `go.mod`.
