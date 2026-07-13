---
title: Common workflows
description: Find the relevant go-car v2 packages for reading, writing, indexing, and storage.
---

## Read a CAR stream

Start in the v2 root package for CAR header inspection and sequential block
reading. Use the API search for `NewBlockReader` and inspect the source-derived
examples beside the related types.

## Write or wrap content

The v2 root package contains writers and options for producing CAR data. Choose
options deliberately: padding, index characteristics, and data layout affect
interoperability and random access.

## Build and use indexes

Use the `index` package to inspect available index codecs and construct or read
an index. The `indexstore` package connects index storage to higher-level
workflows.

## Use CAR files as block storage

Look under `blockstore` and `storage` in the v2 API. Their package docs and
examples explain the contracts for read-only, read-write, and random-access
use. Treat filesystem paths and external readers as caller-controlled resources
and close them according to the documented API contract.

## Validate before deployment

Test with representative archives, including malformed and truncated input.
CAR data may be untrusted; preserve application-level limits for file size,
block count, CID validation, memory use, and I/O time.
