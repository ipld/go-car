### Impact

**Versions impacted**
  * `<=` go-car@v0.3.3
  * `<=` go-car@v2.3.0

**Description**

Decoding CAR data from untrusted user input can cause:

- Panics:
  - Out of bound memory access
  - Out of memory
  - Divide by zero
- Excessive memory usage

Such panics can be triggered by intentionally malformed CARv1 data, including CARv1 data within a CARv2 container; and also CARv2 data with excessively large indexes.

These vulnerabilities are not known to be exploited in the wild and were discovered primarily with the use of code fuzzing tooling.

**Details**

**Out of bound memory access** (OOB), **out of memory** (OOM) panics or **excessive memory usage** can be triggered by decode of malformed CARv1 headers, malformed CARv1 sections, and malformed CIDv0 data used in CARv1 sections. This also applies to CARv1 data within a CARv2 container.

**Divide by zero**, **out of memory** (OOM) panics or **excessive memory usage** can be triggered by decode of intentionally malformed CARv2 indexes, or CARv2 indexes which are larger than available system memory (i.e. parallelization of CARv2 decodes may increase such a vulnerability).

### Patches

**Fixed versions**

* `>=` go-car@v0.4.0
* `>=` go-car@v2.4.0

**Description of user-facing changes**

***go-car@v0.4.0*** imposes a fixed maximum header length and section length of 32 MiB during decode. Headers exceeding this length will cause the decoder to return an error as the initial CAR decode occurs. Sections (the combination of CID and block data) exceeding this length will cause the decoder to return an error as that section is read.

The default maximum of 32 MiB may be changed _globally_ in an application instance by directly changing the `MaxAllowedSectionSize` variable in the `github.com/ipld/go-car/util` package.

We recommend that users of go-car@v0 upgrade to go-car@v2, where these maximums may be applied per-decode rather than globally.

***go-car@v2.4.0*** imposes a default maximum header length of 32 MiB and a default maximum section length of 8 MiB. Headers exceeding this length will cause the decoder to return an error as the initial CAR decode occurs. Sections (the combination of CID and block data) exceeding this length will cause the decoder to return an error as that section is read.

The default values may be adjusted by supplying a `MaxAllowedHeaderSize(x)` or `MaxAllowedSectionSize(y)` option to any decode function that accepts options. These include:

* `OpenReader()`
* `NewReader()`
* `NewBlockReader()`
* `ReadVersion()`
* `LoadIndex()`
* `GenerateIndex()`
* `ReadOrGenerateIndex()`
* `WrapV1()`
* `ExtractV1File()`
* `ReplaceRootsInFile()`
* `blockstore/NewBlockReader()`
* `blockstore/NewReadOnly()`
* `blockstore/OpenReadOnly()`
* `blockstore/OpenReadWrite()`

Please be aware that the default values are **very generous** and may be lowered where a user wants to impose restrictions closer to typical sizes.

* Typical header lengths should be in the order of 60 bytes, but the CAR format does not specify a maximum number of roots a header may contain. The default maximum of 32 MiB makes room for novel uses of the CAR format.
* Typical IPLD block sizes are under 2 MiB, and it is generally recommended that they not be above 1 MiB for maximum interoperability (e.g. there are hard limitations when sharing IPLD data with IPFS). CARv1 sections are the concatenation of CID and block bytes. The default maximum section length of 8 MiB makes room for novel IPLD data.

***go-car@v2.4.0*** also changes the behavior of indexes read from existing CARv2 data. The `index.ReadFrom()` API lazily loads index data as required rather than being fully read into memory on load. The direct `Unmarshal()` API for specific index implementations is still available and will perform a full read of index data; a new `UnmarshalLazyRead()` API is now available on index implementations, this is now used by `index.ReadFrom()`.

Lazy loading *may* impact the performance profile of a CARv2 read depending on the specific usage scenario. The `blockstore` package contains a more efficient in-memory index that is generated each time a CAR is loaded and may be useful where the performance of random-access to blocks is of concern.

***go-car@v2.4.0*** introduces a new API that can be used to inspect a CAR and check for various errors, including those detailed in this advisory. The `Reader#Inspect(bool)` API returns a `CarStats` object with various details about the CAR, such as its version, number of blocks, and details about codecs and multihashers used. When its argument is `true`, it will also perform a full hash consistency check of blocks contained within the CAR to ensure they match the CIDs. When `false`, block data is skipped over so a scan will likely be more efficient than reading blocks through a `BlockReader` if statistics and/or validity checking is all that's required. Note that `Inspect()` does minimal checking of index data; the strong recommendation is that if index data is untrusted then it should be re-generated.

### Workarounds

There are no workarounds for vulnerabilities in impacted versions decoding CARv1 data. Users of impacted versions should avoid accepting CAR data from untrusted sources.

OOM or excessive memory usage vulnerabilities resulting from CARv2 index parsing in impacted versions can be avoided by not reading indexes from CARv2 data from untrusted sources.

### References

Details on the CARv1 and CARv2 formats, including the composition of CARv1 headers and sections, and CARv2 indexes can be found in the CAR format specifications: https://ipld.io/specs/transport/car/

### For more information

If you have any questions or comments about this advisory please open an issue in [go-car](https://github.com/ipld/go-car).
