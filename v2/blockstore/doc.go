// package blockstore implements IPFS blockstore interface backed by a CAR file.
// This package provides two flavours of blockstore: ReadOnly and ReadWrite.
//
// The ReadOnly blockstore provides a read-only random access from a given data payload either in
// unindexed v1 format or indexed/unindexed v2 format:
// - ReadOnly.ReadOnlyOf can be used to instantiate a new read-only blockstore for a given CAR v1
//   data payload and an existing index. See index.Generate for index generation from CAR v1
//   payload.
// - ReadOnly.OpenReadOnly can be used to instantiate a new read-only blockstore for a given CAR v2
//   file with automatic index generation if the index is not present in the given file. This
//   function can optionally attach the index to the given CAR v2 file.
//

package blockstore