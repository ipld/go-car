package car

import "math"

// options holds the configured options after applying a number of
// Option funcs.
type options struct {
	TraverseLinksOnlyOnce  bool
	MaxTraversalLinks      uint64
	ZeroLengthSectionAsEOF bool
}

// Option describes an option which affects behavior when
// interacting with the  interface.
type Option func(*options)

// TraverseLinksOnlyOnce prevents the traversal engine from repeatedly visiting
// the same links more than once.
//
// This can be an efficient strategy for an exhaustive selector where it's known
// that repeat visits won't impact the completeness of execution. However it
// should be used with caution with most other selectors as repeat visits of
// links for different reasons during selector execution can be valid and
// necessary to perform full traversal.
func TraverseLinksOnlyOnce() Option {
	return func(sco *options) {
		sco.TraverseLinksOnlyOnce = true
	}
}

// MaxTraversalLinks changes the allowed number of links a selector traversal
// can execute before failing.
//
// Note that setting this option may cause an error to be returned from selector
// execution when building a SelectiveCar.
func MaxTraversalLinks(MaxTraversalLinks uint64) Option {
	return func(sco *options) {
		sco.MaxTraversalLinks = MaxTraversalLinks
	}
}

// ZeroLengthSectionAsEOF sets whether to allow the CARv1 decoder to treat
// a zero-length section as the end of the input CAR file. For example, this can
// be useful to allow "null padding" after a CARv1 without knowing where the
// padding begins.
func ZeroLengthSectionAsEOF(enable bool) Option {
	return func(sco *options) {
		sco.ZeroLengthSectionAsEOF = enable
	}
}

// applyOptions applies given opts and returns the resulting options.
func applyOptions(opt ...Option) options {
	opts := options{
		TraverseLinksOnlyOnce: false,         // default: recurse until exhausted
		MaxTraversalLinks:     math.MaxInt64, // default: traverse all
	}
	for _, o := range opt {
		o(&opts)
	}
	return opts
}
