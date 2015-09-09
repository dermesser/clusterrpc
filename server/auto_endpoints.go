package server

/*
* This file implements a default RPC endpoint, Health.Check(), which
* responds with an empty body and OK.
 */

import ()

// Returns a handler function that returns OK and an empty body
// iff the server is not in lameduck/loadshed mode, otherwise a NOT_OK status.
func makeHealthHandler(lameduck_state *bool) Handler {
	return func(ctx *Context) {
		if !*lameduck_state {
			ctx.Success([]byte{})
			return
		} else {
			ctx.Fail("Lameduck mode")
			return
		}
	}
}

func pingHandler(ctx *Context) {
	ctx.Success([]byte{})
	return
}
