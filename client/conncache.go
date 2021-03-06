package client

import (
	smgr "github.com/dermesser/clusterrpc/securitymanager"

	"container/list"
	"sync"
	"time"
)

/*
ConnectionCache is a pool of RPC connections. Applications call Connect() and get, transparently,
either a cached connection or a newly created one. After being finished with using the connection,
the application should call Return() with the connection if it wants to use it later again.
*/
type ConnectionCache struct {
	// Map host -> connections
	cache       map[string]*list.List
	client_name string

	mx sync.Mutex
}

func NewConnCache(client_name string) *ConnectionCache {
	return &ConnectionCache{cache: make(map[string]*list.List),
		client_name: client_name}
}

/*
Get a connection, either from the pool or a new one, depending on if there are connections
available.
*/
func (cc *ConnectionCache) Connect(peer PeerAddress,
	security_manager *smgr.ClientSecurityManager) (*Client, error) {

	cc.mx.Lock()
	defer cc.mx.Unlock()

	cls, ok := cc.cache[peer.String()]

	if ok {
		if cls.Len() > 0 {
			cl := cls.Front().Value.(*Client)
			cls.Remove(cls.Front())
			return cl, nil
		}
	} else {
		cc.cache[peer.String()] = list.New()
	}

	ch, err := NewChannelAndConnect(peer, security_manager)

	if err != nil {
		return nil, err
	}

	new_cl := NewClient(cc.client_name, ch)

	if err != nil {
		return nil, err
	}

	return &new_cl, nil
}

/*
Return a connection into the pool. Argument is a pointer to a pointer to make sure that the client
is not used by the calling function after this call.
*/
func (cc *ConnectionCache) Return(clp **Client) {
	cc.mx.Lock()
	defer cc.mx.Unlock()

	cl := *clp

	// We only have one peer, so we can always use the first element.
	cls, ok := cc.cache[(*clp).channel.peers[0].String()]

	if !ok {
		// Happens when there was a garbage collection (CleanOld()) in between
		cc.cache[(*clp).channel.peers[0].String()] = list.New()
	}

	cls.PushBack(cl)
	clp = nil
}

/*
Remove and close all connections from the pool that are older than time.Now() - older_than. Also
cleans up empty cache entries.
*/
func (cc *ConnectionCache) CleanOld(older_than time.Duration) {
	cc.mx.Lock()
	defer cc.mx.Unlock()

	for h, cls := range cc.cache {
		if cls == nil || cls.Len() == 0 {
			delete(cc.cache, h)
		}

		for cl := cls.Front(); cl != nil; cl = cl.Next() {
			if time.Now().Sub(cl.Value.(*Client).last_sent) > older_than {
				cl.Value.(*Client).Destroy()
				cls.Remove(cl)
			}
		}
	}
}

// Closes all connections
func (cc *ConnectionCache) CloseAll() {
	cc.CleanOld(0 * time.Second)
}
