package redis

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

type Sentinel struct {
	conn       Conn
	options    []DialOption
	addrs      []string
	activeAddr int
	sync.Mutex
}

// NewSentinel creates a new sentinel client connection. Dial options passed to
// this function will be used when connecting to the sentinel server. Make sure
// to provide a short timeouts for all opeations (connect, read, write) as per
// redis-sentinel client guidelines.
//
// Note that in a worst-case scenario, the timeout for performing an
// operation with a Sentinel client may take (# sentinels) * timeout to try all
// configured sentinel addresses.
func NewSentinel(addrs []string, options ...DialOption) *Sentinel {
	return &Sentinel{
		options: options,
		addrs:   addrs,
	}
}

// do will atempt to execute single redis command on any of the configured
// sentinel servers. In worst case it will try all sentinel servers exactly once
// and return last encountered error.
func (sc *Sentinel) do(cmd string, args ...interface{}) (interface{}, error) {
	var err error
	var reply interface{}

	for i := 0; i < len(sc.addrs); i++ {
		reply, err = sc.doOnce(cmd, args...)
		if err != nil {
			// Retry with the next sentinel in the list.
			sc.activeAddr = (sc.activeAddr + 1) % len(sc.addrs)
			continue
		}
	}

	return reply, err
}

// doOnce tries to execute single redis command on the sentinel connection. If
// necessary it will dial before sending command.
func (sc *Sentinel) doOnce(cmd string, args ...interface{}) (interface{}, error) {
	if sc.conn == nil {
		var err error
		sc.conn, err = Dial("tcp", sc.addrs[sc.activeAddr], sc.options...)
		if err != nil {
			return nil, err
		}
	}

	reply, err := sc.conn.Do(cmd, args...)
	if err != nil {
		sc.conn.Close()
		sc.conn = nil
	}
	return reply, err
}

// MasterAddress looks up the configuration for a named monitored instance
// set and returns the master's configuration.
func (sc *Sentinel) MasterAddress(name string) (string, error) {
	sc.Lock()
	defer sc.Unlock()

	res, err := Strings(sc.do("SENTINEL", "get-master-addr-by-name", name))
	masterAddr := strings.Join(res, ":")
	return masterAddr, err
}

// Slaves looks up the configuration for a named monitored
// instance set and returns all the slave configuration. Note that the return is
// a []map[string]string, and will most likely need to be interpreted by
// SlaveAddr to be usable.
func (sc *Sentinel) Slaves(name string) ([]map[string]string, error) {
	sc.Lock()
	defer sc.Unlock()

	res, err := Values(sc.do("SENTINEL", "slaves", name))
	if err != nil {
		return nil, err
	}
	slaves := make([]map[string]string, 0)
	for _, a := range res {
		sm, err := StringMap(a, err)
		if err != nil {
			return slaves, err
		}
		slaves = append(slaves, sm)
	}
	return slaves, err
}

// Close will close connection to the sentinel server if one is esatablised.
func (sc *Sentinel) Close() {
	sc.Lock()
	defer sc.Unlock()

	if sc.conn != nil {
		sc.conn.Close()
		sc.conn = nil
	}
}

// SlaveAddresses converts full slaves info slice returned from Slaves to a
// slice of slave server addresses.
func SlaveAddresses(slaves []map[string]string, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}

	addrs := make([]string, len(slaves))
	for i, slave := range slaves {
		addrs[i] = fmt.Sprintf("%s:%s", slave["ip"], slave["port"])
	}
	return addrs, nil
}

// TestRole is a convenience function for checking redis server role. It
// uses the ROLE command introduced in redis 2.8.12. Nil is returned if server
// role matches the expected role.
//
// It is recommended by the redis client guidelines to test the role of any
// newly established connection before use.
func TestRole(c Conn, expectedRole string) error {
	res, err := Values(c.Do("ROLE"))
	if err != nil {
		return err
	}
	role, err := String(res[0], nil)
	if err != nil {
		return err
	}
	if role != expectedRole {
		return errors.New("role check failed")
	}
	return nil
}
