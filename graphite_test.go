package graphite

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rcrowley/go-metrics"
)

func floatEquals(a, b float64) bool {
	return (a - b) < 0.000001 && (b - a) < 0.000001
}

func ExampleGraphite() {
	addr, _ := net.ResolveTCPAddr("net", ":2003")
	go Graphite(metrics.DefaultRegistry, 1 * time.Second, "some.prefix", addr)
}

func ExampleGraphiteWithConfig() {
	addr, _ := net.ResolveTCPAddr("net", ":2003")
	go GraphiteWithConfig(GraphiteConfig{
		Addr:          addr,
		Registry:      metrics.DefaultRegistry,
		FlushInterval: 1 * time.Second,
		DurationUnit:  time.Millisecond,
		Percentiles:   []float64{0.5, 0.75, 0.99, 0.999},
	})
}

func NewTestServerUDP(t *testing.T, prefix string) (map[string]float64, *net.UDPConn, metrics.Registry, GraphiteConfig, *sync.WaitGroup) {
	res := make(map[string]float64)

	cUdp, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("could not start dummy server:", err)
	}

	ln, err := net.ListenUDP("udp", cUdp)
	if err != nil {
		t.Fatal("could not start dummy server:", err)
	}

	var wg sync.WaitGroup
	go func() {
		buf := make([]byte, 1024)

		_, err := ln.Read(buf)
		for err == nil {

			value := string(buf)

			for _, v := range strings.Split(value, "\n") {
				parts := strings.Split(v, " ")
				if len(parts) > 1 {
					i, _ := strconv.ParseFloat(parts[1], 0)
					if testing.Verbose() {
						t.Log("recv", parts[0], i)
					}
					res[parts[0]] = res[parts[0]] + i

				}
			}

			ln.SetDeadline(time.Now().Add(5 * time.Second))
			_, err = ln.Read(buf)

		}
		wg.Done()
		ln.Close()
	}()

	r := metrics.NewRegistry()

	c := GraphiteConfig{
		Addr:          ln.LocalAddr(),
		Registry:      r,
		FlushInterval: 10 * time.Millisecond,
		DurationUnit:  time.Millisecond,
		Percentiles:   []float64{0.5, 0.75, 0.99, 0.999},
		Prefix:        prefix,
	}

	return res, ln, r, c, &wg
}

func NewTestServerTcp(t *testing.T, prefix string) (map[string]float64, net.Listener, metrics.Registry, GraphiteConfig, *sync.WaitGroup) {
	res := make(map[string]float64)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("could not start dummy server:", err)
	}

	var wg sync.WaitGroup
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				t.Fatal("dummy server error:", err)
			}
			r := bufio.NewReader(conn)
			line, err := r.ReadString('\n')
			for err == nil {
				parts := strings.Split(line, " ")
				i, _ := strconv.ParseFloat(parts[1], 0)
				if testing.Verbose() {
					t.Log("recv", parts[0], i)
				}
				res[parts[0]] = res[parts[0]] + i
				line, err = r.ReadString('\n')
			}
			wg.Done()
			conn.Close()
		}
	}()

	r := metrics.NewRegistry()

	c := GraphiteConfig{
		Addr:          ln.Addr().(*net.TCPAddr),
		Registry:      r,
		FlushInterval: 10 * time.Millisecond,
		DurationUnit:  time.Millisecond,
		Percentiles:   []float64{0.5, 0.75, 0.99, 0.999},
		Prefix:        prefix,
	}

	return res, ln, r, c, &wg
}

func writeOnProtocol(t *testing.T, res map[string]float64, r metrics.Registry, c GraphiteConfig, wg *sync.WaitGroup) {
	metrics.GetOrRegisterCounter("foo", r).Inc(2)

	// TODO: Use a mock meter rather than wasting 10s to get a QPS.
	for i := 0; i < 10 * 4; i++ {
		metrics.GetOrRegisterMeter("bar", r).Mark(1)
		time.Sleep(250 * time.Millisecond)
	}

	metrics.GetOrRegisterTimer("baz", r).Update(time.Second * 5)
	metrics.GetOrRegisterTimer("baz", r).Update(time.Second * 4)
	metrics.GetOrRegisterTimer("baz", r).Update(time.Second * 3)
	metrics.GetOrRegisterTimer("baz", r).Update(time.Second * 2)
	metrics.GetOrRegisterTimer("baz", r).Update(time.Second * 1)

	wg.Add(1)
	GraphiteOnce(c)
	wg.Wait()

	if expected, found := 2.0, res["foobar.foo.count"]; !floatEquals(found, expected) {
		t.Fatal("bad value:", expected, found)
	}

	if expected, found := 40.0, res["foobar.bar.count"]; !floatEquals(found, expected) {
		t.Fatal("bad value:", expected, found)
	}

	if expected, found := 4.0, res["foobar.bar.one-minute"]; !floatEquals(found, expected) {
		t.Fatal("bad value:", expected, found)
	}

	if expected, found := 5.0, res["foobar.baz.count"]; !floatEquals(found, expected) {
		t.Fatal("bad value:", expected, found)
	}

	if expected, found := 5000.0, res["foobar.baz.99-percentile"]; !floatEquals(found, expected) {
		t.Fatal("bad value:", expected, found)
	}

	if expected, found := 3000.0, res["foobar.baz.50-percentile"]; !floatEquals(found, expected) {
		t.Fatal("bad value:", expected, found)
	}
}

func TestWritesTcp(t *testing.T) {
	res, l, r, c, wg := NewTestServerTcp(t, "foobar")
	defer l.Close()
	writeOnProtocol(t, res, r, c, wg)
}

func TestWritesUDP(t *testing.T) {
	res, l, r, c, wg := NewTestServerUDP(t, "foobar")
	defer l.Close()
	writeOnProtocol(t, res, r, c, wg)
}
