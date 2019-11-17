package dsndriver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

var accessDeniedErr = &mysql.MySQLError{
	Number:  1045,
	Message: "access denied",
}

type mockConnector struct {
	cfg         *mysql.Config
	connectFunc func(context.Context) (driver.Conn, error)
	calls       int
}

func (c *mockConnector) Driver() driver.Driver { return MySQLDriver{} }
func (c *mockConnector) Connect(ctx context.Context) (driver.Conn, error) {
	c.calls++
	if c.connectFunc != nil {
		return c.connectFunc(ctx)
	}
	return &mockConn{cfg: c.cfg, def: true, connNo: c.calls}, nil
}

type mockConn struct {
	cfg    *mysql.Config
	def    bool // default from Connect() ^ default (not c.connectFunc)
	connNo int
}

func (c mockConn) Prepare(query string) (driver.Stmt, error) { return nil, nil }
func (c mockConn) Close() error                              { return nil }
func (c mockConn) Begin() (driver.Tx, error)                 { return nil, nil }

func init() {
	Debug = true
	newConnector = func(cfg *mysql.Config) (driver.Connector, error) { return &mockConnector{cfg: cfg}, nil }
}

var swapperCalls int
var gotDSN, newDSN string

func testswapper(ctx context.Context, dsn string) string {
	swapperCalls += 1
	gotDSN = dsn
	return newDSN
}

func setup() {
	SetHotswapFunc(testswapper)
	swapperCalls = 0
	gotDSN = ""
	newDSN = ""
}

func TestConnectNoError(t *testing.T) {
	setup()

	connectCalls := 0
	dsn1 := "user:pass@tcp(127.0.0.1)/"
	mct1 := &mockConnector{
		connectFunc: func(context.Context) (driver.Conn, error) {
			connectCalls += 1
			return mockConn{}, nil
		},
	}
	c := NewConnector(dsn1, mct1)

	if connectCalls != 0 {
		t.Errorf("Connect called %d times, expected 0", connectCalls)
	}
	if swapperCalls != 0 {
		t.Errorf("Hotswap func called %d times, expected 0", swapperCalls)
	}

	gotConn, err := c.Connect(context.Background())
	if err != nil {
		t.Errorf("Connect returned error '%s', expected nil", err)
	}
	if gotConn == nil {
		t.Errorf("Connect returned nil driver.Conn, expected non-nil value")
	}
	if connectCalls != 1 {
		t.Errorf("Connect called %d times, expected 1", connectCalls)
	}
	if swapperCalls != 0 {
		t.Errorf("Hotswap func called %d times, expected 0", swapperCalls)
	}
}

func TestConnectNotAccessDeniedError(t *testing.T) {
	setup()

	connectCalls := 0
	dsn1 := "user:pass@tcp(127.0.0.1)/"
	mct1 := &mockConnector{
		connectFunc: func(context.Context) (driver.Conn, error) {
			connectCalls += 1
			return nil, fmt.Errorf("some other error")
		},
	}
	c := NewConnector(dsn1, mct1)
	gotConn, err := c.Connect(context.Background())
	if err == nil {
		t.Error("Connect returned nil error, expected mock error")
	}
	if gotConn != nil {
		t.Errorf("Connect returned non-nil driver.Conn, expected nil on error")
	}
	if connectCalls != 1 {
		t.Errorf("Connect called %d times, expected 1", connectCalls)
	}
	if swapperCalls != 0 {
		t.Errorf("Hotswap func called %d times, expected 0", swapperCalls)
	}
}

func TestHotswapNoBlockers(t *testing.T) {
	setup()

	connectCalls := 0
	dsn1 := "user:pass@tcp(127.0.0.1)/"
	mct1 := &mockConnector{
		connectFunc: func(context.Context) (driver.Conn, error) {
			connectCalls += 1
			return nil, accessDeniedErr
		},
	}

	newDSN = "user2:pass2@tcp(127.0.0.1)/"

	c := NewConnector(dsn1, mct1)
	v, err := c.Connect(context.Background())
	if err != nil {
		t.Errorf("Connect returned error '%s', expected nil", err)
	}
	if v == nil {
		t.Errorf("Connect returned nil driver.Conn, expected non-nil value")
	}
	if connectCalls != 1 {
		t.Errorf("Connect called %d times, expected 1", connectCalls)
	}
	if swapperCalls != 1 {
		t.Errorf("Hotswap func called %d times, expected 0", swapperCalls)
	}

	if gotDSN != dsn1 {
		t.Errorf("got DSN '%s', expected '%s'", gotDSN, dsn1)
	}

	conn, ok := v.(*mockConn)
	if !ok {
		t.Errorf("Connect did not return a mockConn, returned %#v", v)
	} else if conn.cfg.User != "user2" {
		t.Errorf("new DSN user = '%s', expected 'user2'", conn.cfg.User)
	}

	// Using new Connector, not original, so when we connect again, the orginal
	// should not increment the vars it enclosed. We can verify that new conn
	// is not from this test's connector because default conns have default=true.
	v, err = c.Connect(context.Background())
	if err != nil {
		t.Errorf("Connect returned error '%s', expected nil", err)
	}
	if v == nil {
		t.Errorf("Connect returned nil driver.Conn, expected non-nil value")
	}
	if connectCalls != 1 {
		t.Errorf("Connect called %d times, expected 1", connectCalls)
	}
	if swapperCalls != 1 {
		t.Errorf("Hotswap func called %d times, expected 0", swapperCalls)
	}
	conn, ok = v.(*mockConn)
	if !ok {
		t.Errorf("Connect did not return a mockConn, returned %#v", v)
	} else if conn.def != true {
		t.Errorf("new Conn made by old Collector, expected it to be made by new Connector (with mockConn.def=true)")
	}
}

func TestHotswapWithBlocker(t *testing.T) {
	setup()

	// To make other conns block during the hot swap, we make the hot swap func block
	swappingChan := make(chan struct{})
	blockChan := make(chan struct{})
	SetHotswapFunc(func(ctx context.Context, currentDSN string) string {
		close(swappingChan)
		<-blockChan
		return "user3:pass3@tcp(localhost)/"
	})

	connsChan := make(chan driver.Conn, 2)

	// This Connector is called once and causes the hot swap that blocks.
	// It's never called again; a new Connector is created.
	mct1 := &mockConnector{
		connectFunc: func(context.Context) (driver.Conn, error) {
			return nil, accessDeniedErr
		},
	}

	dsn1 := "user:pass@tcp(127.0.0.1)/"
	c := NewConnector(dsn1, mct1)

	// This goroutine becomes the hot swapper. To ensure it runs first and calls
	// the hot swap func, we wait for it to close swappingChan...
	conn1Chan := make(chan struct{})
	go func() {
		defer close(conn1Chan)
		conn, _ := c.Connect(context.Background())
		connsChan <- conn
	}()
	<-swappingChan
	time.Sleep(20 * time.Millisecond)

	// Once swappingChan is closed, we know that ^ goroutine is running and blocked
	// on blockChan. So start another conn attempt which should block on the first.
	conn2Chan := make(chan struct{})
	go func() {
		defer close(conn2Chan)
		conn, _ := c.Connect(context.Background())
		// Wait for conn1 to send and return. This only orders gotConns below,
		// not any func calls because we're waiting here in the test, not anywhere
		// inside the connector.
		<-conn1Chan
		connsChan <- conn
	}()

	// After we close blockChan, the first conn/groutine creates a new Connector and
	// calls its Connect method, getting a new new (mock) Conn. When it returns,
	// it closes an internal chan which unblocks the second conn/goroutine which
	// should call Connect on the new Connector the first one just created. We test
	// this below with mockConnector.calls == 2.
	close(blockChan)
	<-conn1Chan
	<-conn2Chan

	if mct1.calls != 2 {
		t.Errorf("Connect called %d times, expected 2", mct1.calls)
	}

	gotConns := []driver.Conn{}
CONNS:
	for {
		select {
		case c := <-connsChan:
			gotConns = append(gotConns, c)
		default:
			break CONNS
		}
	}
	if len(gotConns) != 2 {
		t.Errorf("got %d Conn, expected 2", len(gotConns))
	}

	for i, v := range gotConns {
		conn, ok := v.(*mockConn)
		if !ok {
			t.Errorf("Connect did not return a mockConn, returned %#v", v)
			continue
		}
		if conn.def != true {
			t.Errorf("new Conn made by old Collector, expected it to be made by new Connector (with mockConn.def=true)")
		}
		if conn.connNo != i+1 {
			t.Errorf("Conn number %d, expected %d", conn.connNo, i+1)
		}
	}
}
