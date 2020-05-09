package rmq

import (
	"testing"
	"time"

	. "github.com/adjust/gocheck"
)

func TestConnectionSuite(t *testing.T) {
	TestingSuiteT(&ConnectionSuite{}, t)
}

type ConnectionSuite struct{}

func (suite *ConnectionSuite) TestConnection(c *C) {
	connection := NewTestConnection()
	var conn Connection
	c.Check(connection, Implements, &conn)
	c.Check(connection.GetDelivery("things", 0), Equals, "rmq.TestConnection: delivery not found: things[0]")

	queue := connection.OpenQueue("things")
	c.Check(connection.GetDelivery("things", -1), Equals, "rmq.TestConnection: delivery not found: things[-1]")
	c.Check(connection.GetDelivery("things", 0), Equals, "rmq.TestConnection: delivery not found: things[0]")
	c.Check(connection.GetDelivery("things", 1), Equals, "rmq.TestConnection: delivery not found: things[1]")

	c.Check(queue.Publish("bar"), Equals, true)
	c.Check(connection.GetDelivery("things", 0), Equals, "bar")
	c.Check(connection.GetDelivery("things", 1), Equals, "rmq.TestConnection: delivery not found: things[1]")
	c.Check(connection.GetDelivery("things", 2), Equals, "rmq.TestConnection: delivery not found: things[2]")

	c.Check(queue.Publish("foo"), Equals, true)
	c.Check(connection.GetDelivery("things", 0), Equals, "bar")
	c.Check(connection.GetDelivery("things", 1), Equals, "foo")
	c.Check(connection.GetDelivery("things", 2), Equals, "rmq.TestConnection: delivery not found: things[2]")

	connection.Reset()
	c.Check(connection.GetDelivery("things", 0), Equals, "rmq.TestConnection: delivery not found: things[0]")

	c.Check(queue.Publish("blab"), Equals, true)
	c.Check(connection.GetDelivery("things", 0), Equals, "blab")
	c.Check(connection.GetDelivery("things", 1), Equals, "rmq.TestConnection: delivery not found: things[1]")
}

func TestConnectionDie(t *testing.T) {
	conn := OpenConnection("test-conn-die-1", "tcp", "localhost:6379", 0)
	q1 := conn.OpenQueue("q1")
	q1.StartConsuming(20, time.Second)
	q1.AddConsumerFunc("q1Consumer", func(delivery Delivery) {
	})

	q1.Publish("m1")
	q1.Publish("m2")
	q1.Publish("m3")

	time.Sleep(5 * time.Second)
	q1.StopConsuming()
	conn.StopHeartbeat()

	time.Sleep(1 * time.Minute)

	q1 = conn.OpenQueue("q1")
	q1.StartConsuming(20, time.Second)
	q1.AddConsumerFunc("q1Consumer", func(delivery Delivery) {
	})

	time.Sleep(10 * time.Second)
}
