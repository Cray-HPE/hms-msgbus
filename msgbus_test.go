// MIT License
//
// (C) Copyright [2021] Hewlett Packard Enterprise Development LP
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
// THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
// OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.

package msgbus

import (
	"testing"
	"time"
	"github.com/sirupsen/logrus"
)

const CBFUNC_TEST_MSG = "CB Function Test Message"
const BLK_READ_MSG = "Blocking Read Message"

// Place for cbfunc to store incoming messages so they can be checked
var cbfunc_message string

// Callback func to use with cbfunc registration

func cbfunc(msg string) {
	cbfunc_message = msg
}

// All msgbus functions are checked from this function.  This kills 2 birds
// with one stone -- it's a set of unit tests, but also it tests that
// the individual functions work together, so it's also an integration
// test of sorts.

func TestConnect(t *testing.T) {
	var mcfg MsgBusConfig
	var mbusW MsgBusIO
	var mbusR MsgBusIO
	var msg string
	var err error

	t.Logf("** RUNNING MSG BUS TESTS\n")

	__testmode = true

	// First connect as a writer, blocking, happy path, test sending
	// a message

	t.Logf("** Opening a blocking writer **\n")
	mcfg.BusTech = BusTechKafka
	mcfg.Host = "localhost"
	mcfg.Port = 9092
	mcfg.Blocking = Blocking
	mcfg.Direction = BusWriter
	mcfg.ConnectRetries = 2
	mcfg.Topic = "hb_events_wb"

	mbusW, err = Connect(mcfg)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("** Sending message using a blocking writer\n")
	err = mbusW.MessageWrite("the rain in spain")
	if err != nil {
		t.Fatal(err)
	}

	//Quick logger test

	t.Log("Should see 'TestMode: Sending message'")
	mbusW.MessageWrite("the rain in spain")
	locLogger := logrus.New()
	SetLogger(locLogger)
	t.Log("Should see 'TestMode: Sending message' again")
	mbusW.MessageWrite("falls mainly on the plain")

	mbusW.Disconnect()

	// Connect as a writer, non-blocking, happy path, test sending
	// a message

	t.Logf("** Opening a non-blocking writer **\n")
	mcfg.BusTech = BusTechKafka
	mcfg.Host = "localhost"
	mcfg.Port = 9092
	mcfg.Blocking = NonBlocking
	mcfg.Direction = BusWriter
	mcfg.ConnectRetries = 2
	mcfg.Topic = "hb_events_wnb"

	mbusW, err = Connect(mcfg)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("** Sending message using a non-blocking writer\n")
	err = mbusW.MessageWrite("fell mainly on the plain")
	if err != nil {
		t.Fatal(err)
	}
	mbusW.Disconnect()

	//Connect as a reader, blocking, happy path, test receiving a message

	t.Logf("** Opening a blocking reader **\n")
	mcfg.BusTech = BusTechKafka
	mcfg.Host = "localhost"
	mcfg.Port = 9092
	mcfg.Blocking = Blocking
	mcfg.Direction = BusReader
	mcfg.ConnectRetries = 2
	mcfg.Topic = "hb_events_rb"

	__read_inject = BLK_READ_MSG

	mbusR, err = Connect(mcfg)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second / 2)

	t.Logf("** Receiving message using a blocking reader\n")
	if mbusR.MessageAvailable() == 0 {
		t.Fatal("Non-blocking read, message available check failed.")
	}
	msg, err = mbusR.MessageRead()
	if err != nil {
		t.Fatal(err)
	}
	if msg != BLK_READ_MSG {
		t.Fatal("Unexpected message received.")
	}
	mbusR.Disconnect()

	//Connect as a reader, cbfunc, happy path, test receiving a message

	t.Logf("** Opening a cbfunc reader **\n")
	mcfg.BusTech = BusTechKafka
	mcfg.Host = "localhost"
	mcfg.Port = 9092
	mcfg.Blocking = NonBlocking
	mcfg.Direction = BusReader
	mcfg.ConnectRetries = 2
	mcfg.Topic = "hb_events_rb"

	__read_inject = CBFUNC_TEST_MSG
	__testmode = true

	mbusR, err = Connect(mcfg)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second / 2)

	err = mbusR.RegisterCB(cbfunc)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second / 2)

	if cbfunc_message != CBFUNC_TEST_MSG {
		t.Fatal("CB function didn't get the correct message.")
	}
	time.Sleep(time.Second / 2)

	//Try to call MessageRead -- should fail since we have a CB registered

	_, err = mbusR.MessageRead()
	if err == nil {
		t.Fatal("MessageRead() call succeeded, shouldn't have, CB registered.")
	}

	mbusR.Disconnect()

	//Connect as reader, register a callback func,

	//Connect as a writer, but test the failure cases

	t.Logf("** Testing Connect Function Error Conditions **\n")

	mcfg.BusTech = 0
	mbusW, err = Connect(mcfg)
	if err == nil {
		t.Fatal(err)
	}

	mcfg.BusTech = BusTechKafka
	mcfg.Topic = ""
	mbusW, err = Connect(mcfg)
	if err == nil {
		t.Fatal(err)
	}

	mcfg.Topic = "TestTopic"
	mcfg.Direction = 0
	mbusW, err = Connect(mcfg)
	if err == nil {
		t.Fatal(err)
	}

	//Call all of the "illegal" functions, insure they fail.

	t.Logf("** Testing illegal writer functions **\n")
	__testmode = true
	mcfg.BusTech = BusTechKafka
	mcfg.Host = "localhost"
	mcfg.Port = 9092
	mcfg.Blocking = Blocking
	mcfg.Direction = BusWriter
	mcfg.ConnectRetries = 2
	mcfg.Topic = "hb_events_wb"

	mbusW, err = Connect(mcfg)
	if err != nil {
		t.Fatal(err)
	}

	_, merr := mbusW.MessageRead()
	if merr == nil {
		t.Fatal("ERROR: writer calling MessageRead() succeeded, should have failed.")
	}
	mint := mbusW.MessageAvailable()
	if mint != 0 {
		t.Fatal("ERROR: writer calling MessageAvailable() said message was ready.")
	}
	merr = mbusW.RegisterCB(cbfunc)
	if merr == nil {
		t.Fatal("ERROR: writer allowed CB function registration, shouldn't have.")
	}
	merr = mbusW.UnregisterCB()
	if merr == nil {
		t.Fatal("ERROR: writer allowed CB function unregistration, shouldn't have.")
	}
	mbusW.Disconnect()

	mcfg.BusTech = BusTechKafka
	mcfg.Host = "localhost"
	mcfg.Port = 9092
	mcfg.Blocking = Blocking
	mcfg.Direction = BusReader
	mcfg.ConnectRetries = 2
	mcfg.Topic = "hb_events_rb"

	mbusR, err = Connect(mcfg)
	if err != nil {
		t.Fatal(err)
	}

	merr = mbusR.MessageWrite("hello")
	if merr == nil {
		t.Fatal("ERROR: reader allowed MessageWrite(), shouldn't have.")
	}

	mbusR.Disconnect()

}

