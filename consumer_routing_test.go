package aamqp_test

import (
	"context"
	"log"
	"runtime"
	"testing"
	"time"

	"github.com/luexu/aamqp"
)

func maxParallelism() int {
	maxProcs := runtime.GOMAXPROCS(0)
	numCPU := runtime.NumCPU()
	if maxProcs < numCPU {
		return maxProcs
	}
	return numCPU
}
func infoLogHandler(msg []byte) bool {
	log.Println("handler is handling...")
	return true
}
func errorLogHandler(msg []byte) bool {
	log.Println("handler is handling...")
	return true
}
func TestRouting(t *testing.T) {
	uri := "amqp://Aario:Ai@127.0.0.1:5276/"
	ex := aamqp.Exchange{"logs_direct", "direct", true, false, false, false, nil}
	cc := aamqp.ConnectionConfig{
		Timeout: 2 * time.Second,
	}

	consumer := aamqp.NewConsumer(context.Background(), "routing_consumer_1", uri, ex, cc)

	if err := consumer.Connect(); err != nil {
		t.Errorf("AMQP consumer %s connect failed: %s", consumer.Tag, err)
		return
	}

	cp := aamqp.ConsumeParams{true, false, false, false, nil}
	var qos *aamqp.BasicQos
	que := aamqp.Queue{"test_sub_quename", false, false, false, false, nil}
	binding := aamqp.QueueBinding{"info", "logs_direct", false, nil}
	binding2 := aamqp.QueueBinding{"error", "logs_direct", false, nil}
	binding3 := aamqp.QueueBinding{"warning", "logs_direct", false, nil}

	deliveries, err := consumer.ConsumeQueue(cp, que, qos, binding, binding2, binding3)
	if err != nil {
		t.Errorf("AMQP consume queue failed: %s", err)
		return
	}
	consumer.Handle(deliveries, infoLogHandler, maxParallelism(), cp, que, qos, binding, binding2, binding3)

	consumer2 := aamqp.NewConsumer(context.Background(), "routing_consumer_2", uri, ex, cc)

	if err := consumer2.Connect(); err != nil {
		t.Errorf("AMQP consumer2 %s connect failed: %s", consumer2.Tag, err)
		return
	}

	cp2 := aamqp.ConsumeParams{true, false, false, false, nil}
	var qos2 *aamqp.BasicQos
	que2 := aamqp.Queue{"test_sub_quename", false, false, false, false, nil}
	bindingB := aamqp.QueueBinding{"error", "logs_direct", false, nil}

	deliveries2, err := consumer2.ConsumeQueue(cp2, que2, qos2, bindingB)
	if err != nil {
		t.Errorf("AMQP consume queue failed: %s", err)
		return
	}
	consumer2.Handle(deliveries2, errorLogHandler, maxParallelism(), cp2, que2, qos2, bindingB)

}
