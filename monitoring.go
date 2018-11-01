package kafpc

import (
	"github.com/prometheus/client_golang/prometheus"
)

const servicename = "kafpc"

var ReqCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Subsystem: servicename,
		Name:      "requests_total",
		Help:      "number of kafpc jobs",
	},
	[]string{"service", "path"},
)

var RepCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Subsystem: servicename,
		Name:      "replies_total",
		Help:      "number of kafpc reply",
	},
	[]string{"service", "path", "success"},
)

var ProcessDuration = prometheus.NewSummaryVec(
	prometheus.SummaryOpts{
		Subsystem: servicename,
		Name:      "processing_duration",
		Help:      "The process time in seconds.",
	},
	[]string{"service", "path", "success"},
)

var TotalDuration = prometheus.NewSummaryVec(
	prometheus.SummaryOpts{
		Subsystem: servicename,
		Name:      "total_duration",
		Help:      "Kafpc lag in time",
	},
	[]string{"service", "path"},
)

var LagQueueDuration = prometheus.NewSummaryVec(
	prometheus.SummaryOpts{
		Subsystem: servicename,
		Name:      "lag_queue_duration",
		Help:      "Kafpc lag queue time",
	},
	[]string{"service", "path"},
)

func init() {
	prometheus.MustRegister(ReqCounter)
	prometheus.MustRegister(RepCounter)
	prometheus.MustRegister(ProcessDuration)
	prometheus.MustRegister(TotalDuration)
	prometheus.MustRegister(LagQueueDuration)
}
