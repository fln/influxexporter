package influxexporter

import (
	"fmt"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	client "github.com/influxdata/influxdb/client/v2"
)

// Service holds internal state of Influx exporter service instance. Public
// fields can be changed before service is started.
type Service struct {
	Database          string // name of database to connect to
	BufferSize        int
	MinExportInterval time.Duration
	// if not empty each data point sent to influx will be tagged with
	// "instance" tag having the value of this variable
	Instance string

	c        client.Client
	log      logrus.FieldLogger
	sinkChan chan *client.Point
	wg       sync.WaitGroup
}

// New creates a new buffered influx data exporting service. User of this
// service must start exporter thread using Start() before sending any data.
// Username and password should be provided in the url argument.
func New(url string, database string, log logrus.FieldLogger) (*Service, error) {
	c, err := client.NewHTTPClient(client.HTTPConfig{Addr: url})
	if err != nil {
		return nil, err
	}

	return &Service{
		Database:          database,
		BufferSize:        100,
		MinExportInterval: 2 * time.Second,
		Instance:          "",
		c:                 c,
		log:               log,
	}, err
}

// Start starts data exporting service. This function will run in a seperate
// goroutine until Stop() is called.
//
// It is safe to call this function on nil value.
func (svc *Service) Start() {
	if svc == nil {
		return
	}
	// Block in case instance of this service is already running
	svc.wg.Wait()

	svc.sinkChan = make(chan *client.Point, svc.BufferSize)
	svc.wg.Add(1)
	go func() {
		defer svc.wg.Done()

		hb := time.NewTicker(svc.MinExportInterval)
		defer hb.Stop()

		points := make([]*client.Point, 0, 8)
		for {
			select {
			case <-hb.C:
				if len(points) > 0 {
					svc.batchWrite(points)
					// Allocate new buffer to allow GC to remove old data points
					points = make([]*client.Point, 0, 8)
				}
			case p, ok := <-svc.sinkChan:
				if !ok {
					svc.batchWrite(points)
					return
				}
				points = append(points, p)
			}
		}
	}()
}

func (svc *Service) batchWrite(points []*client.Point) {
	if len(points) == 0 {
		return
	}
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  svc.Database,
		Precision: "s",
	})
	if err != nil {
		svc.log.WithError(err).Error("creating batch points")
	}
	for _, p := range points {
		bp.AddPoint(p)
	}
	err = svc.c.Write(bp)
	if err != nil {
		svc.log.WithError(err).Error("writing batch points")
	}
}

// Stop signals data exporting service to stop waiting for new events and flush
// all buffered events to influx DB. After service is stopped any call to Log()
// will cause a panic. Service can be restarted again with Start().
//
// It is safe to call this function on nil value.
func (svc *Service) Stop() {
	if svc == nil {
		return
	}
	close(svc.sinkChan)
	svc.wg.Wait()
}

// LogDuration sends a new single-field datapoint with a time duration
// measurement. This function should be given a start time of the measurement,
// duration will be computed inside this function by substracting start time
// from the time of this function execution. Calling this function after service
// is stopped will cause panic.
func (svc *Service) LogDuration(measurement string, tags map[string]string, started time.Time) error {
	return svc.Log(measurement, tags,
		map[string]interface{}{
			"value": int64(time.Since(started)),
		},
	)
}

// LogValue sends a new single-field datapoint to influx DB. This function can
// be used concurrently. Calling this function after service is stopped will
// cause panic.
func (svc *Service) LogValue(measurement string, tags map[string]string, value interface{}) error {
	return svc.Log(measurement, tags, map[string]interface{}{"value": value})
}

// Log sends a new datapoint to influx DB. This function can be used
// concurrently. Calling this function after service is stopped will cause
// panic.
//
// It is safe to call this function on nil value.
func (svc *Service) Log(measurement string, tags map[string]string, fields map[string]interface{}) error {
	if svc == nil {
		return nil
	}

	if tags == nil {
		tags = map[string]string{}
	}
	if len(svc.Instance) > 0 {
		tags["instance"] = svc.Instance
	}
	point, err := client.NewPoint(
		measurement,
		tags,
		fields,
		time.Now(),
	)
	if err != nil {
		return err
	}
	select {
	case svc.sinkChan <- point:
		return nil
	default:
		return fmt.Errorf("influxservice: exporter can't keep up, dropping data")
	}
}
