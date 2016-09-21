influxexporter
==============

[![Godoc](http://img.shields.io/badge/godoc-reference-blue.svg?style=flat)](https://godoc.org/github.com/fln/influxexporter)

This is a tiny helper package for exporting application metrics to influx DB. It
does not perform any data aggregation - each call to log a metrics value will be
recorded.
