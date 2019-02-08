# influxdb

This [openpipe] plugin provides integration with [InfluxDB] for time series data insertion.

---

[openpipe]: https://www.openpipe.org/
[InfluxDB]: https://www.influxdata.com/

## Features
The following features are supported:

- Data insertion into an InfluxDB instance:
    - Bulk data upload for improved performance
    - Custom data time format using [strfime]
    - Explicit timezone setting when the data is timezone unaware

[strfime]: http://strftime.org/

## How to test (Linux Example)

```bash
# Download and run InfluxDB
wget https://dl.influxdata.com/influxdb/releases/influxdb-1.7.3_linux_amd64.tar.gz
tar xzvf influxdb-1.7.3_linux_amd64.tar.gz
./influxdb-1.7.3-1/usr/bin/influxd

# Open a new terminal while keeping the influxd running

# Create the DB
influxdb-1.7.3-1/usr/bin/influx -execute 'create database openpipe'

# Clone this repo, and run the test
git clone https://github.com/openpipe-plugins/influxdb
openpipe run test_pipeline.yaml

# Check that the data was inserted as expected
influxdb-1.7.3-1/usr/bin/influx -precision rfc3339  -database openpipe -execute 'select * from logs'
```
## How to use

Include the library into your pipeline, and configure the export step:
```yaml
libraries:
    - https://github.com/openpipe-plugins/influxdb

# Adjust the configuration for your pipeline data format and influxdb instance
    - export to influxdb:
        url: 'http://localhost:8086/'
        db_name': 'openpipe'
        measurement: 'logs'
        buffer_size' : 100,
        tag_set: [ hostname ]
        field_set: [ hits ]
        timestamp_field_name: timestamp
        timestamp_format: "%d/%m/%Y %H:%M:%S"
```