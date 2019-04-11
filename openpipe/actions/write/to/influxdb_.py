"""
Write data to an InfluxDB instance
"""
import sys
import requests
import dateutil.parser
from pytz import timezone, UTC
from calendar import timegm
from datetime import datetime
from openpipe.pipeline.engine import ActionRuntime


class Action(ActionRuntime):

    required_config = """
    tag_set:            # list of keys to be used as InfluxDB tags
    field_set:          # list of keys to be used as InfluxDB fields
    measurement:        # Name of the measureament
    """

    optional_config = """
    url: http://localhost:8086/ # InfluxDB instance endpoint
    db_name: openpipe           # DB Name
    timestamp_key: timestamp    # Name of input key containing the TS
    time_zone: UTC              # Time zone from the received TS
    time_format: auto           # strftime format of the input
    buffer_size: 1              # Max size for batch loading
    precision: s                # Precision to use on the TS
    """

    def on_start(self, config):
        self.buffer_size = config["buffer_size"]
        self.timezone = timezone(config["time_zone"])
        self.timestamp_field_name = config["timestamp_key"]
        self.url = config["url"]
        self.db_name = config["db_name"]
        self.precision = config["precision"]

        self.data_lines = []
        self.session = requests.Session()

    def on_input(self, item):
        tag_set_list = []
        for tag_name in self.config.get("tag_set", ""):
            tag_set_list.append("%s=%s" % (tag_name, item[tag_name]))
        tag_set = ",".join(tag_set_list)
        field_set_list = []
        skip_line = False
        for field_name in self.config["field_set"]:
            field_value = item.get(field_name, None)
            # Skip lines with values set to None
            # if field_value is None:
            #    skip_line = True
            #    break
            field_set_list.append("%s=%s" % (field_name, field_value))
        if skip_line:
            return
        field_set = ",".join(field_set_list)
        timestamp = self.utc_timestamp(item)
        if tag_set:
            tag_set = "," + tag_set
        data = "%s%s %s %s" % (
            self.config["measurement"],
            tag_set,
            field_set,
            timestamp,
        )
        self.data_lines.append(data)
        if len(self.data_lines) == self.buffer_size:
            self.flush_buffer()

    def flush_buffer(self):
        if len(self.data_lines) == 0:
            return

        url = "%swrite?db=%s&precision=%s" % (self.url, self.db_name, self.precision)
        data = "\n".join(self.data_lines)
        response = self.session.post(
            url, data, headers={"Content-Type": "application/octet-stream"}
        )
        if not response.ok:
            print(response.content, file=sys.stderr)
            response.raise_for_status()
            raise Exception("Unable to insert into influxdb")
        self.data_lines = []

    def utc_timestamp(self, item):
        timestamp_field = self.timestamp_field_name
        if not timestamp_field:
            return ""
        timestamp = item.get(timestamp_field, "")
        if timestamp and isinstance(timestamp, str):
            if self.config["time_format"] == "auto":
                timestamp = dateutil.parser.parse(timestamp)
            else:
                timestamp = datetime.strptime(timestamp, self.config["time_format"])
            if timestamp.tzinfo is None:
                local_timestamp = self.timezone.localize(timestamp)
            else:
                local_timestamp = timestamp
            timestamp = local_timestamp.astimezone(UTC)
            timestamp = timestamp.strftime("%s")
            return timestamp
        if isinstance(timestamp, datetime):
            timestamp = timegm(timestamp.utctimetuple())
        return timestamp

    def on_finish(self, reason):
        self.flush_buffer()
