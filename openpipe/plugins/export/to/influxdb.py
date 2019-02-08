import sys
import requests
import pytz
from calendar import timegm
from datetime import datetime
from openpipe.engine import PluginRuntime


class Plugin(PluginRuntime):

    __default_config__ = {
        'url': 'http://localhost:8086/',
        'db_name': 'openpipe',
        'timestamp_field_name': 'timestamp',
        'timestamp_zone': None,
        'buffer_size' : 1,
        'precision' : 's'
    }

    def on_start(self, config):
        self.buffer_size = config['buffer_size']
        self.timezone = config.get('timestamp_zone', None)
        self.timestamp_field_name = config['timestamp_field_name']
        self.url = config['url']
        self.db_name = config['db_name']
        self.precision = config['precision']
        self.data_lines = []
        self.session = requests.Session()


    def on_input(self, item):
        tag_set_list = []
        for tag_name in self.config.get('tag_set', ''):
            tag_set_list.append("%s=%s" % (tag_name, item[tag_name]))
        tag_set = ','.join(tag_set_list)
        field_set_list = []
        skip_line = False
        for field_name in self.config['field_set']:
            field_value = item.get(field_name, None)
            # Skip lines with values set to None
            # if field_value is None:
            #    skip_line = True
            #    break
            field_set_list.append("%s=%s" % (field_name, field_value))
        if skip_line:
            return
        field_set = ','.join(field_set_list)
        timestamp = self.utc_timestamp(item)
        if tag_set:
            tag_set = ',' + tag_set
        data = "%s%s %s %s" % (self.config['measurement'], tag_set, field_set, timestamp)
        self.data_lines.append(data)
        if len(self.data_lines) == self.buffer_size:
            self.flush_buffer()

    def flush_buffer(self):
        if len(self.data_lines) == 0:
            return

        url = "%swrite?db=%s&precision=%s" % (self.url, self.db_name, self.precision)
        data = '\n'.join(self.data_lines)
        response = self.session.post(
            url, data,  headers={'Content-Type': 'application/octet-stream'}
        )
        if not response.ok:
            print(response.content, file=sys.stderr)
            response.raise_for_status()
            raise Exception("Unable to insert into influxdb")
        self.data_lines = []

    def utc_timestamp(self, item):
        timestamp_field = self.timestamp_field_name
        if not timestamp_field:
            return ''
        timestamp = item.get(timestamp_field, '')
        if timestamp and isinstance(timestamp, str):
            timestamp = datetime.strptime(timestamp, self.config['timestamp_format'])
            if self.timezone:
                local_timestamp = self.timezone.localize(timestamp)
                timestamp = local_timestamp.astimezone(pytz.UTC)
            timestamp = timestamp.strftime("%s")
            if self.config.get('timestamp_ms_count'):
                ms = "%03d" % self.counter
                timestamp += ms
                self.counter += 1
                if self.counter > 999:
                    self.counter = 0
            return timestamp
        if isinstance(timestamp, datetime):
            timestamp = timegm(timestamp.utctimetuple())
        return timestamp

    def on_complete(self):
        self.flush_buffer()
