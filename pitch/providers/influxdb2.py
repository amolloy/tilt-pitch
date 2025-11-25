from ..models import TiltStatus
from ..abstractions import CloudProviderBase
from ..configuration import PitchConfig
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS, WritePrecision


class InfluxDb2CloudProvider(CloudProviderBase):

    def __init__(self, config: PitchConfig):
        self.config = config
        self.str_name = "InfluxDb2 ({})".format(config.influxdb2_url)
        self.batch = list()

    def __str__(self):
        return self.str_name

    def start(self):
        self.client = InfluxDBClient(
            url=self.config.influxdb2_url,
            token=self.config.influxdb2_token,
            org=self.config.influxdb2_org,
            timeout=self.config.influxdb_timeout_seconds*1000)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def update(self, tilt_status: TiltStatus):
        self.batch.append(self.get_point(tilt_status))
        if len(self.batch) < self.config.influxdb_batch_size:
            print("Batch size not met yet, skipping.")
            return
        print("Sending data to influxdb2")
        max_retries = getattr(self.config, 'influxdb2_retries', 3)
        backoff_seconds = getattr(self.config, 'influxdb2_retry_backoff_seconds', 2)

        for attempt in range(1, max_retries + 1):
            try:
                self.write_api.write(
                    bucket=self.config.influxdb2_bucket,
                    record=self.batch,
                    write_precision=WritePrecision.MS)
                print(f"InfluxDB write succeeded on attempt {attempt}.")
                self.batch.clear()
                break
            except Exception as e:
                print(f"InfluxDB write attempt {attempt} failed: {e}")
                if attempt < max_retries:
                    sleep_time = backoff_seconds * (2 ** (attempt - 1))
                    print(f"Retrying in {sleep_time} seconds...")
                    try:
                        import time
                        time.sleep(sleep_time)
                    except KeyboardInterrupt:
                        print("Retry sleep interrupted by user.")
                        break
                else:
                    print("All InfluxDB write attempts failed — keeping batch for later retry.")

    def enabled(self):
        return (self.config.influxdb2_url)

    def get_point(self, tilt_status: TiltStatus):
        return {
            "measurement": "tilt",
            "tags": {
                "color": tilt_status.color,
                "name": tilt_status.name
            },
            "fields": {
                "temp_fahrenheit": tilt_status.temp_fahrenheit,
                "temp_celsius": tilt_status.temp_celsius,
                "gravity": tilt_status.gravity,
                "degrees_plato": tilt_status.degrees_plato,
                "alcohol_by_volume": tilt_status.alcohol_by_volume,
                "apparent_attenuation": tilt_status.apparent_attenuation
            }
        }
