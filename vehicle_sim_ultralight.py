import logging,time,sys,random,json,datetime
import paho.mqtt.client as mqtt
from threading import Lock
from time import sleep

logging.basicConfig(level=logging.DEBUG,
                    format='%(name)s: %(message)s',)
logger = logging.getLogger('DeviceStateUpdater')
logger.setLevel(logging.INFO)


class VehicleSimulatorUltralight(mqtt.Client):

    def __init__(self, settings, device_id, keepalive=60):
        logger.info('initializing...')
        self._settings = settings
        self._keepalive = keepalive
        self.device_id = device_id
        self._vehicle_data_topic = '/ul/4jggokgpepnvsb2uv4s40d59ov/{}/attrs'.format(device_id)

        self._connected = False
        # creating a lock for the above var for thread-safe reasons
        self._lock = Lock()

        super(VehicleSimulatorUltralight, self).__init__()

    def connect(self):
        logger.info('connecting...')
        self.username_pw_set(self._settings['mqtt_broker_username'], self._settings['mqtt_broker_password'])
        super(VehicleSimulatorUltralight, self).connect(self._settings['mqtt_broker_hostname'],
                                                self._settings['mqtt_broker_port'],
                                                keepalive=self._keepalive)
        self.loop_start()

        while True:

            with self._lock:
                if self._connected:
                    logger.info('CONNECTED')
                    break

            sleep(1)

    def disconnect(self):
        logger.info('disconnecting...')
        with self._lock:
            # if already disconnected, don't do anything
            if not self._connected:
                return

        # sleep for 3 secs so we receive TCP acknowledgement for the above message
        sleep(3)
        super(VehicleSimulatorUltralight, self).disconnect()

    # BELOW WE OVERRIDE CALLBACK FUNCTIONS

    def on_connect(self, client, userdata, flags, rc):
        # successful connection
        if rc == 0:
            logger.info('successful connection')
            with self._lock:
                self._connected = True

    def on_disconnect(self, client, userdata, rc):
        logger.info('on_disconnect')
        with self._lock:
            self._connected = False


    def sendData(self):
        slat = 24.734659
        slong = 46.665124
        offset = random.random()
        lat = slat + offset
        offset = random.random()
        long = slong + offset
        vehicle_data ="f|{0}|l|{1},{2}|v|{3}".format(60, lat, long, 140)
        print("vehicle data: ", vehicle_data)
        self.publish(self._vehicle_data_topic, vehicle_data, qos=2)


if __name__ == '__main__':
    with open('config.json') as config_file:
        settings = json.load(config_file)

    vehicle_simulator_ultralight = VehicleSimulatorUltralight(settings, device_id=str(sys.argv[1]))
    vehicle_simulator_ultralight.connect()

    count = 0
    while count < int(sys.argv[2]):
        vehicle_simulator_ultralight.sendData()
        logger.info('sleeping for 5 sec')
        sleep(5)
        count += 1
