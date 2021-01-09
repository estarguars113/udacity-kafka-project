"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)

DEFAULT_TEMPERATURE = 70.0
DEFAULT_STATUS = "sunny"
class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = DEFAULT_TEMPERATURE
        self.status = DEFAULT_STATUS

    def process_message(self, message):
        """Handles incoming weather data"""
        if 'arrivals' in message.topic():
            info = message.value()
            self.temperature = info.get('temperature', DEFAULT_TEMPERATURE)
            self.status = info.get('status', DEFAULT_STATUS)
