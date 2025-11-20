from ..models import TiltStatus
from abc import ABC, abstractmethod

class CloudProviderBase(ABC):

    def start(self):
        pass

    def update(self, tilt_status: TiltStatus):
        pass

    def enabled(self):
        return False