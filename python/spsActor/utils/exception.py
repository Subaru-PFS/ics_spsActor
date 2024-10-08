def factory(className, *args, **kwargs):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""
    if className == "RdaMoveCmd":
        return RdaMoveFailed(*args, **kwargs)
    elif className == "SlitMoveCmd":
        return SlitMoveFailed(*args, **kwargs)
    elif className == "SlitStartCmd":
        return SlitStartFailed(*args, **kwargs)
    elif className == "SlitStopCmd":
        return SlitStopFailed(*args, **kwargs)
    elif className == "BiaCmd":
        return BiaFailed(*args, **kwargs)
    elif className == "IisCmd":
        return IisFailed(*args, **kwargs)
    elif className == "FpaMoveCmd":
        return FpaMotorsFailed(*args, **kwargs)
    elif className == "CcdEraseCmd":
        return EraseFailed(*args, **kwargs)


class SpsException(Exception):
    def __init__(self, subSystem="", reason=""):
        self.subSystem = subSystem
        self.reason = reason
        Exception.__init__(self)

    def __str__(self):
        return f"{self.__class__.__name__}({self.subSystem} with {self.reason})"


class WipeFailed(SpsException):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""


class ReadFailed(SpsException):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""


class EraseFailed(SpsException):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""


class HxRampFailed(SpsException):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""


class ShuttersFailed(SpsException):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""


class RdaMoveFailed(SpsException):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""


class SlitMoveFailed(SpsException):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""


class SlitStartFailed(SpsException):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""


class SlitStopFailed(SpsException):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""


class BiaFailed(SpsException):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""

class IisFailed(SpsException):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""


class FpaMotorsFailed(SpsException):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""


class LampsFailed(SpsException):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""


class ExposureAborted(Exception):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""

    def __str__(self):
        return f'ExposureAborted()'


class EarlyFinish(Exception):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""

    def __str__(self):
        return f'ExposureAborted(doFinish requested before exposing)'

class DiscardExposure(Exception):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""

    def __str__(self):
        return f'ExposureDiscarded()'


class Failures(list):
    def add(self, reason):
        if 'ExposureAborted(' in reason and self.format():
            pass  # something else causes the failure
        else:
            self.append(reason)

    def format(self):
        return ','.join(list(set(self)))
