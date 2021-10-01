class ExposureException(Exception):
    def __init__(self, subSystem="", reason=""):
        self.subSystem = subSystem
        self.reason = reason
        Exception.__init__(self)

    def __str__(self):
        return f"{self.__class__.__name__}({self.subSystem} with {self.reason})"


class WipeFailed(ExposureException):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""


class ReadFailed(ExposureException):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""


class ShuttersFailed(ExposureException):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""


class LampsFailed(ExposureException):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""


class ExposureAborted(Exception):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""

    def __str__(self):
        return f'ExposureAborted()'


class EarlyFinish(Exception):
    """Exception raised when exposure is just trash and needs to be cleared ASAP."""

    def __str__(self):
        return f'ExposureAborted(doFinish requested before exposing)'


class Failures(list):
    def add(self, reason):
        if 'ExposureAborted(' in reason and self.format():
            pass  # something else causes the failure
        else:
            self.append(reason)

    def format(self):
        return ','.join(list(set(self)))
