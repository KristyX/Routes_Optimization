class Error(Exception):
    def __init__(self, message):
        self.message = message


class ObjectionRangeError(Error):
    pass


class OptparaRangeError(Error):
    pass


class NoDriverError(Error):
    pass


class NoOrderError(Error):
    pass


class TravelTimeError(Error):
    pass


class NoSolutionError(Error):
    pass
