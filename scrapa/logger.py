import logging


def make_logger(name):
    """ Create two log handlers, one to output info-level ouput to the
    console, the other to store all logging in a JSON file which will
    later be used to generate reports. """

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    fmt = '%(name)s [%(levelname)-8s]: %(message)s'
    formatter = logging.Formatter(fmt)
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    return logger
