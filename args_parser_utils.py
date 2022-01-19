import argparse
import logging
import sys


class ParserUtils:
    @staticmethod
    def parse_employee_details() -> argparse.Namespace:
        parser = argparse.ArgumentParser(description='Parsing employee details')
        parser.add_argument("--name", type=str, required=True, help="Please provide the name of Employee")
        parser.add_argument("--age", type=int, required=True, help="Please provide the age of Employee")
        parser.add_argument("--salary", type=float, required=True, help="Please provide the salary of Employee")

        return parser.parse_args()


class CustomLogger:
    def __init__(self, class_name=None):
        self.logger = self.init_logger(class_name)

    @staticmethod
    def init_logger(class_name=None):
        if not class_name:
            class_name = __name__
        _logger = logging.getLogger(class_name)
        _logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s %(name)s %(module)s %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S')

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        handler.setFormatter(formatter)

        _logger.addHandler(handler)

        return logging.LoggerAdapter(logging.getLogger(class_name), None)
