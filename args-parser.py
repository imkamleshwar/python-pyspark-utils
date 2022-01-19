from argparse import Namespace

from args_parser_utils_demo import CustomLogger
from args_parser_utils_demo import ParserUtils


class Employee(CustomLogger):
    class_name = __qualname__

    def __init__(self, args: Namespace):
        super().__init__(self.class_name)
        self.name = args.name
        self.age = args.age
        self.salary = args.salary

    def log_employee_details(self):
        self.logger.info("Name of Employee: " + str(self.name))
        self.logger.info("Age of Employee: " + str(self.age))
        self.logger.info("Salary of Employee: " + str(self.salary))


if __name__ == "__main__":
    emp_args = ParserUtils.parse_employee_details()
    emp = Employee(emp_args)
    emp.log_employee_details()
