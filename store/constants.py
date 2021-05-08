from appyratus.enum import Enum


OP_CODE = Enum.of_strings(
    'EQ', 'NE', 'LT', 'GT', 'GE', 'LE', 'IN', 'NOT_IN', 'AND', 'OR'
)