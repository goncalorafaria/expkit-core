from dataclasses import dataclass
from enum import Enum

from typing import *


class OperationType(Enum):
    DATA = 1
    EVAL = 2
    EXP = 3


@dataclass
class Operation:
    """
    Represents an operation that can be performed on an `Exp` object.
    """

    type: OperationType
    func: Callable
    key: Optional[str] = None

    @staticmethod
    def data(func):
        """
        Creates an operation of type `DATA` that applies the given function to the instances of an `Exp` object.

        Args:
            func (Callable): The function to be applied to the instances.

        Returns:
            Operation: The created operation.
        """
        return Operation(type=OperationType.DATA, func=func)

    @staticmethod
    def eval(func, key: str):
        """
        Creates an operation of type `EVAL` that applies the given function to the evaluation result of a specific key in an `Exp` object.

        Args:
            func (Callable): The function to be applied to the evaluation result.
            key (str): The key of the evaluation result.

        Returns:
            Operation: The created operation.
        """
        return Operation(type=OperationType.EVAL, func=func, key=key)

    @staticmethod
    def exp(func):
        """
        Creates an operation of type `EXP` that applies the given function to an `Exp` object.

        Args:
            func (Callable): The function to be applied to the `Exp` object.

        Returns:
            Operation: The created operation.
        """
        return Operation(type=OperationType.EXP, func=func)

    def __call__(self, exp):
        """
        Calls the operation on the given `Exp` object.

        Args:
            exp (Exp): The `Exp` object to apply the operation on.

        Returns:
            Any: The result of the operation.

        Raises:
            ValueError: If the operation type is not recognized.
        """

        if self.type == OperationType.DATA:
            return self.func(exp.instances)
        elif self.type == OperationType.EVAL:
            return self.func(exp.evals[self.key])
        elif self.type == OperationType.EXP:
            return self.func(exp)
        else:
            raise ValueError(f"Operation type {self.type} not recognized")
