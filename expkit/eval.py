import abc
from expkit.exp import Exp, InstanceEval
from expkit.ops import Operation, OperationType
from typing import *


@abc.abstractmethod
class Evalutor(Operation):

    def __init__(self, eval_name: str):
        super().__init__(type=OperationType.EXP, func=self.apply, key=None)
        self.eval_name = eval_name

    def eval(self, experiment: Exp) -> List[InstanceEval]:
        raise NotImplementedError

    def apply(self, exp: Exp) -> Exp:

        evals = self.eval(exp)
        
        exp.add_eval(self.eval_name, evals)

        return exp


