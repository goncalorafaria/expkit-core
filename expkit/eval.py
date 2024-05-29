import abc
from expkit.exp import Exp, InstanceEval
from typing import *


@abc.abstractmethod
class Evalutor:

    def eval(self, experiment: Exp) -> List[InstanceEval]:
        raise NotImplementedError

    def apply(self, exp: Exp, key: str) -> Exp:

        evals = self.eval(exp)
        exp.add_eval(key, evals)

        return exp
