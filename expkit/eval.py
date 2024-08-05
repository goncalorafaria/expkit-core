import abc
from expkit.exp import Exp, InstanceEval
from expkit.ops import (
    Operation,
    OperationType,
)
from typing import *
import os

import logging


@abc.abstractmethod
class Evalutor(Operation):

    def __init__(self, eval_name: str):
        super().__init__(
            type=OperationType.EXP,
            func=lambda e: e.call_locked(
                self.apply
            ),
            key=None,
        )
        self.eval_name = eval_name

    def eval(
        self, experiment: Exp
    ) -> List[InstanceEval]:
        raise NotImplementedError

    def apply(self, exp: Exp) -> Exp:

        exp.refresh()

        if self.eval_name in exp.evals:
            logging.info(
                f"Skipping evaluation {self.eval_name} to experiment {exp.get_name()}"
            )
            return exp
        else:
            logging.info(
                f"Starting evaluation {self.eval_name} to experiment {exp.get_name()}"
            )

            exp.add_eval(
                self.eval_name,
                self.eval(exp),
            )

            directory = os.path.dirname(
                exp.save_path
            )
            exp.save(directory)

            logging.info(
                f"Finished evaluation {self.eval_name} to experiment {exp.get_name()}"
            )

            return exp
