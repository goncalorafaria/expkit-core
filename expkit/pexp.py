from expkit.exp import Exp
from expkit.ops import *
from expkit.storage import Storage
from typing import *


class PExp(Exp):
    """
    Represents a parameterized experiment.

    Attributes:
        ops (Dict[str, Operation]): A dictionary of operations associated with the experiment.
        ops_results (Dict[str, Any]): A dictionary to store the results of the operations.

    Methods:
        __init__(self, ops: Dict[str, Operation], **exp_args): Initializes a new instance of the PExp class.
        run_ops(self): Executes the operations associated with the experiment.
        get(self, key): Retrieves the value associated with the specified key.
        load(base_path, experiment_name, ops: Dict[str, Operation]): Loads a parameterized experiment from a file.

    """

    ops: Dict[str, Operation]
    ops_results: Dict[str, Any]

    def __init__(
        self,
        ops: Dict[str, Operation],
        **exp_args,
    ):
        """
        Initializes a new instance of the PExp class.

        Args:
            ops (Dict[str, Operation]): A dictionary of operations associated with the experiment.
            **exp_args: Additional arguments to be passed to the base class constructor.

        """
        self.ops = ops
        self.ops_results = {}

        super().__init__(
            **exp_args,
        )

    def __deepcopy__(self, memo):

        e = PExp(
            name=self.name,
            meta=copy.deepcopy(self.meta, memo),
            storage=copy.deepcopy(self.document_storage.storage()),
            ops=copy.deepcopy(self.ops),
        )

        return e

    def run_ops(self):
        """
        Executes the operations associated with the experiment.

        """

        self.ops_results = {}
        for key, op in self.ops.items():
            try:
                self.ops_results[key] = op(self)
            except Exception as e:
                pass
        return self

    def get(self, key):
        """
        Retrieves the value associated with the specified key.

        Args:
            key (str): The key to retrieve the value for.

        Returns:
            Any: The value associated with the specified key.

        Raises:
            ValueError: If the key is not found in the experiment or the operations results.

        """
        try:
            return super().get(key)
        except:
            if key in self.ops_results:
                return self.ops_results[key]
            else:
                raise ValueError(f"key : {key} not found")

    @staticmethod
    def load(
        storage: Storage,
        name: str,
        ops: Dict[str, Operation],
        **kwargs,
    ):
        """
        Loads a parameterized experiment from a file.

        Args:
            base_path (str): The base path of the experiment.
            experiment_name (str): The name of the experiment.
            ops (Dict[str, Operation]): A dictionary of operations associated with the experiment.

        Returns:
            PExp: The loaded parameterized experiment.

        """

        pexp = PExp(
            name=name,
            meta=None,
            storage=storage,
            ops=ops,
            **kwargs,
        )

        return pexp
