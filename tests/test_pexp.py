import unittest
from expkit.pexp import PExp
from expkit.ops import Operation

import os


class TestPExp(unittest.TestCase):

    def test_init(self):
        ops = {"op1": Operation.data(len), "op2": Operation.data(len)}
        exp = PExp(ops=ops, name="TestExp", meta={"author": "John Doe"})
        self.assertEqual(exp.ops, ops)
        self.assertEqual(exp.ops_results, {})

    def test_run_ops(self):
        ops = {"op1": Operation.data(len), "op2": Operation.data(len)}

        exp = PExp(ops=ops, name="TestExp", meta={"author": "John Doe"})
        inputs = [{"input_1": 1, "input_2": 2}, {"input_3": 3, "input_4": 4}]
        outputs = [
            [{"output_1": 3}, {"output_2": 4}],
            [{"output_3": 5}, {"output_4": 6}],
        ]
        exp.add_instances(inputs, outputs)
        exp.run_ops()

        self.assertEqual(exp.ops, ops)
        self.assertEqual(exp.ops_results, {"op1": 2, "op2": 2})
        # {'op1': 2, 'op2': 2}

        # Add assertions to check the results of the operations

    def test_get(self):
        ops = {"op1": Operation.data(len), "op2": Operation.data(len)}

        exp = PExp(ops=ops, name="TestExp", meta={"author": "John Doe"})
        inputs = [{"input_1": 1, "input_2": 2}, {"input_3": 3, "input_4": 4}]
        outputs = [
            [{"output_1": 3}, {"output_2": 4}],
            [{"output_3": 5}, {"output_4": 6}],
        ]
        exp.add_instances(inputs, outputs)
        exp.run_ops()

        result = exp.get("op1")
        self.assertEqual(result, 2)
        # 2

    def test_save_and_load(self):
        base_dir = "/tmp/"
        ops = {"op1": Operation.data(len), "op2": Operation.data(len)}

        exp = PExp(ops=ops, name="TestExp", meta={"author": "John Doe"})
        inputs = [{"input_1": 1, "input_2": 2}, {"input_3": 3, "input_4": 4}]
        outputs = [
            [{"output_1": 3}, {"output_2": 4}],
            [{"output_3": 5}, {"output_4": 6}],
        ]
        exp.add_instances(inputs, outputs)
        # exp.run_ops()

        # Save the experiment
        exp.save(base_dir)

        # Load the experiment

        loaded_exp = PExp.load(base_dir, "TestExp", ops=ops)

        # Verify that the loaded experiment has the same attributes as the original experiment
        self.assertEqual(loaded_exp.ops, exp.ops)
        # self.assertEqual(loaded_exp.ops_results, exp.ops_results)
        self.assertEqual(loaded_exp.instances, exp.instances)
        self.assertEqual(loaded_exp.name, exp.name)
        self.assertEqual(loaded_exp.meta, exp.meta)

        save_path = os.path.join(base_dir, "TestExp")

        # Clean up the saved files
        for file_name in os.listdir(save_path):
            file_path = os.path.join(save_path, file_name)
            if os.path.isfile(file_path):
                os.remove(file_path)
        os.rmdir(save_path)


if __name__ == "__main__":
    unittest.main()
