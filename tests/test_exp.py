import os
import json
import unittest
from expkit.exp import Exp


class TestExp(unittest.TestCase):

    def test_create_exp(self):
        # Test creating an instance of Exp
        exp = Exp("TestExp", {"author": "John Doe"})
        self.assertEqual(exp.get_name(), "TestExp")
        self.assertEqual(exp.get("author"), "John Doe")

    def test_add_instance(self):
        # Test adding a single instance to Exp
        exp = Exp("TestExp", {"author": "John Doe"})
        input_data = {"input_1": 1, "input_2": 2}
        output = [{"output_1": 3}, {"output_2": 4}]
        exp.add_instance(input_data, output)
        self.assertEqual(len(exp.instances), 1)
        self.assertEqual(exp.instances[0].input_data, input_data)
        self.assertEqual(exp.instances[0].outputs, output)

    def test_add_instances(self):
        # Test adding multiple instances to Exp
        exp = Exp("TestExp", {"author": "John Doe"})
        inputs = [{"input_1": 1, "input_2": 2}, {"input_3": 3, "input_4": 4}]
        outputs = [
            [{"output_1": 3}, {"output_2": 4}],
            [{"output_3": 5}, {"output_4": 6}],
        ]
        exp.add_instances(inputs, outputs)
        self.assertEqual(len(exp.instances), 2)
        self.assertEqual(exp.instances[0].input_data, inputs[0])
        self.assertEqual(exp.instances[0].outputs, outputs[0])
        self.assertEqual(exp.instances[1].input_data, inputs[1])
        self.assertEqual(exp.instances[1].outputs, outputs[1])

    def test_save_and_load(self):
        base_dir = "data/"
        # Test saving and loading an Exp instance
        exp = Exp(name="test_exp", meta={"author": "John Doe"})
        inputs = [{"input_1": 1, "input_2": 2}, {"input_3": 3, "input_4": 4}]
        outputs = [
            [{"output_1": 3}, {"output_2": 4}],
            [{"output_3": 5}, {"output_4": 6}],
        ]
        exp.add_instances(inputs, outputs)
        exp.add_eval("testeval", [{"metric_1": 1, "metric_2": 2}])
        # Save the Exp instance
        exp.save(base_dir)
        save_path = os.path.join(base_dir, "test_exp")

        # Load the saved Exp instance
        loaded_exp = Exp.load(base_dir, "test_exp")
        self.assertEqual(loaded_exp.get_name(), "test_exp")
        self.assertEqual(loaded_exp.get("author"), "John Doe")
        self.assertEqual(len(loaded_exp.instances), 2)
        self.assertEqual(loaded_exp.instances[0].input_data, inputs[0])
        self.assertEqual(loaded_exp.instances[0].outputs, outputs[0])
        self.assertEqual(loaded_exp.instances[1].input_data, inputs[1])
        self.assertEqual(loaded_exp.instances[1].outputs, outputs[1])

        # Clean up the saved files
        for file_name in os.listdir(save_path):
            file_path = os.path.join(save_path, file_name)
            if os.path.isfile(file_path):
                os.remove(file_path)
        os.rmdir(save_path)


if __name__ == "__main__":
    unittest.main()
