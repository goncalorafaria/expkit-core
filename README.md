# ExpKit

ExpKit is a flexible experiment management framework for machine learning projects, providing easy storage, retrieval, and evaluation of experimental results.

## Features

- Multiple storage backends (Disk, Zip, Memory, Mongo)
- Caching support with ROCache
- Flexible metadata management
- Batch processing of inputs/outputs
- Evaluation metrics computation
- Query interface for experiment retrieval

## Installation

```bash
pip install expkit-core
```

## Quick Start

### Creating an Experiment

```python
from expkit import Exp, DiskStorage

# Configure experiment metadata
meta = {
    "steps": 1000,
    "temperature": 0.7,
    "n": 5,
    "model_path": "path/to/model",
    "batch_size": 32,
}

# Create experiment with disk storage
experiment = Exp(
    storage=DiskStorage(save_path, "rw"),
    meta=meta
)
```

### Adding Data

```python
# Add input/output instances
experiment.add_instances(
    inputs=["input1", "input2", "input3"],
    outputs=["output1", "output2", "output3"]
)

# Add evaluation scores
experiment.add_eval("accuracy", [0.85, 0.92, 0.88])
```

### Loading and Querying Experiments

```python
from expkit import ZipStorage, ExpSetup

# Initialize storage
storage = ZipStorage(base_dir="path/to/outputs", mode="r")

# Create experiment setup
setup = ExpSetup(storage=storage)

# Query experiments
results = setup.query({
   "split":"test",
})
```

### Working with Results

```python
# Access experiment metadata
model_path = exp.get("model_path")

# Get instances
data = exp.instances()

# Get evaluation results
values = exp.get_eval(key)
```

## Storage Backends

ExpKit supports multiple storage backends:

- **DiskStorage**: Stores experiments as files on disk
- **ZipStorage**: Stores experiments in zip archives
- **MongoStorage**: Stores experiments in mongo server
- **MemoryStorage**: Keeps experiments in memory
- **ROCache**: Caching layer that can wrap other storage backends

## Evaluation Operations

You can define custom evaluation operations:

```python
setup = ExpSetup(
    storage=storage,
    ops={
        "mean-reward-1": EvalTotalMean(
            entry_key="scores",
            eval_key=eval_key,
            n=1
        )
    }
)

# Run evaluation operations
setup.run_ops()


# Access results
first_exp_reward = setup[0].get("mean-reward-1")

# Transform experiments
mapped_results = setup.map(lambda exp: {
    "reward": exp.get("mean-reward-1"),
    "model": exp.get("model_path")
})

# Filter experiments
filtered = setup.filter(lambda exp: exp.get("temperature") > 0.5)

# Sort experiments
sorted_exps = setup.sort("temperature")  # ascending order

```

## Contributing

Contributions are welcome! Please read our contributing guidelines and submit pull requests to our repository.
