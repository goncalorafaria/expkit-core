from expkit.setup import ExpSetup
from expkit.ops import EvalMeanOperation

es = ExpSetup(
    "data/",
    ops={
        "da-mean": EvalMeanOperation(entry_key="mean_reward", eval_key="da"),
        "xcomet-mean": EvalMeanOperation(entry_key="mean_reward", eval_key="xcomet"),
    },
)

print(es[0].get("da-mean"))
print(es[0].get("xcomet-mean"))
print(es.meta())


print(es.query({"method": "mcmc", "beta": 0.1}).get_and_stack("da-mean"))
print(es.query({"method": "mcmc", "beta": 0.1}).get_and_stack("xcomet-mean"))
es.query({"method": "mcmc", "beta": 0.1}).print_get_table("xcomet-mean")
