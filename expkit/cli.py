from expkit.setup import ExpSetup

from expkit.storage import DiskStorage

from qflow.utils.eval import *
import json

from termcolor import colored

# python expkit-cli.py --base_dir ~/quest-rlhf/gemma-outputs/ --mode clean


# python expkit-cli.py --base_dir ~/quest-rlhf/gemma-outputs/ --mode list --query_args {"n":64}


# expkit data --base_dir llama3.2-outputs/ --query_args '{"dataset":"lighteval/MATH","model_path":"allenai/Llama-3.1-Tulu-3-8B-SFT"}'
# expkit progress --base_dir llama3.2-outputs/ --query_args '{"dataset":"lighteval/MATH","model_path":"allenai/Llama-3.1-Tulu-3-8B-SFT"}'
# expkit progress --base_dir llama3.2-outputs/ --query_args '{"dataset":"openai/gsm8k","model_path":"allenai/Llama-3.1-Tulu-3-8B-SFT"}'
# --query_args '{"dataset":"lighteval/MATH","model_path":"allenai/Llama-3.1-Tulu-3-8B-SFT"}'

# 7d8f08c5-3d14-412e-94c9-a92acb05216a


# 3db1e043-2288-4f89-aa36-c41dcfef4f2a
def main(
    mode="list",
    base_dir="outputs/",
    n: int = 1,
    query_args: str = {},
):

    print(query_args)

    # query_args = json.loads(str(query_args))

    # r = {}

    if mode == "list":
        setup = ExpSetup(storage=DiskStorage(base_dir=base_dir, mode="r")).query(
            query_args
        )

        for e in setup.experiments:
            print(e.name)

    elif mode == "show":
        setup = ExpSetup(storage=DiskStorage(base_dir=base_dir, mode="r")).query(
            query_args
        )

        for e in setup.experiments:
            print("--" * 20)
            print(e)

    elif mode == "progress":
        setup = ExpSetup(storage=DiskStorage(base_dir=base_dir, mode="r")).query(
            query_args
        )

        for e in setup.sort("dataset"):
            print("--" * 20)
            try:
                target, count = e.get("n"), len(e.instances())
                print(
                    f"[{colored(e.get('variant'),'green')}][{colored(e.get('model_path'),'blue')}]{e.name} : {colored(e.get('dataset'),'red')} : {count}/{target} ({count/target*100:.2f}%)"
                )
            except Exception as excp:
                print(f"{e.name} :  ooops ")
                raise excp

    elif mode == "data":

        setup = ExpSetup(storage=DiskStorage(base_dir=base_dir, mode="r")).query(
            query_args
        )

        for e in setup.experiments:
            if e.has_data():
                print("--" * 20)

                data = e.instances()[0]

                print(f"{e.name} : {data['input']['prompt']}")

                print("**" * 5 + "Answer" + "**" * 5)
                print(data["input"]["answer"])

                for j in range(min(n, len(data["outputs"]))):
                    print("**" * 5 + f"{j}:Output" + "**" * 5)

                    print(data["outputs"][j]["text"])
                    print("..." * 20)

    elif mode == "clean":

        setup = ExpSetup(storage=DiskStorage(base_dir=base_dir, mode="rw")).query(
            query_args
        )

        empty = setup.filter(lambda e: not e.has_data())

        for e in empty:
            print(e.name)
            e.document_storage.delete()

    elif mode == "count":
        setup = ExpSetup(storage=DiskStorage(base_dir=base_dir, mode="r")).query(
            query_args
        )

        print(len(setup.experiments))
    else:
        setup = ExpSetup(storage=DiskStorage(base_dir=base_dir, mode="r")).query(
            query_args
        )

        e = setup[mode]

        print("--" * 20)
        print(e)
        print("--" * 20)
        target, count = e.get("n"), len(e.instances())
        print(f"{e.name} : {count}/{target} ({count/target*100:.2f}%)")

        # raise ValueError("Invalid mode - available options : {list,clean,count}")

        # r[e.meta["model_path"]] = 1

    # for k in r.keys():
    #    print(k)


if __name__ == "__main__":

    import fire

    fire.Fire(main)

# python expkit-cli.py --base_dir ../quest-rlhf/gemma-outputs/ --query_args '{"steps":1,"variant":"ancestral","model_path": "/gscratch/ark/graf/LLaMA-Factory/saves/gemma-2-2b-it/full/dposft/"}'


# python m expkit.cli list --base_dir ../quest-rlhf/llama3-outputs/ --query_args '{"model_path":"allenai/Llama-3.1-Tulu-3-8B-SFT"}'
