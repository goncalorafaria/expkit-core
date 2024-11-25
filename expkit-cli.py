from expkit.setup import ExpSetup

from expkit.storage import DiskStorage

from qflow.utils.eval import *
import json

# python expkit-cli.py --base_dir ~/quest-rlhf/gemma-outputs/ --mode clean

# python expkit-cli.py --base_dir ~/quest-rlhf/gemma-outputs/ --mode list --query_args {"n":64}


def main(
    base_dir="outputs/",
    mode="list",
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

    elif mode == "clean":

        setup = ExpSetup(storage=DiskStorage(base_dir=base_dir, mode="r")).query(
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
        raise ValueError("Invalid mode - available options : {list,clean,count}")

        # r[e.meta["model_path"]] = 1

    # for k in r.keys():
    #    print(k)


if __name__ == "__main__":

    import fire

    fire.Fire(main)

# python expkit-cli.py --base_dir ../quest-rlhf/gemma-outputs/ --query_args '{"steps":1,"variant":"ancestral","model_path": "/gscratch/ark/graf/LLaMA-Factory/saves/gemma-2-2b-it/full/dposft/"}'
