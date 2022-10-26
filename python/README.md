# discv5 experiment setup

### Installing

You need Python >= 3.9.

It is recommended to use virtualenv to keep your Python package collection in check.
Create your environment like this:

```
python3 -m venv .virtualenv
```

Now enter the environment (assuming a bash-like shell). This sets up your PATH and
Python-related environment variables to store everything in `.virtualenv`.

```
source .virtalenv/bin/activate
```

Now install the dependencies:

```
python -m pip install -r python/requirements.txt
```

### Running Experiments

All commands are to be executed from the top-level directory. If you are using virtualenv,
remember to enter the environment first.

You can run the testbed with default settings like this:

```
./run_testbed.py
```

There are several customization options available. You can run a single experiment with
custom configuration using the command below. Check out discv5-stdconfig.json for an
overview of the available parameters.

```
./run_testbed.py --config myconfig.json
```

By default, `run_testbed.py` will perform analysis after running the experiment. This can
be disabled using the `--no-analysis` flag.

Logs and analysis outputs will be written to a subdirectoy of `discv5_test_logs/` named
after the experiment parameters. You can override the name using the `--name` flag.

If you want to re-run analysis for a past experiment, use the following command:

```
python -m python.analyse_logs ./discv5_test_logs/<experiment>
```
