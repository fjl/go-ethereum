# discv5 experiment setup

All commands are to be executed from the top-level directory.

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

Logs and analysis outputs will be written to a directory named after the experiment
parameters by default. You can override the this directory name using the `--name` flag.

If you want to re-run the analysis for a past experiment, use the following command

```
python -m python.analyse_logs ./discv5_test_logs/<experiment>`
```
