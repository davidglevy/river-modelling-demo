# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction to NeuralHydrology
# MAGIC
# MAGIC **Before we start**
# MAGIC
# MAGIC - This tutorial is rendered from a Jupyter notebook that is hosted on GitHub. If you want to run the code yourself, you can find the notebook and configuration files [here](https://github.com/neuralhydrology/neuralhydrology/tree/master/examples/01-Introduction).
# MAGIC - To be able to run this notebook locally, you need to download the publicly available CAMELS US rainfall-runoff dataset. See the [Data Prerequisites Tutorial](https://raw.githubusercontent.com/neuralhydrology/neuralhydrology/master/examples/01-Introduction/data-prerequisites.nblink) for a detailed description on where to download the data and how to structure your local dataset folder. You will also need to follow the [installation instructions](https://neuralhydrology.readthedocs.io/en/latest/usage/quickstart.html#installation) (the easiest option if you don't plan to implement your own models/datasets is `pip install neuralhydrology`; for other options refer to the installation instructions).
# MAGIC
# MAGIC The Python package NeuralHydrology was was developed with a strong focus on research. The main application area is hydrology, however, in principle the code can be used with any data. To allow fast iteration of research ideas, we tried to develop the package as modular as possible so that new models, new data sets, new loss functions, new regularizations, new metrics etc. can be integrated with minor effort.
# MAGIC
# MAGIC There are two different ways to use this package:
# MAGIC
# MAGIC 1. From the terminal, making use of some high-level entry points (such as `nh-run` and `nh-run-scheduler`)
# MAGIC 2. From any other Python file or Jupyter Notebook, using NeuralHydrology's API
# MAGIC
# MAGIC In this tutorial, we will give a very short overview of the two different modes.
# MAGIC
# MAGIC Both approaches require a **configuration file**. These are `.yml` files which define the entire run configuration (such as data set, basins, data periods, model specifications, etc.). A full list of config arguments is listed in the [documentation](https://neuralhydrology.readthedocs.io/en/latest/usage/config.html) and we highly recommend to check this page and read the documentation carefully. There is a lot that you can do with this Python package and we can't cover everything in tutorials.
# MAGIC
# MAGIC For every run that you start, a new folder will be created. This folder is used to store the model and optimizer checkpoints, train data means/stds (needed for scaling during inference), tensorboard log file (can be used to monitor and compare training runs visually), validation results (optionally) and training progress figures (optionally, e.g., model predictions and observations for _n_ random basins). During inference, the evaluation results will also be stored in this directory (e.g., test period results).
# MAGIC
# MAGIC
# MAGIC ### TensorBoard logging
# MAGIC By default, the training progress is logged in TensorBoard files (add `log_tensorboard: False` to the config to disable TensorBoard logging). If you installed a Python environment from one of our environment files, you have TensorBoard already installed. If not, you can install TensorBoard with:
# MAGIC
# MAGIC ```
# MAGIC pip install tensorboard
# MAGIC ``` 
# MAGIC
# MAGIC To start the TensorBoard dashboard, run:
# MAGIC
# MAGIC ```
# MAGIC tensorboard --logdir /path/to/run-dir
# MAGIC ```
# MAGIC
# MAGIC You can also visualize multiple runs at once if you point the `--logdir` to the parent directory (useful for model intercomparison)
# MAGIC
# MAGIC ### File logging
# MAGIC In addition to TensorBoard, you will always find a file called `output.log` in the run directory. This file is a dump of the console output you see during training and evaluation.
# MAGIC
# MAGIC
# MAGIC ## Using NeuralHydrology from the Terminal
# MAGIC
# MAGIC ### nh-run
# MAGIC
# MAGIC
# MAGIC Given a run configuration file, you can use the bash command `nh-run` to train/evaluate a model. To train a model, use
# MAGIC
# MAGIC
# MAGIC ```bash
# MAGIC nh-run train --config-file path/to/config.yml
# MAGIC ```
# MAGIC
# MAGIC to evaluate the model after training, use
# MAGIC
# MAGIC ```bash
# MAGIC nh-run evaluate --run-dir path/to/run-directory
# MAGIC ```
# MAGIC
# MAGIC ### nh-run-scheduler
# MAGIC
# MAGIC If you want to train/evaluate multiple models on different GPUs, you can use the `nh-run-scheduler`. This tool automatically distributes runs across GPUs and starts a new one, whenever one run finishes.
# MAGIC
# MAGIC Calling `nh-run-scheduler` in `train` mode will train one model for each `.yml` file in a directory (or its sub-directories).
# MAGIC
# MAGIC ```bash
# MAGIC nh-run-scheduler train --directory /path/to/config-dir --runs-per-gpu 2 --gpu_ids 0 1 2 3 
# MAGIC ```
# MAGIC Use `-runs-per-gpu` to define the number of models that are simultaneously trained on a _single_ GPU (2 in this case) and `--gpu-ids` to define which GPUs will be used (numbers are ids according to nvidia-smi). In this example, 8 models will train simultaneously on 4 different GPUs.
# MAGIC
# MAGIC Calling `nh-run-scheduler` in `evaluate` mode will evaluate all models in all run directories in a given root directory.
# MAGIC
# MAGIC ```bash
# MAGIC nh-run-scheduler evaluate --directory /path/to/parent-run-dir/ --runs-per-gpu 2 --gpu_ids 0 1 2 3 
# MAGIC ```
# MAGIC
# MAGIC ## API usage
# MAGIC
# MAGIC Besides the command line tools, you can also use the NeuralHydrology package just like any other Python package by importing its modules, classes, or functions.
# MAGIC
# MAGIC This can be helpful for exploratory studies with trained models, but also if you want to use some of the functions or classes within a different codebase. 
# MAGIC
# MAGIC Look at the [API Documentation](https://neuralhydrology.readthedocs.io/en/latest/api/neuralhydrology.html) for a full list of functions/classes you could use.
# MAGIC
# MAGIC The following example shows how to train and evaluate a model via the API.

# COMMAND ----------

# MAGIC %pip install tensorboard neuralhydrology

# COMMAND ----------

import pickle
from pathlib import Path

import matplotlib.pyplot as plt
import torch
from neuralhydrology.evaluation import metrics
from neuralhydrology.nh_run import start_run, eval_run

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train a model for a single config file
# MAGIC
# MAGIC **Note**
# MAGIC
# MAGIC - The config file assumes that the CAMELS US dataset is stored under `data/CAMELS_US` (relative to the main directory of this repository) or a symbolic link exists at this location. Make sure that this folder contains the required subdirectories `basin_mean_forcing`, `usgs_streamflow` and `camels_attributes_v2.0`. If your data is stored at a different location and you can't or don't want to create a symbolic link, you will need to change the `data_dir` argument in the `1_basin.yml` config file that is located in the same directory as this notebook.
# MAGIC - By default, the config (`1_basin.yml`) assumes that you have a CUDA-capable NVIDIA GPU (see config argument `device`). In case you don't have any or you have one but want to train on the CPU, you can either change the config argument to `device: cpu` or pass `gpu=-1` to the `start_run()` function.

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp/basin_timeseries_v1p2_metForcing_obsFlow/basin_dataset_public_v1p2

# COMMAND ----------

# by default we assume that you have at least one CUDA-capable NVIDIA GPU
if torch.cuda.is_available():
    start_run(config_file=Path("1_basin.yml"))

# fall back to CPU-only mode
else:
    start_run(config_file=Path("1_basin.yml"), gpu=-1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluate run on test set
# MAGIC The run directory that needs to be specified for evaluation is printed in the output log above. Since the folder name is created dynamically (including the date and time of the start of the run) you will need to change the `run_dir` argument according to your local directory name. By default, it will use the same device as during the training process.

# COMMAND ----------

run_dir = Path("runs/test_run_0501_214945")
eval_run(run_dir=run_dir, period="test")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load and inspect model predictions
# MAGIC Next, we load the results file and compare the model predictions with observations. The results file is always a pickled dictionary with one key per basin (even for a single basin). The next-lower dictionary level is the temporal resolution of the predictions. In this case, we trained a model only on daily data ('1D'). Within the temporal resolution, the next-lower dictionary level are `xr`(an xarray Dataset that contains observations and predictions), as well as one key for each metric that was specified in the config file.

# COMMAND ----------

with open(run_dir / "test" / "model_epoch050" / "test_results.p", "rb") as fp:
    results = pickle.load(fp)
    
results.keys()

# COMMAND ----------

# MAGIC %md
# MAGIC The data variables in the xarray Dataset are named according to the name of the target variables, with suffix `_obs` for the observations and suffix `_sim` for the simulations.

# COMMAND ----------

results['01022500']['1D']['xr']

# COMMAND ----------

# MAGIC %md
# MAGIC Let's plot the model predictions vs. the observations

# COMMAND ----------

# extract observations and simulations
qobs = results['01022500']['1D']['xr']['QObs(mm/d)_obs']
qsim = results['01022500']['1D']['xr']['QObs(mm/d)_sim']

fig, ax = plt.subplots(figsize=(16,10))
ax.plot(qobs['date'], qobs)
ax.plot(qsim['date'], qsim)
ax.set_ylabel("Discharge (mm/d)")
ax.set_title(f"Test period - NSE {results['01022500']['1D']['NSE']:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we are going to compute all metrics that are implemented in the NeuralHydrology package. You will find additional hydrological signatures implemented in `neuralhydrology.evaluation.signatures`.

# COMMAND ----------

values = metrics.calculate_all_metrics(qobs.isel(time_step=-1), qsim.isel(time_step=-1))
for key, val in values.items():
    print(f"{key}: {val:.3f}")
