import pandas as pd
import numpy as np

def rey_features(sensor_data_files, time_segment, provider, filter_data_by_segment, *args, **kwargs):
    acc_data = pd.read_csv(sensor_data_files["sensor_data"])
    requested_features = provider["FEATURES"]
    # name of the features this function can compute
    base_features_names = ["max_impulsivity_1", "max_impulsivity_2"]
    # the subset of requested features this function can compute
    features_to_compute = list(set(requested_features) & set(base_features_names))
    
    acc_features = pd.DataFrame(columns=["local_segment"] + features_to_compute)
    if not acc_data.empty:
        acc_data = filter_data_by_segment(acc_data, time_segment)

        if not acc_data.empty:
            acc_features = pd.DataFrame()

            if "max_impulsivity_1" in features_to_compute:
                acc_features["max_impulsivity_1"] = acc_data.groupby(["local_segment"])["impulsivity_1"].max()
            if "max_impulsivity_2" in features_to_compute:
                acc_features["max_impulsivity_2"] = acc_data.groupby(["local_segment"])["impulsivity_2"].max()

            acc_features = acc_features.reset_index()


    return acc_features
