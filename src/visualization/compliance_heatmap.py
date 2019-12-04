import pandas as pd
import numpy as np
import plotly.io as pio
import plotly.graph_objects as go
import datetime

def getDatesComplianceMatrix(phone_sensed_bins):
    dates = phone_sensed_bins.index
    compliance_matrix = []
    for date in dates:
        compliance_matrix.append(phone_sensed_bins.loc[date, :].tolist())
    return dates, compliance_matrix

def getComplianceHeatmap(dates, compliance_matrix, pid, output_path, bin_size):
    bins_per_hour = int(60 / bin_size)
    x_axis_labels = ["{0:0=2d}".format(x // bins_per_hour) + ":" + \
                    "{0:0=2d}".format(x % bins_per_hour * bin_size) for x in range(24 * bins_per_hour)]
    plot = go.Figure(data=go.Heatmap(z=compliance_matrix,
                                     x=x_axis_labels,
                                     y=[datetime.datetime.strftime(date, '%Y/%m/%d') for date in dates],
                                     colorscale='Viridis',
                                     colorbar={'tick0': 0,'dtick': 1}))
    plot.update_layout(title="Compliance heatmap. Five-minute bins showing how many sensors logged at least one row of data in that period for " + pid)
    pio.write_html(plot, file=output_path, auto_open=False)

# get current patient id
pid = snakemake.params["pid"]
phone_sensed_bins = pd.read_csv(snakemake.input[0], parse_dates=["local_date"], index_col="local_date")

if phone_sensed_bins.empty:
    empty_html = open(snakemake.output[0], "w")
    empty_html.write("There is no sensor data for " + pid)
    empty_html.close()
else:
    # resample to impute missing dates
    phone_sensed_bins = phone_sensed_bins.resample("1D").asfreq().fillna(0)
    # get dates and compliance_matrix
    dates, compliance_matrix = getDatesComplianceMatrix(phone_sensed_bins)
    # get heatmap
    getComplianceHeatmap(dates, compliance_matrix, pid, snakemake.output[0], 5)