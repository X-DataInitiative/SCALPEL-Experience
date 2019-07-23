import os
import json
from itertools import product
import datetime


def config_file_generator(petit_condtionnement, grand_condtionnement, end_delay) -> str:
    now = datetime.datetime.now()
    date = now.strftime("%Y-%m-%d")
    hdfs_output = "PC={}-GC={}-ED={}".format(
        petit_condtionnement, grand_condtionnement, end_delay
    )
    return """
output.root = "/shared/Observapur/staging/{}/{}"
output.save_mode = "overwrite"

exposures.min_purchases: 1           
exposures.start_delay: 0 months      
exposures.purchases_window: 0 months 
exposures.end_threshold_ngc: {} days
exposures.end_threshold_gc: {} days
exposures.end_delay: {} days

patients.start_gap_in_months: 12

sites.sites: ["BodySites"]""".format(
        date, hdfs_output, petit_condtionnement, grand_condtionnement, end_delay
    )


def generate_parameters(
    json_file_path,
    lag,
    bucket_size,
    gender_list=["homme", "femme", "all"],
    sites=["all"],
    petit_condtionnements=[30],
    grand_condtionnements=[90],
    end_delays=[0],
):
    for (
        path,
        gender,
        bucket,
        lag,
        site,
        petit_condtionnement,
        grand_condtionnement,
        end_delay,
    ) in product(
        json_file_path,
        gender_list,
        bucket_size,
        lag,
        sites,
        petit_condtionnements,
        grand_condtionnements,
        end_delays,
    ):
        directory_name = "gender={}-bucket-size={}-lag={}-site={}-PC={}-GC={}-ED={}".format(
            gender,
            bucket,
            lag,
            site,
            petit_condtionnement,
            grand_condtionnement,
            end_delay,
        )
        os.mkdir(directory_name)
        parameters = {
            "path": path,
            "gender": gender,
            "bucket": bucket,
            "lag": lag,
            "site": site,
            "petit_condtionnement": petit_condtionnement,
            "grand_condtionnement": grand_condtionnement,
            "end_delay": end_delay,
        }
        with open(
            os.path.join(directory_name, "parameters.json"), "w"
        ) as parameters_file:
            parameters_file.write(json.dumps(parameters))

        config_file = config_file_generator(
            petit_condtionnement, grand_condtionnement, end_delay
        )
        with open(os.path.join(directory_name, "fall.conf"), "w") as configuration_file:
            configuration_file.write(config_file)


if __name__ == "__main__":
    json_file_path = [
        "/home/sebiat/builds/serial_extraction/metadata_fall_2019_07_22_10_17_25.json"
    ]
    bucket_size = [1]
    lag = [30]
    sites = ["ColDuFemur", "Poignet"]
    generate_parameters(json_file_path, lag, bucket_size, sites=sites)
