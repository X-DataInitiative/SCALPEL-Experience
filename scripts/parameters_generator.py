import os
import json
from itertools import product


def generate_parameters(
    json_file_path,
    lag,
    bucket_size,
    gender_list=["homme", "femme", "all"],
    sites=["all"],
):
    for (path, gender, bucket, lag, site) in product(
        json_file_path, gender_list, bucket_size, lag, sites
    ):
        directory_name = "gender={}-bucket-size={}-lag={}-site={}".format(
            gender, bucket, lag, site
        )
        os.mkdir(directory_name)
        parameters = {
            "path": path,
            "gender": gender,
            "bucket": bucket,
            "lag": lag,
            "site": site,
        }
        with open(
            os.path.join(directory_name, "parameters.json"), "w"
        ) as parameters_file:
            parameters_file.write(json.dumps(parameters))


if __name__ == "__main__":
    json_file_path = ["/home/sebiat/builds/serial_extraction/metadata_fall_2019_07_22_10_17_25.json"]
    bucket_size = [1]
    lag = [30]
    sites = ["ColDuFemur", "all", "Poignet"]
    generate_parameters(json_file_path, lag, bucket_size, sites=sites)
