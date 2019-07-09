import os
import json
from itertools import product


def generate_parameters(json_file_path, lag, bucket_size, gender_list=["homme", "femme", "all"], ):
    for (path, gender, bucket, lag) in product(
        json_file_path, gender_list, bucket_size, lag
    ):
        directory_name = "gender={}-bucket-size={}-lag={}".format(gender, bucket, lag)
        os.mkdir(directory_name)
        parameters = {"path": path, "gender": gender, "bucket": bucket, "lag": lag}
        with open(
            os.path.join(directory_name, "parameters.json"), "w"
        ) as parameters_file:
            parameters_file.write(json.dumps(parameters))


if __name__ == "__main__":
    json_file_path = ["/home/sebiat/remote_repos/production_data.json"]
    bucket_size = [1]
    lag = [30, 45, 60]
    generate_parameters(json_file_path, lag, bucket_size)

    bucket_size = [5]
    lag = [6, 12, 18]
    generate_parameters(json_file_path, lag, bucket_size)
