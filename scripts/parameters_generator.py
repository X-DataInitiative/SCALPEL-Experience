import datetime
import json
import os
from itertools import product


def config_file_generator(petit_condtionnement, grand_condtionnement, end_delay) -> str:
    now = datetime.datetime.now()
    date = now.strftime("%Y-%m-%d")
    hdfs_output = "PC={}-GC={}-ED={}".format(
        petit_condtionnement, grand_condtionnement, end_delay
    )
    return """
output.root = "/shared/Observapur/staging/CNAM-399/{}/{}"
output.save_mode = "overwrite"

exposures.end_threshold_ngc: {} days
exposures.end_threshold_gc: {} days
exposures.end_delay: {} days

patients.start_gap_in_months: 12

outcomes.fall_frame: 4 months

sites.sites: ["BodySites"]""".format(
        date, hdfs_output, petit_condtionnement, grand_condtionnement, end_delay
    )


def generate_parameters(
    json_file_path,
    lags=[44],
    bucket_size=[1],
    gender_list=["all"],
    sites=["all"],
    petit_condtionnements=[30],
    grand_condtionnements=[90],
    end_delays=[15],
    keep_elderly=[True],
    keep_multi_fractured=[True],
    keep_multi_admitted=[True],
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
        kp,
        kmf,
        kma,
    ) in product(
        json_file_path,
        gender_list,
        bucket_size,
        lags,
        sites,
        petit_condtionnements,
        grand_condtionnements,
        end_delays,
        keep_elderly,
        keep_multi_fractured,
        keep_multi_admitted,
    ):
        directory_name = "gender={}-bucket-size={}-lag={}-site={}-PC={}-GC={}-ED={}-KeepElderly={}-KeepMultiFractured={}-KeepMultiAdmitted={}".format(
            gender,
            bucket,
            lag,
            site,
            petit_condtionnement,
            grand_condtionnement,
            end_delay,
            kp,
            kmf,
            kma,
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
            "keep_elderly": kp,
            "keep_multi_fractured": kmf,
            "keep_multi_admitted": kma
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
    # Default
    json_file_path = ["metadata_fall.json"]
    generate_parameters(json_file_path)

    # Remove Elderly
    generate_parameters(json_file_path, keep_elderly=[False])

    # Gender variation
    generate_parameters(json_file_path, gender_list=["homme", "femme"])

    # No end delay
    generate_parameters(json_file_path, lags=[29], end_delays=[0])

    # Large End Delay
    generate_parameters(json_file_path, end_delays=[30], lags=[59])

    # Smoothed
    generate_parameters(json_file_path, bucket_size=[5], lags=[8])

    # Hip Fracture
    generate_parameters(json_file_path, sites=["ColDuFemur"])

    # One admission for fracture
    generate_parameters(json_file_path, keep_multi_admitted=[False])

    # Exclude multi trauma patients
    generate_parameters(json_file_path, keep_multi_fractured=[False])

    # One admission and one fracture for the admission
    generate_parameters(
        json_file_path, keep_multi_fractured=[False], keep_multi_admitted=[False]
    )

    # Under lagged
    generate_parameters(json_file_path, lags=[30])

    # Under lagged, low granularity
    generate_parameters(json_file_path, lags=[6], bucket_size=[5])
