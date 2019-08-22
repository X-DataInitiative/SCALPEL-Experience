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
    epileptics_controls=[False],
    drugs_controls=[False],
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
        ec,
        dc,
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
        epileptics_controls,
        drugs_controls,
    ):
        directory_name = (
            "gender={}-bucketsize={}-lag={}-site={}"
            "-PC={}-GC={}-ED={}"
            "-KeepElderly={}-KeepMultiFractured={}-"
            "KeepMultiAdmitted={}-epileptics={}-drugs={}"
        ).format(
            gender,
            bucket,
            lag,
            site.__repr__()
            .translate({ord(c): "" for c in ["[", "]", " ", "'"]})
            .replace(",", "_"),
            petit_condtionnement,
            grand_condtionnement,
            end_delay,
            kp,
            kmf,
            kma,
            ec,
            dc,
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
            "keep_multi_admitted": kma,
            "epileptics_control": ec,
            "drugs_control": dc,
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
    json_file_path = ["metadata_fall.json"]

    # Remove Epileptics
    generate_parameters(json_file_path, epileptics_controls=[True])

    # Control drugs
    generate_parameters(json_file_path, drugs_controls=[True])

    # Poignet & Bassin
    generate_parameters(json_file_path, sites=[["Bassin", "Poignet"]])
