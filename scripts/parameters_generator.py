import datetime
import json
import os
from itertools import product


def config_file_generator(petit_condtionnement, grand_condtionnement, start_delay, end_delay) -> str:
    now = datetime.datetime.now()
    date = now.strftime("%Y-%m-%d")
    hdfs_output = "PC={}-GC={}-SD={}-ED={}".format(
        petit_condtionnement, grand_condtionnement, start_delay, end_delay
    )
    return """
output.root = "/shared/Observapur/staging/CNAM-339/{}/{}"
output.save_mode = "overwrite"

exposures.end_threshold_ngc: {} days
exposures.end_threshold_gc: {} days
exposures.start_delay: {} days
exposures.end_delay: {} days

patients.start_gap_in_months: 12

outcomes.fall_frame: 4 months

sites.sites: ["BodySites"]""".format(
        date, hdfs_output, petit_condtionnement, grand_condtionnement, start_delay, end_delay
    )


def generate_parameters(
    json_file_path,
    lags=[44],
    bucket_size=[1],
    gender_list=["all"],
    sites=["all"],
    petit_condtionnements=[30],
    grand_condtionnements=[90],
    start_delays=[0],
    end_delays=[15],
    fracture_severities=["all"],
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
        start_delay,
        end_delay,
        fs,
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
        start_delays,
        end_delays,
        fracture_severities,
        keep_elderly,
        keep_multi_fractured,
        keep_multi_admitted,
        epileptics_controls,
        drugs_controls,
    ):
        directory_name = (
            "gender={}-bucketsize={}-lag={}-site={}"
            "-PC={}-GC={}-SD={}-ED={}"
            "-FractureSeverity={}"
            "-KeepElderly={}-KeepMultiFractured={}-"
            "KeepMultiAdmitted={}-Epileptics={}-Drugs={}"
        ).format(
            gender,
            bucket,
            lag,
            site.__repr__()
            .translate({ord(c): "" for c in ["[", "]", " ", "'"]})
            .replace(",", "_"),
            petit_condtionnement,
            grand_condtionnement,
            start_delay,
            end_delay,
            fs.__repr__()
            .translate({ord(c): "" for c in ["[", "]", " ", "'"]})
            .replace(",", "_"),
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
            "start_delay": start_delay,
            "end_delay": end_delay,
            "fracture_severity": fs,
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
            petit_condtionnement, grand_condtionnement, start_delay, end_delay
        )
        with open(os.path.join(directory_name, "fall.conf"), "w") as configuration_file:
            configuration_file.write(config_file)


if __name__ == "__main__":
    json_file_path = ["metadata_fall.json"]

    generate_parameters(json_file_path, fracture_severities=[[3], [3, 2], [4], [3, 2, 4]])
