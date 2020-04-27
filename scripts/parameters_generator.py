import datetime
import json
import os
from itertools import product


def config_file_generator(
    petit_conditionnement,
    grand_conditionnement,
    start_delay,
    end_delay,
    interaction_level,
    interaction_min_duration
) -> str:
    now = datetime.datetime.now()
    date = now.strftime("%Y-%m-%d")
    hdfs_output = "PC={}-GC={}-SD={}-ED={}-IL={}-IMD={}".format(
        petit_conditionnement,
        grand_conditionnement,
        start_delay,
        end_delay,
        interaction_level,
        interaction_min_duration
    )
    return """
output.root = "/shared/FALL/staging/featuring/CNAM-model-003-min-duration/{}/{}"
output.save_mode = "overwrite"

exposures.end_threshold_ngc: {} days
exposures.end_threshold_gc: {} days
exposures.start_delay: {} days
exposures.end_delay: {} days

patients.start_gap_in_months: 12

outcomes.fall_frame: 4 months

interactions.level: {}
interactions.minimum_duration: {} days

sites.sites: ["BodySites"]""".format(
        date,
        hdfs_output,
        petit_conditionnement,
        grand_conditionnement,
        start_delay,
        end_delay,
        interaction_level,
        interaction_min_duration
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
    interaction_levels=[2],
    interaction_min_durations=[30],
    exposure_types = ["exposures"]
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
        il,
        imd,
        et
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
        interaction_levels,
        interaction_min_durations,
        exposure_types
    ):
        directory_name = (
            "G={}-BS={}-L={}-S={}-"
            "PC={}-GC={}-SD={}-ED={}-"
            "FS={}-"
            "KE={}-KMF={}-"
            "KMA={}-EC={}-DC={}-"
            "IL={}-IMD={}"
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
            il,
            imd
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
            "interaction_level": il,
            "exposure_type": et,
        }
        with open(
            os.path.join(directory_name, "parameters.json"), "w"
        ) as parameters_file:
            parameters_file.write(json.dumps(parameters))

        config_file = config_file_generator(
            petit_condtionnement, grand_condtionnement, start_delay, end_delay, il, imd
        )
        with open(os.path.join(directory_name, "fall.conf"), "w") as configuration_file:
            configuration_file.write(config_file)


if __name__ == "__main__":
    json_file_path = ["metadata_fall.json"]

    generate_parameters(
        json_file_path, interaction_min_durations=[0, 15, 30], exposure_types=["interactions"]
    )

    # # Fracture unique
    # generate_parameters(
    #     json_file_path,
    #     keep_multi_admitted=[False],
    #     keep_multi_fractured=[False, True]
    # )
    #
    # # Les jeunes vieux
    # generate_parameters(json_file_path, keep_elderly=[False])
    #
    # # Drugs control
    # generate_parameters(json_file_path, drugs_controls=[True])
    #
    # # Epileptics
    # generate_parameters(json_file_path, epileptics_controls=[True])
    #
    # # Le sexe
    # generate_parameters(json_file_path, gender_list=["femme", "homme"])
    #
    # # Severites
    # generate_parameters(json_file_path, fracture_severities=[[1], [1, 2], [3]])
    #
    # # Sites
    # generate_parameters(json_file_path, sites=[["ColDuFemur"], ["Poignet"], ["Rachis"]])
