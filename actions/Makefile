help:
        @echo "all - Run all in order. "
        @echo "parameters-generator - generate parameters for experiments. (Python)"
        @echo "submit-extract - run Fall Main Extractors.(Spark Scala)"
        @echo "submit-transform - run Fall Main Transformers (Outcomes & Exposures) for each experiment.(Spark Scala)"
        @echo "submit-loaders - submit ConvSCCS loaders for each experiment. (PySpark)"
        @echo "run-models - run models for each experiment.(python + tick)"
        @echo "submit-stats - submit pyspark stats for each experiment. (PySpark)"
        @echo "extract-results - extract results to BDP out. (Shell)"
        @echo "clean - delete all experiments. (Shell)"

all: default

default: parameters-generator submit-extract submit-transform submit-stats submit-loaders run-models extract-results

parameters-generator:
        cd ../experiments && python ../scripts/parameters_generator.py

submit-extract:
        cd ../experiments && zsh ../actions/submit_extract.zsh

submit-transform:
        cd ../experiments && zsh ../actions/submit_transform.zsh

submit-loaders:
        cd ../experiments && zsh ../actions/submit_loaders.zsh

submit-stats:
        cd ../experiments && zsh ../actions/submit_stats.zsh

run-models:
        cd ../experiments && zsh ../actions/run_model.zsh

clean:
      	rm -rf ../experiments