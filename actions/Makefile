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
	@echo "build-scalpel-analysis - build SCALPEL-Analysis & put it under dist.(Shell)"
	@echo "build-scalpel-extraction- build SCALPEL-Extraction & put it under dist. MUST HAVE SBT 0.13 installed & Internet.(Shell)"
	@echo "build-parameters- build parameters module & put it under dist.(Shell)"
	@echo "build-components- build needed components for this repo.(Shell)"

all: default

default: parameters-generator submit-extract submit-transform submit-stats submit-loaders run-models extract-results

parameters-generator:
	cd ../experiments && python ../scripts/parameters_generator.py

extract-results:
	zsh extract_results.zsh

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
	rm -rf ../experiments/*

build-scalpel-analysis:
	cd ../SCALPEL-Analysis && zip -r ../dist/scalpel.zip scalpel

build-scalpel-extraction:
	cd ../SCALPEL-Extraction && sbt 'set assemblyOutputPath in assembly := new File("../dist/extraction.jar")' assembly

build-parameters:
	cd .. && zip -r ./dist/parameters.zip parameters

build-components: build-scalpel-extraction build-scalpel-analysis build-parameters
