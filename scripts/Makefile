help:
	@echo "parameters-generator - generate parameters for loaders"
	@echo "submit-pyspark - submit pyspark jobs for each parameters group"
	@echo "run-models - run models for generated data"

all: default

default: parameters-generator submit-pyspark run-models

parameters-generator: 
	mkdir ../expirements
	cd ../expirements && python ../scripts/parameters_generator.py

submit-pyspark:
	cd ../expirements && zsh ../scripts/submit_all.zsh

run-models:
	cd ../expirements && zsh ../scripts/run_all.zsh

clean: 
	rm -rf ../expirements