help:
	@echo "parameters-generator - generate parameters for loaders"
	@echo "submit-pyspark - submit pyspark jobs for each parameters group"
	@echo "run-models - run models for generated data"
	@echo "submit-transform- run Fall Main transformers. Need to update transform_submit.zsh"
	@echo "clean - delete all experiments"

all: default

default: parameters-generator submit-transform submit-pyspark run-models

parameters-generator: 
	mkdir ../expirements
	cd ../expirements && python ../scripts/parameters_generator.py

submit-transform:
	cd ../expirements && zsh ../scripts/transform_submit.zsh

submit-pyspark:
	cd ../expirements && zsh ../scripts/submit_all.zsh

run-models:
	cd ../expirements && zsh ../scripts/run_all.zsh

clean: 
	rm -rf ../expirements