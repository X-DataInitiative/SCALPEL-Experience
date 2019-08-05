help:
	@echo "parameters-generator - generate parameters for experiments"
	@echo "submit-loaders - submit ConvSCCS loaders for each experiment"
	@echo "run-models - run models for each experiments"
	@echo "submit-transform - run Fall Main transformers for each experiment. Need to update transform_submit.zsh"
	@echo "submit-stats - submit pyspark stats for each experiment"
	@echo "clean - delete all experiments"

all: default

default: parameters-generator submit-transform submit-pyspark run-models

parameters-generator: 
	mkdir ../expirements
	cd ../expirements && python ../scripts/parameters_generator.py

submit-transform:
	cd ../expirements && zsh ../scripts/transform_submit.zsh

submit-loaders:
	cd ../expirements && zsh ../scripts/submit_all.zsh

submit-stats:
	cd ../expirements && zsh ../scripts/submit_stats.zsh

run-models:
	cd ../expirements && zsh ../scripts/run_all.zsh

clean: 
	rm -rf ../expirements