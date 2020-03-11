#!/bin/zsh

dir_name=$(date +%Y-%m-%d)
sudo mkdir -p /home/bdp1/OUT/$dir_name
cd ../experiments && sudo rsync -zarvm --include="*/" --include="*.csv" --include="*.json" --include="*.pdf" --exclude="*" ./ /home/bdp1/OUT/$dir_name/.
echo "Output file is /home/bdp1/OUT/$dir_name"