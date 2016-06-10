echo "finding the maximum mutations in detectable tumors"
hive -e "set hive.execution.engine=tez;select concat('export mutations=' , max(mutations)) from tumorsim.tumors_detectable where cells > 0" > /home/project/tumorsim/mutations.sh

echo "setting environment with max mutations"
source /home/project/tumorsim/mutations.sh

echo "initialize json builder with the maximum number of mutations $mutations"
hive -e "source /home/project/tumorsim/initialize_tumors_to_json.hive"

for ((mutations=$mutations-1; mutations > 0 ; mutations--))
do

  echo "Working on mutation level: $mutations"
  hive -e "source /home/project/tumorsim/parent_and_children_to_json.hive"

  echo "exporting json and strip out the tabs and double quotes"
  hive -e "set hive.execution.engine=tez; select * from tumorsim.detectable_json_parents" | tr -d '\"' | sed 's/\t//g'  > /home/project/tumorsim/detectable.json

  echo "importing cleaned json before next mutation"
  hive -e "load data local inpath '/home/project/tumorsim/detectable.json' overwrite into table tumorsim.detectable_json_children"
done

### Export one tumor like this
### hive -e "set hive.execution.engine=tez; select 'var flare =' , json from tumorsim.detectable_json_children where tumorid='BD9_20308'" | tr -d '\"' | sed 's/\t//g'  | sed 's/NULL/0/g' > /home/project/tumorsim/BD9_20308.js
### cat /home/project/tumorsim/BD9_20308.js | cut -c-400
