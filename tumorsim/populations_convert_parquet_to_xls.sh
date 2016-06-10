for partition in $(hadoop dfs -ls /project/tumorsim/populations | grep populations | cut -c96- )
do
  export tumorid=$partition
  export outfile=/home/project/tumorsim/populations.xls/${tumorid}_population.xls
  if [ ! -f $outfile ]; then
    echo sending $partition to $outfile
    hive -e "set hive.execution.engine=tez;source /home/project/tumorsim/populations_convert_parquet_to_xls.hive" | grep -E '^\_c0'| cut -c5- > $outfile
  fi
done
