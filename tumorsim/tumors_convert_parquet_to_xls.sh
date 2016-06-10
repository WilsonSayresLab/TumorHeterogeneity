for partition in $(hadoop dfs -ls /project/tumorsim/tumors/*/* | grep tumors | cut -c91- )
do
  if [ ${#partition} = 33 ]; then
     export tumorid=${partition:0:8}
     export finalsnapshot=${partition:23:1}
     export year=${partition:30:3}
  else
     export tumorid=${partition:0:9}
     export finalsnapshot=${partition:24:1}
     export year=${partition:31:3}
  fi
  export outfile=/home/project/tumorsim/tumors.xls/${tumorid}_${finalsnapshot}_${year}_tumor.xls
  if [ ! -f $outfile ]; then
    echo sending $partition to $outfile
    hive -e "set hive.execution.engine=tez;source /home/project/tumorsim/tumors_convert_parquet_to_xls.hive" | grep -E '^\_c0'| cut -c5- > $outfile
  fi
done
