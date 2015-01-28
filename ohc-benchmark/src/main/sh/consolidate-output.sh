#!/bin/bash

echo -n '"seconds";"cpu_secs_kernel";"cpu_secs_user";"cpu_perc";"mem_rss";"mem_total";'
echo -n '"capacity";"threads";"key_len";"val_siz";"ratio";"segments";'
echo '"runtime";"r_count";"r_oneMinuteRate";"r_fiveMinuteRate";"r_fifteenMinuteRate";"r_meanRate";"r_snapMin";"r_snapMax";"r_snapMean";"r_snapStdDev";"r_snap75";"r_snap95";"r_snap98";"r_snap99";"r_snap999";"r_snapMedian";"w_count";"w_oneMinuteRate";"w_fiveMinuteRate";"w_fifteenMinuteRate";"w_meanRate";"w_snapMin";"w_snapMax";"w_snapMean";"w_snapStdDev";"w_snap75";"w_snap95";"w_snap98";"w_snap99";"w_snap999";"w_snapMedian"'

find . -name "bench-???-*-time.csv" | while read f ; do
    bn=`basename $f`
    datapoints=`echo $f | sed 's/-time//'`

    from_time=`cat $f | sed 's/^\(.*\);".*"/\1;/'`

    # -c$capacity-kl$key_len-r$ratio-sc$segments-t$threads-rkd$read_key_dist-wkd$write_key_dist-vs$value_size
    capacity=`echo $bn | sed 's/.*-c\([0-9.]*\)-.*/\1/'`
    key_len=`echo $bn | sed 's/.*-kl\([0-9.]*\)-.*/\1/'`
    ratio=`echo $bn | sed 's/.*-r\([0-9.]*\)-.*/\1/'`
    segments=`echo $bn | sed 's/.*-sc\([0-9.]*\)-.*/\1/'`
    threads=`echo $bn | sed 's/.*-t\([0-9.]*\)-.*/\1/'`
    read_key_dist=`echo $bn | sed 's/.*-rkd\([0-9.]*\)-.*/\1/'`
    write_key_dist=`echo $bn | sed 's/.*-wkd\([0-9.]*\)-.*/\1/'`
    value_size=`echo $bn | sed 's/.*-vs\([0-9.]*\)-.*/\1/'`

    lastline=`cat $datapoints | grep -v "^#" | grep -v "^\"" | tail -1`

    echo -n ${from_time}
    echo -n "$capacity;$threads;$key_len;$value_size;$ratio;$segments;"
    echo ${lastline}
done
