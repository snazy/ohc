#!/bin/bash
#
# This script has been used to execute a bunch of benchmarks on a single host.
# It varies some parameters to check cache performance using mutations of parameters.
#

#
# Formula to calculate size of a single hash entry:
# HEADER_SIZE + key_length + ROUND_UP_8(key_length) + value_length
#
#   HEADER_SIZE is a constant = 56
#   ROUND_UP_8 rounds up key_length to the next 8 byte boundary
#
# IMPORTANT: always add 8 in mind since the benchmark uses a 'long' in the key
#
# Example:
#  key_length = 5 (+8)
#  value_length = 10
#  that is: 56 + (8 + 5 + 3) + 10 = 82
#
# To calculate the number of entries for a capacity is thus just
#   CAPACITY / ENTRY_LEN
#
# This does not include the overhead required by memory management (jemalloc, OS native allocator).
#

GIG_8=8589934592
GIG_16=$((2*GIG_8))
GIG_64=$((8*GIG_8))
GIG_192=$((24*GIG_8))

SEGMENTS=64
SEGMENTS4=256

#
# entry sizes vs. capacity      key-len     value-size      entry-size      capacity    # of entries
#                               256         4096            4480            8G          ~2M
#                               256         16384           16704           8G          ~500k
#                               256         65536           65856           8G          ~130k
#                               256         131072          131392          8G          ~65k
#
#
PARAMS=""
#                   Duration    Capacity    Threads     Segments    Ratio   Key-Len     Value-Size  Read-Key-Dist   Write-Key-Dist
PARAMS="$PARAMS     300         $GIG_8      8           $SEGMENTS   .8      0           4096        2000000         2000000"
PARAMS="$PARAMS     300         $GIG_64     8           $SEGMENTS   .8      0           4096        16000000        16000000"
PARAMS="$PARAMS     300         $GIG_192    8           $SEGMENTS   .8      0           4096        48000000        48000000"
PARAMS="$PARAMS     300         $GIG_8      30          $SEGMENTS   .8      0           4096        8000000         8000000"
PARAMS="$PARAMS     300         $GIG_64     30          $SEGMENTS   .8      0           4096        16000000        16000000"
PARAMS="$PARAMS     300         $GIG_192    30          $SEGMENTS   .8      0           4096        48000000        48000000"
PARAMS="$PARAMS     300         $GIG_8      64          $SEGMENTS   .8      0           4096        8000000         8000000"
PARAMS="$PARAMS     300         $GIG_64     64          $SEGMENTS   .8      0           4096        16000000        16000000"
PARAMS="$PARAMS     300         $GIG_192    64          $SEGMENTS   .8      0           4096        48000000        48000000"

PARAMS="$PARAMS     300         $GIG_8      8           $SEGMENTS   .8      256         4096        2000000         2000000"
PARAMS="$PARAMS     300         $GIG_64     8           $SEGMENTS   .8      256         4096        16000000        16000000"
PARAMS="$PARAMS     300         $GIG_192    8           $SEGMENTS   .8      256         4096        48000000        48000000"
PARAMS="$PARAMS     300         $GIG_8      30          $SEGMENTS   .8      256         4096        8000000         8000000"
PARAMS="$PARAMS     300         $GIG_64     30          $SEGMENTS   .8      256         4096        16000000        16000000"
PARAMS="$PARAMS     300         $GIG_192    30          $SEGMENTS   .8      256         4096        48000000        48000000"
PARAMS="$PARAMS     300         $GIG_8      64          $SEGMENTS   .8      256         4096        8000000         8000000"
PARAMS="$PARAMS     300         $GIG_64     64          $SEGMENTS   .8      256         4096        16000000        16000000"
PARAMS="$PARAMS     300         $GIG_192    64          $SEGMENTS   .8      256         4096        48000000        48000000"

PARAMS="$PARAMS     300         $GIG_8      8           $SEGMENTS   .8      512         4096        2000000         2000000"
PARAMS="$PARAMS     300         $GIG_64     8           $SEGMENTS   .8      512         4096        16000000        16000000"
PARAMS="$PARAMS     300         $GIG_192    8           $SEGMENTS   .8      512         4096        48000000        48000000"
PARAMS="$PARAMS     300         $GIG_8      30          $SEGMENTS   .8      512         4096        8000000         8000000"
PARAMS="$PARAMS     300         $GIG_64     30          $SEGMENTS   .8      512         4096        16000000        16000000"
PARAMS="$PARAMS     300         $GIG_192    30          $SEGMENTS   .8      512         4096        48000000        48000000"
PARAMS="$PARAMS     300         $GIG_8      64          $SEGMENTS   .8      512         4096        8000000         8000000"
PARAMS="$PARAMS     300         $GIG_64     64          $SEGMENTS   .8      512         4096        16000000        16000000"
PARAMS="$PARAMS     300         $GIG_192    64          $SEGMENTS   .8      512         4096        48000000        48000000"

PARAMS="$PARAMS     300         $GIG_8      8           $SEGMENTS   .8      0           16384       500000          500000"
PARAMS="$PARAMS     300         $GIG_64     8           $SEGMENTS   .8      0           16384       4000000         4000000"
PARAMS="$PARAMS     300         $GIG_192    8           $SEGMENTS   .8      0           16384       12000000        12000000"
PARAMS="$PARAMS     300         $GIG_8      30          $SEGMENTS   .8      0           16384       500000          500000"
PARAMS="$PARAMS     300         $GIG_64     30          $SEGMENTS   .8      0           16384       4000000         4000000"
PARAMS="$PARAMS     300         $GIG_192    30          $SEGMENTS   .8      0           16384       12000000        12000000"
PARAMS="$PARAMS     300         $GIG_8      64          $SEGMENTS   .8      0           16384       500000          500000"
PARAMS="$PARAMS     300         $GIG_64     64          $SEGMENTS   .8      0           16384       4000000         4000000"
PARAMS="$PARAMS     300         $GIG_192    64          $SEGMENTS   .8      0           16384       12000000        12000000"

PARAMS="$PARAMS     300         $GIG_8      8           $SEGMENTS   .8      0           65536       130000          130000"
PARAMS="$PARAMS     300         $GIG_64     8           $SEGMENTS   .8      0           65536       1000000         1000000"
PARAMS="$PARAMS     300         $GIG_192    8           $SEGMENTS   .8      0           65536       3000000         3000000"
PARAMS="$PARAMS     300         $GIG_8      30          $SEGMENTS   .8      0           65536       130000          130000"
PARAMS="$PARAMS     300         $GIG_64     30          $SEGMENTS   .8      0           65536       1000000         1000000"
PARAMS="$PARAMS     300         $GIG_192    30          $SEGMENTS   .8      0           65536       3000000         3000000"
PARAMS="$PARAMS     300         $GIG_8      64          $SEGMENTS   .8      0           65536       130000          130000"
PARAMS="$PARAMS     300         $GIG_64     64          $SEGMENTS   .8      0           65536       1000000         1000000"
PARAMS="$PARAMS     300         $GIG_192    64          $SEGMENTS   .8      0           65536       3000000         3000000"

#

PARAMS="$PARAMS     300         $GIG_8      8           $SEGMENTS   .5      0           4096        2000000         2000000"
PARAMS="$PARAMS     300         $GIG_64     8           $SEGMENTS   .5      0           4096        16000000        16000000"
PARAMS="$PARAMS     300         $GIG_192    8           $SEGMENTS   .5      0           4096        48000000        48000000"
PARAMS="$PARAMS     300         $GIG_8      30          $SEGMENTS   .5      0           4096        8000000         8000000"
PARAMS="$PARAMS     300         $GIG_64     30          $SEGMENTS   .5      0           4096        16000000        16000000"
PARAMS="$PARAMS     300         $GIG_192    30          $SEGMENTS   .5      0           4096        48000000        48000000"
PARAMS="$PARAMS     300         $GIG_8      64          $SEGMENTS   .5      0           4096        8000000         8000000"
PARAMS="$PARAMS     300         $GIG_64     64          $SEGMENTS   .5      0           4096        16000000        16000000"
PARAMS="$PARAMS     300         $GIG_192    64          $SEGMENTS   .5      0           4096        48000000        48000000"

PARAMS="$PARAMS     300         $GIG_8      8           $SEGMENTS   .5      0           16384       500000          500000"
PARAMS="$PARAMS     300         $GIG_64     8           $SEGMENTS   .5      0           16384       4000000         4000000"
PARAMS="$PARAMS     300         $GIG_192    8           $SEGMENTS   .5      0           16384       12000000        12000000"
PARAMS="$PARAMS     300         $GIG_8      30          $SEGMENTS   .5      0           16384       500000          500000"
PARAMS="$PARAMS     300         $GIG_64     30          $SEGMENTS   .5      0           16384       4000000         4000000"
PARAMS="$PARAMS     300         $GIG_192    30          $SEGMENTS   .5      0           16384       12000000        12000000"
PARAMS="$PARAMS     300         $GIG_8      64          $SEGMENTS   .5      0           16384       500000          500000"
PARAMS="$PARAMS     300         $GIG_64     64          $SEGMENTS   .5      0           16384       4000000         4000000"
PARAMS="$PARAMS     300         $GIG_192    64          $SEGMENTS   .5      0           16384       12000000        12000000"

#

PARAMS="$PARAMS     300         $GIG_8      8           $SEGMENTS   .2      0           4096        2000000         2000000"
PARAMS="$PARAMS     300         $GIG_64     8           $SEGMENTS   .2      0           4096        16000000        16000000"
PARAMS="$PARAMS     300         $GIG_192    8           $SEGMENTS   .2      0           4096        48000000        48000000"
PARAMS="$PARAMS     300         $GIG_8      30          $SEGMENTS   .2      0           4096        8000000         8000000"
PARAMS="$PARAMS     300         $GIG_64     30          $SEGMENTS   .2      0           4096        16000000        16000000"
PARAMS="$PARAMS     300         $GIG_192    30          $SEGMENTS   .2      0           4096        48000000        48000000"
PARAMS="$PARAMS     300         $GIG_8      64          $SEGMENTS   .2      0           4096        8000000         8000000"
PARAMS="$PARAMS     300         $GIG_64     64          $SEGMENTS   .2      0           4096        16000000        16000000"
PARAMS="$PARAMS     300         $GIG_192    64          $SEGMENTS   .2      0           4096        48000000        48000000"

PARAMS="$PARAMS     300         $GIG_8      8           $SEGMENTS   .2      0           16384       500000          500000"
PARAMS="$PARAMS     300         $GIG_64     8           $SEGMENTS   .2      0           16384       4000000         4000000"
PARAMS="$PARAMS     300         $GIG_192    8           $SEGMENTS   .2      0           16384       12000000        12000000"
PARAMS="$PARAMS     300         $GIG_8      30          $SEGMENTS   .2      0           16384       500000          500000"
PARAMS="$PARAMS     300         $GIG_64     30          $SEGMENTS   .2      0           16384       4000000         4000000"
PARAMS="$PARAMS     300         $GIG_192    30          $SEGMENTS   .2      0           16384       12000000        12000000"
PARAMS="$PARAMS     300         $GIG_8      64          $SEGMENTS   .2      0           16384       500000          500000"
PARAMS="$PARAMS     300         $GIG_64     64          $SEGMENTS   .2      0           16384       4000000         4000000"
PARAMS="$PARAMS     300         $GIG_192    64          $SEGMENTS   .2      0           16384       12000000        12000000"

#

PARAMS="$PARAMS     300         $GIG_8      8           $SEGMENTS4  .8      0           4096        2000000         2000000"
PARAMS="$PARAMS     300         $GIG_64     8           $SEGMENTS4  .8      0           4096        16000000        16000000"
PARAMS="$PARAMS     300         $GIG_192    8           $SEGMENTS4  .8      0           4096        48000000        48000000"
PARAMS="$PARAMS     300         $GIG_8      30          $SEGMENTS4  .8      0           4096        8000000         8000000"
PARAMS="$PARAMS     300         $GIG_64     30          $SEGMENTS4  .8      0           4096        16000000        16000000"
PARAMS="$PARAMS     300         $GIG_192    30          $SEGMENTS4  .8      0           4096        48000000        48000000"
PARAMS="$PARAMS     300         $GIG_8      64          $SEGMENTS4  .8      0           4096        8000000         8000000"
PARAMS="$PARAMS     300         $GIG_64     64          $SEGMENTS4  .8      0           4096        16000000        16000000"
PARAMS="$PARAMS     300         $GIG_192    64          $SEGMENTS4  .8      0           4096        48000000        48000000"

PARAMS="$PARAMS     300         $GIG_8      8           $SEGMENTS4  .8      0           16384       500000          500000"
PARAMS="$PARAMS     300         $GIG_64     8           $SEGMENTS4  .8      0           16384       4000000         4000000"
PARAMS="$PARAMS     300         $GIG_192    8           $SEGMENTS4  .8      0           16384       12000000        12000000"
PARAMS="$PARAMS     300         $GIG_8      30          $SEGMENTS4  .8      0           16384       500000          500000"
PARAMS="$PARAMS     300         $GIG_64     30          $SEGMENTS4  .8      0           16384       4000000         4000000"
PARAMS="$PARAMS     300         $GIG_192    30          $SEGMENTS4  .8      0           16384       12000000        12000000"
PARAMS="$PARAMS     300         $GIG_8      64          $SEGMENTS4  .8      0           16384       500000          500000"
PARAMS="$PARAMS     300         $GIG_64     64          $SEGMENTS4  .8      0           16384       4000000         4000000"
PARAMS="$PARAMS     300         $GIG_192    64          $SEGMENTS4  .8      0           16384       12000000        12000000"

#

PARAMS="$PARAMS     300         $GIG_8      8           $SEGMENTS4  .2      0           4096        2000000         2000000"
PARAMS="$PARAMS     300         $GIG_64     8           $SEGMENTS4  .2      0           4096        16000000        16000000"
PARAMS="$PARAMS     300         $GIG_192    8           $SEGMENTS4  .2      0           4096        48000000        48000000"
PARAMS="$PARAMS     300         $GIG_8      30          $SEGMENTS4  .2      0           4096        8000000         8000000"
PARAMS="$PARAMS     300         $GIG_64     30          $SEGMENTS4  .2      0           4096        16000000        16000000"
PARAMS="$PARAMS     300         $GIG_192    30          $SEGMENTS4  .2      0           4096        48000000        48000000"
PARAMS="$PARAMS     300         $GIG_8      64          $SEGMENTS4  .2      0           4096        8000000         8000000"
PARAMS="$PARAMS     300         $GIG_64     64          $SEGMENTS4  .2      0           4096        16000000        16000000"
PARAMS="$PARAMS     300         $GIG_192    64          $SEGMENTS4  .2      0           4096        48000000        48000000"

PARAMS="$PARAMS     300         $GIG_8      8           $SEGMENTS4  .2      0           16384       500000          500000"
PARAMS="$PARAMS     300         $GIG_64     8           $SEGMENTS4  .2      0           16384       4000000         4000000"
PARAMS="$PARAMS     300         $GIG_192    8           $SEGMENTS4  .2      0           16384       12000000        12000000"
PARAMS="$PARAMS     300         $GIG_8      30          $SEGMENTS4  .2      0           16384       500000          500000"
PARAMS="$PARAMS     300         $GIG_64     30          $SEGMENTS4  .2      0           16384       4000000         4000000"
PARAMS="$PARAMS     300         $GIG_192    30          $SEGMENTS4  .2      0           16384       12000000        12000000"
PARAMS="$PARAMS     300         $GIG_8      64          $SEGMENTS4  .2      0           16384       500000          500000"
PARAMS="$PARAMS     300         $GIG_64     64          $SEGMENTS4  .2      0           16384       4000000         4000000"
PARAMS="$PARAMS     300         $GIG_192    64          $SEGMENTS4  .2      0           16384       12000000        12000000"

#

warm_up=15
cold_sleep=5
mode="exec"
j_exec=""
pre=""
while [ $# -ne 0 ] ; do
    case $1 in
        -warm)  warm_up=$2; shift;;
        -cold)  cold_sleep=$2; shift;;
        -count) mode="count";;
        -dry)   mode="dry";;
        -pre)   pre="$2 "; shift;;
        -X)     jvm_arg="$JVM_ARG $2"; shift;;
    esac
    shift
done

#

script_dir=`dirname $0`
base_dir="$script_dir/../../.."
version=`grep '<version>' -m 1 pom.xml | sed 's/.*<version>\(.*\)<\/version>.*/\1/'`
jar=$base_dir/target/ohc-benchmark-$version.jar
if [ ! -f $jar ] ; then
    echo "Executable jar file $jar does not exist" > /dev/stderr
    exit 1
fi
java -jar $jar -h > /dev/null
if [ ! $? ] ; then
    echo "Cannot execute ohc-benchmark jar file $jar" > /dev/stderr
    exit 1
fi

time_cmd=
if [ `which gtime 2> /dev/null` ] ; then
    time_cmd=gtime
else
    time_cmd=time
fi

t_exec="$time_cmd -f %e;%S;%U;%P;%M;%K;\"%C\" "
j_exec="$j_exec java $jvm_arg -jar $jar "

capacity=
threads=
segments=
ratio=
key_len=
value_size=
read_key_dist=
write_key_dist=
p_num=1
count=0
seconds=0

log_dir=./batch-bench/`date "+%Y-%m-%d-%H-%M-%S"`
echo "Log directory for this batch is $log_dir"
mkdir -p $log_dir
if [ ! $? ] ; then
    echo "Cannot create $log_dir" > /dev/stderr
    exit 1
fi

exec >  >(tee -a $log_dir/exec.log)
exec 2> >(tee -a $log_dir/exec.log >&2)

for p in $PARAMS ; do
    case $p_num in
        1)  duration=$p;        p_num=$((p_num+1));;
        2)  capacity=$p;        p_num=$((p_num+1));;
        3)  threads=$p;         p_num=$((p_num+1));;
        4)  segments=$p;        p_num=$((p_num+1));;
        5)  ratio=$p;           p_num=$((p_num+1));;
        6)  key_len=$p;         p_num=$((p_num+1));;
        7)  value_size=$p;      p_num=$((p_num+1));;
        8)  read_key_dist=$p;   p_num=$((p_num+1));;
        9)  write_key_dist=$p
            p_num=1
            count=$((count+1))
            seconds=$((seconds+duration+warm_up+cold_sleep))
            name=`printf "bench-%03d" $count`"-c$capacity-kl$key_len-r$ratio-sc$segments-t$threads-rkd$read_key_dist-wkd$write_key_dist-vs$value_size"

            export $pre
            j_run="$t_exec -o $log_dir/$name-time.csv"
            j_run="$j_run $j_exec -cap $capacity -d $duration -kl $key_len -r $ratio -sc $segments -t $threads -wu \"$warm_up,$cold_sleep\""
            j_run="$j_run -rkd gaussian(1..$read_key_dist,2)"
            j_run="$j_run -wkd gaussian(1..$write_key_dist,2)"
            j_run="$j_run -vs gaussian(1024..$value_size,2)"
            j_run="$j_run -csv $log_dir/$name.csv"

            case $mode in
                "count")    ;;
                "dry")      echo "   would execute:   $j_run"
                            ;;
                "exec")
                            $j_run > $log_dir/$name.log
                            ;;
            esac

            ;;
    esac
done

echo "$count executions , runtime of $seconds seconds ($((seconds/60)) minutes)"
