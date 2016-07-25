#!/bin/sh
basePath=/Users/adarsa/ilimi/github/Learning-Platform-Analytics/platform-scripts/VidyaVani/GenieSearch/libfmTest/PerformEval
cd $basePath
SPARCITY=$1
Learn_Rate=0.1
Dim='1,1,10'
log_file="log_file.log"
num_iter=100

mkdir "dataSPAR${1}"
cp triple_format_to_libfm.pl "dataSPAR${1}"
cp libFM "dataSPAR${1}"
cp train.csv "dataSPAR${1}"
cp test.csv "dataSPAR${1}"
rm train.csv
rm test.csv
#head -n $NUM_LINES dat20000264.dat >> "dataSPAR${1}/dat${2}.dat"
echo "## Processing file for sparcity: ${1}, Learn_Rate:" $Learn_Rate "##"
cd "dataSPAR${1}"
echo "Sparcity:" $SPARCITY >> $log_file
echo "Dimension:" $Dim >> $log_file

mv train.csv train.dat
mv test.csv test.dat
#create test and train data in libfm format
./triple_format_to_libfm.pl -in train.dat  -target 2 -delete_column 3 -separator " "  creates data 
./triple_format_to_libfm.pl -in test.dat -target 2 -delete_column 3 -separator " "  creates data 

#run libfm
start=`date +%s`
# train and test and save the model
echo "Memory usage-training:">> $log_file
./libFM -train train.dat.libfm -test test.dat.libfm -dim $Dim -iter $num_iter -method 'sgd' -task r -regular '1,1,1' -learn_rate $Learn_Rate -seed 100 -save_model "fm${1}.model"
#ps -p $! -o %cpu,%mem>>$log_file

end=`date +%s`
runtime=$((end-start))
echo "Train_time:" $runtime "secs">> $log_file

# load the model and test
start=`date +%s`
echo "Memory usage-testing:">> $log_file
./libFM -train train.dat.libfm -test train.dat.libfm -dim $Dim -iter 0 -method 'sgd' -task r -regular '1,1,1' -learn_rate $Learn_Rate -seed 100 -load_model "fm${1}.model"

#ps -p $! -o %cpu,%mem>>$log_file

end=`date +%s`
runtime=$((end-start))
echo "Test_time:" $runtime "secs">> $log_file


