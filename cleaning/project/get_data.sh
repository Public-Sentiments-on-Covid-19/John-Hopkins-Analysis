# url of the data being parsed
URL="https://github.com/CSSEGISandData/COVID-19.git"
PWD=`pwd`

# remove any existing files
rm data.csv
rm -rf data

# get the data from the repository
git clone $URL
git -C COVID-19 pull


# copy the files over to local dir
mkdir data
LOC="${PWD}/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/*.csv"
for file in $LOC;
do
    f=${file##*/}
    dos2unix -n $file "${PWD}/data/${f}"
done

LOC="${PWD}/data/*.csv"
for file in $LOC;
do
    f=${file##*/}
    sed -e "1d;s/$/,${f%.csv}/" $file >> "${PWD}/data.csv"
done
