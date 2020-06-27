#!/bin/bash
# A bash script to set up ACCOMP folder and copy the required files


# Check whether ACCOMP already exists
if [[ -d "/home/nicolas/ACCOMP/" ]]; then
    echo "ACCOMP folder already exists"
    exit
fi


# folder structure
mkdir -p $HOME/ACCOMP/{REQUIRED_FILES/Events_ALL,Scripts/Python/storage}
#mkdir -p $HOME/ACCOMP/{REQUIRED_FILES/{ACCOMP_VISUAL,Events_ALL},Scripts/{Python/{send/{email_templates,env,utils},storage},R}}
#mkdir -p $HOME/ACCOMP/{REQUIRED_FILES/{ACCOMP_VISUAL,Events_ALL},Scripts/{Python/{send/{email_templates,env,utils},storage},R},OUTPUT-XXX}

# Script files

# CERT FILE try 1
# 1. CertificateWebApp1T2019a.csv
# 2. CertificateWebApp1T2019a-EPSIINF057202.csv
# 3. WebApp-certified-verified-audit.csv
# cp /home/nicolas/LAT-INV/COURSES_DATA/WebApp/CertificateWebApp1T2019a.csv /home/nicolas/ACCOMP/REQUIRED_FILES/cert.csv

# GRADES FILE
# cp /home/nicolas/LAT-INV/COURSES_DATA/WebApp/UAMx_WebApp_1T2019a_grade_report_2020-03-11-0004.csv /home/nicolas/ACCOMP/REQUIRED_FILES/grades.csv

# PROFILES FILE try 1
# 1. UAMx_WebApp_1T2019a_student_profile_info_2020-02-26-0831.csv
# 2. UAMx_WebApp_1T2019a_student_profile_info_2020-03-11-0003.csv
cp /home/nicolas/LAT-INV/COURSES_DATA/WebApp/UAMx_WebApp_1T2019a_student_profile_info_2020-02-26-0831.csv /home/nicolas/ACCOMP/REQUIRED_FILES/profiles.csv


# EVENTS files (copy 4 files)
# cp /home/nicolas/LAT-INV/COURSES_DATA/WebApp/Events_ALL/1T2019aanonymized_WebApp-uamx-edx-events-2019-04-03-edx.txt /home/nicolas/LAT-INV/COURSES_DATA/WebApp/Events_ALL/1T2019aanonymized_WebApp-uamx-edx-events-2019-04-04-edx.txt /home/nicolas/LAT-INV/COURSES_DATA/WebApp/Events_ALL/1T2019aanonymized_WebApp-uamx-edx-events-2019-04-05-edx.txt /home/nicolas/LAT-INV/COURSES_DATA/WebApp/Events_ALL/1T2019aanonymized_WebApp-uamx-edx-events-2019-04-06-edx.txt /home/nicolas/ACCOMP/REQUIRED_FILES/Events_ALL/.

# EVENTS files (copy all files)
cp /home/nicolas/LAT-INV/COURSES_DATA/WebApp/Events_ALL/*.txt /home/nicolas/ACCOMP/REQUIRED_FILES/Events_ALL/.


# Scripts de Python
cp /home/nicolas/Documents/MasterII/TFM/Proyecto/edx-la-interest-prediction/Scripts/Python/bridging_mongo.py /home/nicolas/Documents/MasterII/TFM/Proyecto/edx-la-interest-prediction/Scripts/Python/config.yaml /home/nicolas/Documents/MasterII/TFM/Proyecto/edx-la-interest-prediction/Scripts/Python/edxevent.py /home/nicolas/ACCOMP/Scripts/Python/.
cp /home/nicolas/Documents/MasterII/TFM/Proyecto/edx-la-interest-prediction/Scripts/Python/storage/edxmongodbstore.py /home/nicolas/ACCOMP/Scripts/Python/storage/.


# INITIAL
# ls -alR /home/nicolas/ACCOMP/ > /home/nicolas/ACCOMP/original_detail.txt
# ls -aR /home/nicolas/ACCOMP/ > /home/nicolas/ACCOMP/original_simple.txt

# Backup previous Output (REVIEW NUMBER nnn)
mkdir /home/nicolas/ACCOMP_historic/ACCOMPnnn
cp --recursive /home/nicolas/ACCOMP/Scripts /home/nicolas/ACCOMP_historic/ACCOMPnnn/.
cp --recursive /home/nicolas/ACCOMP/OUTPUT* /home/nicolas/ACCOMP_historic/ACCOMPnnn/.

# Remove previous Output
rm -I --verbose /home/nicolas/ACCOMP/OUTPUT*/*.csv
rm --dir /home/nicolas/ACCOMP/OUTPUT*
rm --dir /home/nicolas/ACCOMP/REQUIRED_FILES/user_current_inactivity_days.csv

# Remove previous Code


# Connect to mondo, create backup and erase db:
# mongo
# > db.copyDatabase("edxmongo", "edxmongo_2020_mm_DD")
# > use edxmongo
# > db.dropDatabase()

