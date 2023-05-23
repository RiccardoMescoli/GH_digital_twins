#! /bin/sh

echo hello world, from a script file!
cd Data_logger || exit
echo starting the python script!
python data_logger.py
echo DONE