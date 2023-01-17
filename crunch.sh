#!/bin/bash
cd ~/Data/RAW
read -p "Enter Crunch Password. " num
if [ $num -eq 2134 ]
then
  echo "Password correct."
else
  echo "Incorrect."
  exit
fi
read -p "Enter directory to save this file to. " target

#python3 ~/BentoCruncher/ohlcv-1m-parser.py target
