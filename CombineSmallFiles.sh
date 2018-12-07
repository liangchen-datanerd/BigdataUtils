#!/bin/bash

#combine all directory small file into one file
echo source dir $1
echo combined File $2
find $1 -type f -exec cat {} \; >$2
