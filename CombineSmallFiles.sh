#!/bin/bash
echo source dir $1
echo combined File $2
find $1 -type f -exec cat {} \; >$2
