#!/bin/sh

# usage: batkraken.sh outdir command options
# reads input files from stdin

outdir=$1
shift
rest=$*

outdir="$outdir/"

#echo $outdir
#echo $rest

# read input files from stdin
ins=`cat`

#echo $ins

mkdir -p `dirname $ins | sed "s/ /\n/g" | perl -pe "\\$_=\"$outdir\".\\$_"`

#echo mkdir -p `dirname $ins | sort -u | sed "s/^/$outdir\//"`

inargs=`for f in $ins; do echo "-i $f $outdir/$f" | perl -pe 's/\.png$/.xml/'; done`

#echo $ins | sed 's/ /\n/g' | xargs -L 1 echo kraken "{} $rest"

kraken $inargs $rest
