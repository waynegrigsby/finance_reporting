#!/bin/zsh
if [[ -e $VIRTUAL_ENV/src/.env ]]; then
  zmodload zsh/mapfile
  FNAME=$VIRTUAL_ENV/src/.env
  FLINES=( "${(f)mapfile[$FNAME]}" )
#  integer POS=1
#  integer SIZE=$#FLINES
#  for i in $FLINES; do echo "export $i"; done
  source "$VIRTUAL_ENV/src/.env"
  print "Environment Established!"
else
  print "No .env file present, please create before processing.";
fi
