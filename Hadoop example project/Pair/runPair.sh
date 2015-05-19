#!/bin/sh

cp -f /media/sf_Shared/pairapproach.jar /usr/lib/hue
chmod 777 /usr/lib/hue/pairapproach.jar
hadoop jar /usr/lib/hue/pairapproach.jar /user/hue/inputpair /user/hue/output

echo "Done!"