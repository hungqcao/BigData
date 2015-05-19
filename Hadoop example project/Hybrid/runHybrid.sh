#!/bin/sh

cp -f /media/sf_Shared/hybridapproach.jar /usr/lib/hue
chmod 777 /usr/lib/hue/hybridapproach.jar
hadoop jar /usr/lib/hue/hybridapproach.jar /user/hue/inputpair /user/hue/output

echo "Done!"