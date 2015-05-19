#!/bin/sh

cp -f /media/sf_Shared/stripesapproach.jar /usr/lib/hue
chmod 777 /usr/lib/hue/stripesapproach.jar
hadoop jar /usr/lib/hue/stripesapproach.jar /user/hue/inputpair /user/hue/output

echo "Done!"