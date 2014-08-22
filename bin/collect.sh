# Author: Igo Brilhante
# Created at 22-04-2014
# Download tweets from the repo
#!/bin/bash

# Download tweets from 10-02-2014 to 31-03-2014

#wget -r --no-parent --reject "index.html*"  -A "georef-tweets-2014061*.json.gz*" http://rojo.isti.cnr.it/tweets/

for i in 0 1 2 3 4 5;
do
wget http://rojo.isti.cnr.it/tweets/georef-tweets/georef-tweets-2014061$i.json.gz;
done