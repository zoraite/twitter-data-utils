# Author: Igo Brilhante
# Created at 22-04-2014
# Download tweets from the repo
#!/bin/bash

# Download tweets from 10-02-2014 to 31-03-2014

wget -r --no-parent --reject "index.html*","georef-tweets-2014020*.json.gz*","georef-tweets-201404*.json.gz*" http://rojo.isti.cnr.it/tweets/georef-tweets/
