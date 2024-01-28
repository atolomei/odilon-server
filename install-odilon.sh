




# --------------------
# DOWNLOAD ODILON
# We will install the server on /opt/odilon
#
cd /opt
sudo wget http://odilon.io/resources/odilon-server-0.5-beta.tar.gz 

# --------------------
# add odilon user
#
sudo useradd -s /sbin/nologin -d /opt/odilon odilon
sudo chown -R odilon:odilon /opt/odilon-server-0.5-beta.tar.gz

# --------------------
# uncompress the server
# and make user odilon owner
#  
sudo tar xvf /opt/odilon-server-0.5-beta.tar.gz
sudo chown -R odilon:odilon /opt/odilon


# ---------------
# create data directory
#
sudo mkdir -p /opt/odilon-data
sudo chown -R odilon:odilon /opt/odilon-data
sudo chmod u+rxw /opt/odilon-data

# --------------
# Make it a service
#

# sudo nano /etc/systemd/system/odilon.service
#---

#---

sudo systemctl enable odilon
sudo systemctl start odilon
sudo systemctl status odilon

#---
#
# the following command should display the info 
sudo curl -u odilon:odilon localhost:9234/info




# ps -ef | grep odilon



--------------------------------------------------------
useradd -s /sbin/nologin -d /opt/odilon odilon
mkdir -p /opt/odilon
mkdir -p /opt/odilon-data


wget http://odilon.io/resources/odilon-server-0.5-beta.tar.gz -O /opt/
tar xvf odilon-server-0.5-beta.tar.gz

chown odilon:odilon /opt/odilon
chown odilon:odilon /opt/odilon-data

chmod u+rwx /opt/odilon
chmod u+rwx /opt/odilon-data

chmod g-rwx /opt/odilon
chmod g-rwx /opt/odilon-data

chmod o-rwx /opt/odilon
chmod o-rwx /opt/odilon-data

nano /etc/default/odilon
------------------------

systemctl enable odilon
systemctl start odilon
systemctl status odilon

nano /etc/systemd/system/odilon.service
--------------------------------------
# To install in /opt with Linux user 'odilon'
							# after extracting the binaries, the server will be located in:
							# /opt/odilon
							#

							sudo su - odilon
							cd /opt

							# create default data directory
							mkdir odilon-data

							#extract directory /odilon
							tar xvf odilon-server-0.5-beta.tar.gz