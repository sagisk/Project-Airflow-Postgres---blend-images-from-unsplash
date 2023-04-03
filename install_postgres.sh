# To make sure that Ubuntu server trusts enterprise environments
sudo apt-get install wget ca-certificates
# download GPG key:
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'

# download package information from all configured sources
sudo apt-get update
# install the Postgres package along with a -contrib package
sudo apt-get install postgresql postgresql-contrib

# start the server
sudo service postgresql start

sudo su - postgres
psql

# change the password to the one you are going to use
\password