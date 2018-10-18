# MockDataBaseUtils

### This repository will help you generate a random mock database based on sensitive database

---
#### 1. make sure the database configuration is correct in the config.yml file

#### 2. make sure both databases can be accessed remotely (assume user is root and password is root)
```bash
sudo vim /etc/mysql/mysql.conf.d/mysqld.cnf  # comment line: bind-address = 0.0.0.0 in cnf file
sudo service mysql restart
# username is 'root 'and password is 'root'
mysql -u root -proot -e "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'root' with grant option;"
mysql -u root -proot -e "FLUSH PRIVILEGES;"
```

#### 3. currently the code is only tested in the mysql database
