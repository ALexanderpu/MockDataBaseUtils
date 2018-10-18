# MockDataBaseUtils

### This repository will help you generate a random mock database based on sensitive database

---
#### make sure the database configuration is correct in the config.yml file

#### make sure both databases can be accessed remotely
```bash
sudo vim /etc/mysql/mysql.conf.d/mysqld.cnf  # comment line: bind-address = 0.0.0.0 in cnf file
sudo service mysql restart
# username is 'root 'and password is 'root'
mysql -u root -proot -e "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'root' with grant option;"
mysql -u root -proot -e "FLUSH PRIVILEGES;"
```

