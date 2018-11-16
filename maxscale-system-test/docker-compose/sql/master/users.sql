RESET MASTER;
CREATE DATABASE test;

CREATE USER 'maxuser'@'127.0.0.1' IDENTIFIED BY 'maxpwd';
CREATE USER 'maxuser'@'%' IDENTIFIED BY 'maxpwd';
GRANT ALL ON *.* TO 'maxuser'@'127.0.0.1' WITH GRANT OPTION;
GRANT ALL ON *.* TO 'maxuser'@'%' WITH GRANT OPTION;

CREATE USER 'skysql'@'127.0.0.1' IDENTIFIED BY 'skysql';
CREATE USER 'skysql'@'%' IDENTIFIED BY 'skysql';
GRANT ALL ON *.* TO 'skysql'@'127.0.0.1' WITH GRANT OPTION;
GRANT ALL ON *.* TO 'skysql'@'%' WITH GRANT OPTION;

CREATE USER 'maxskysql'@'127.0.0.1' IDENTIFIED BY 'skysql';
CREATE USER 'maxskysql'@'%' IDENTIFIED BY 'skysql';
GRANT ALL ON *.* TO 'maxskysql'@'127.0.0.1' WITH GRANT OPTION;
GRANT ALL ON *.* TO 'maxskysql'@'%' WITH GRANT OPTION;

CREATE USER 'repl'@'127.0.0.1' IDENTIFIED BY 'repl';
CREATE USER 'repl'@'%' IDENTIFIED BY 'repl';
GRANT ALL ON *.* TO 'repl'@'127.0.0.1' WITH GRANT OPTION;
GRANT ALL ON *.* TO 'repl'@'%' WITH GRANT OPTION;

SET GLOBAL max_connections=10000;
-- SET GLOBAL gtid_strict_mode=ON;
