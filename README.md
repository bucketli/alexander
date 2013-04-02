##alexander

##intro

anlexander is a distribute sequence service,use multi database generate the unique sequence but not sequential.it is high performance and failure tolerate with multi database except they are all failed.

##key note
if we us 4 db generate the unique sequence,the snapshot value of the start time just like `form1`.

    +-----+-----+
    | db  |value|
    +-----+-----+
    | db1 | 0   |
    +-----+-----+
    | db2 | 1000|   form1
    +-----+-----+
    | db3 | 2000|
    +-----+-----+
    | db4 | 3000|
    +-----+-----+

when the sequence server try to get the sequence or range, it may be get from the db2,the value will change to 6000, this guarantee the server get the 1000-1999 without get twice.just like `form2`.
   
    +-----+-----+
    | db  |value|
    +-----+-----+
    | db1 | 0   |
    +-----+-----+
    | db2 | 6000|  form2
    +-----+-----+
    | db3 | 2000|
    +-----+-----+
    | db4 | 3000|
    +-----+-----+
we keep such contract in all sequence server ,so the sequence will never be duplicated.

##usage
1.config the `sequence.properties` in `alexander\src\test\resources\conf\`

2.create the table in database you config,just like 
   
    CREATE TABLE `sequence` (
       `name` VARCHAR(50) COLLATE gbk_chinese_ci DEFAULT NULL,
       `value` INTEGER(11) DEFAULT NULL,
       `gmt_modified` DATETIME DEFAULT NULL,
       `id` INTEGER(11) NOT NULL AUTO_INCREMENT,
    PRIMARY KEY (`id`)
    )ENGINE=InnoDB

3.checkout the code and assembly the package
    
    mvn -Dtest -DfailIfNoTests=false clean package -P build
    
3.get the `alexander-v0.0.1-SNAPSHOT.tar.gz` package from the target,and `tar -zxvf alexander-v0.0.1-SNAPSHOT.tar.gz`,`cd xxx/bin`,type the start.sh
    
    ./start.sh
  
4.use mysql client to make a connection
    
    $>mysql -ujunyu -p123 -P8507 -hxxx.xxx.xxx.xxx
    
5.type the command to get the next sequence
    
    mysql>select next_val() where cluster='cluster1' and slice='slice1';
    +----------+--------+-------+
    | CLUSTER  | SLICE  | VALUE |
    +----------+--------+-------+
    | cluster1 | slice1 |  7001 |
    +----------+--------+-------+
    1 row in set (0.02 sec)
  
  or type command to get the next range
    
    mysql>  select next_range() where cluster='cluster1' and slice='slice1';
    +----------+--------+-------+-------+
    | CLUSTER  | SLICE  | START | END   |
    +----------+--------+-------+-------+
    | cluster1 | slice1 | 18001 | 19000 |
    +----------+--------+-------+-------+
    1 row in set (0.01 sec)
  
