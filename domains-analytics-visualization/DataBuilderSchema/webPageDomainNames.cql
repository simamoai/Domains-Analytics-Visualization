drop keyspace webpagekeyspace;

create keyspace webpagekeyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

use webpagekeyspace;

create table accessedwebpages ( pageKey varchar,pageContent varchar,ipAddress varchar, pageCode varchar, primary key (pageKey ) ) WITH COMPACT STORAGE; 