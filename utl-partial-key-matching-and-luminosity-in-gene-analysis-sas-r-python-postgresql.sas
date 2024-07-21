%let pgm=utl-partial-key-matching-and-luminosity-in-gene-analysis-sas-r-python-postgresql;

Partial key matching to add the full key and luminosity in gene matching sas r, python and postgresql

      Solutions
           1 sas sql
           2 r sqldf
           3 python sqldf
           4 python PostgreSQL (relational database)
           5 r postgresql
           6 related repos

github
https://tinyurl.com/yxcnsfsx
https://github.com/rogerjdeangelis/utl-partial-key-matching-and-luminosity-in-gene-analysis-sas-r-python-postgresql

stackoverflow
https://tinyurl.com/5n74r2a8
https://stackoverflow.com/questions/78673584/r-data-manipulation-help-for-each-value-in-column-b-look-in-column-a-for-the

github (how to set up postgresql
https://tinyurl.com/5n87zmsb
https://github.com/rogerjdeangelis/utl-saving-and-creating-r-dataframes-to-and-from-a-postgresql-database-schema

/*               _     _
 _ __  _ __ ___ | |__ | | ___ _ __ ___
| `_ \| `__/ _ \| `_ \| |/ _ \ `_ ` _ \
| |_) | | | (_) | |_) | |  __/ | | | | |
| .__/|_|  \___/|_.__/|_|\___|_| |_| |_|
|_|
*/

/**************************************************************************************************************************/
/*                                              |                               |                                         */
/*              INPUT                           |        PROCESS                |        OUTPUT                           */
/*              =====                           |        =======                |        ======                           */
/*                                              |                               |                                         */
/* Update the parial keys in table b            | Normalize the keys table a    |                                         */
/* to full compound keys.                       |                               |                                         */
/* Note: Both gene# and DI00# are primary       |      KEY        PART          |      KEY        LUMINS                  */
/* keys.                                        |                               |                                         */
/*                                              |  gene1-ID001    gene1         |  gene1-ID001      22                    */
/* SD1.A total obs=5                            |  gene2-ID002    gene2         |  gene2-ID002      13                    */
/*                                              |  gene3-ID003    gene3         |  gene3-ID003      11                    */
/*     Obs        KEY                           |  gene4-ID004    gene4         |  gene4-ID004      15                    */
/*                                              |  gene5-ID005    gene5         |                                         */
/*      1     gene1-ID001  Note gene# and       |  gene1-ID001    ID001         |                                         */
/*      2     gene2-ID002  ID00# are primary    |  gene2-ID002    ID002         |                                         */
/*      3     gene3-ID003  keys.                |  gene3-ID003    ID003         |                                         */
/*      4     gene4-ID004                       |  gene4-ID004    ID004         |                                         */
/*      5     gene5-ID005                       |  gene5-ID005    ID005         |                                         */
/*                                              |                               |                                         */
/*                                              |                               |                                         */
/*                                              |  Join the normaized keys      |                                         */
/* SD1.B total obs=4                            |  with table b. Ignore         |                                         */
/*                                              |  'part2'                      |                                         */
/*                                              |                               |                                         */
/*    Obs     KEY           LUMINS              |   PART1    LUMINS             |                                         */
/*                                              |                               |                                         */
/*     1      gene1           22                |   gene1      22               |                                         */
/*     2      ID002           13                |   ID002      13               |                                         */
/*     3      gene3-ID003     11 ignore ID003   |   gene3      11               |                                         */
/*     4      gene4           15                |   gene4      15               |                                         */
/*                                              |                               |                                         */
/*                                              |   Join tables                 |                                         */
/*                                              |                               |                                         */
/*                                              |   on                          |                                         */
/*                                              |     a.key  = b.part1          |                                         */
/*                                              |   where                       |                                         */
/*                                              |     not missing(r.part1       |                                         */
/*                                              |                               |                                         */
/**************************************************************************************************************************/

/*                   _           _ _
(_)_ __  _ __  _   _| |_    __ _| | |
| | `_ \| `_ \| | | | __|  / _` | | |
| | | | | |_) | |_| | |_  | (_| | | |
|_|_| |_| .__/ \__,_|\__|  \__,_|_|_|
        |_|
*/

libname sd1 "d:/sd1";
options validvarname=any;
data sd1.a (rename='Key'n = 'key'n);
 input key $16.;
cards4;
gene1-ID001
gene2-ID002
gene3-ID003
gene4-ID004
gene5-ID005
;;;;
run;quit;

data sd1.b(rename=('lumins'n = 'lumins'n 'Key'n='key'n));
input lumins key $16.;
cards4;
22 gene1
13 ID002
11 gene3-ID003
15 gene4
;;;;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*                                                                                                                        */
/* SD1.A total obs=5                                                                                                      */
/*                                                                                                                        */
/*  Obs        KEY                                                                                                        */
/*                                                                                                                        */
/*   1     gene1-ID001                                                                                                    */
/*   2     gene2-ID002                                                                                                    */
/*   3     gene3-ID003                                                                                                    */
/*   4     gene4-ID004                                                                                                    */
/*   5     gene5-ID005                                                                                                    */
/*                                                                                                                        */
/*                                                                                                                        */
/* SD1.B total obs=4                                                                                                      */
/*                                                                                                                        */
/*  Obs    LUMINS    KEY                                                                                                  */
/*                                                                                                                        */
/*   1       22      gene1                                                                                                */
/*   2       13      ID002                                                                                                */
/*   3       11      gene3-ID003                                                                                          */
/*   4       15      gene4                                                                                                */
/*                                                                                                                        */
/**************************************************************************************************************************/

proc datasets lib=work nodetails nolist;
 delete want;
run;quit;

/*                             _
/ |  ___  __ _ ___   ___  __ _| |
| | / __|/ _` / __| / __|/ _` | |
| | \__ \ (_| \__ \ \__ \ (_| | |
|_| |___/\__,_|___/ |___/\__, |_|
                            |_|
*/
proc sql;
  create
     table nrma as
  select
     key
    ,scan(key,1,'-') as part
  from
     sd1.a
  union
     all
  select
     key
    ,scan(key,2,'-')  as part
  from
     sd1.a
;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* Obs        KEY        PART                                                                                             */
/*                                                                                                                        */
/*   1    gene1-ID001    gene1                                                                                            */
/*   2    gene2-ID002    gene2                                                                                            */
/*   3    gene3-ID003    gene3                                                                                            */
/*   4    gene4-ID004    gene4                                                                                            */
/*   5    gene5-ID005    gene5                                                                                            */
/*   6    gene1-ID001    ID001                                                                                            */
/*   7    gene2-ID002    ID002                                                                                            */
/*   8    gene3-ID003    ID003                                                                                            */
/*   9    gene4-ID004    ID004                                                                                            */
/*  10    gene5-ID005    ID005                                                                                            */
/*                                                                                                                        */
/**************************************************************************************************************************/

proc sql;
  create
     table nrmb as
  select
     key
    ,scan(key,1,'-') as part
    ,lumins
  from
     sd1.b
;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  Obs    KEY            PART     LUMINS                                                                                 */
/*                                                                                                                        */
/*   1     gene1          gene1      22                                                                                   */
/*   2     ID002          ID002      13                                                                                   */
/*   3     gene3-ID003    gene3      11                                                                                   */
/*   4     gene4          gene4      15                                                                                   */
/*                                                                                                                        */
/**************************************************************************************************************************/

proc sql;
  create
     table want as
 select
     l.key
    ,r.lumins
 from
    nrma as l left join nrmb as r
 on
   l.part = r.part
 where
   not missing(r.part)
 order
   by key
;quit;

proc print data=want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*    Obs        KEY        LUMINS                                                                                        */
/*                                                                                                                        */
/*   1     gene1-ID001      22                                                                                            */
/*   2     gene2-ID002      13                                                                                            */
/*   3     gene3-ID003      11                                                                                            */
/*   4     gene4-ID004      15                                                                                            */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                     _     _  __
|___ \   _ __   ___  __ _| | __| |/ _|
  __) | | `__| / __|/ _` | |/ _` | |_
 / __/  | |    \__ \ (_| | | (_| |  _|
|_____| |_|    |___/\__, |_|\__,_|_|
                       |_|
*/

proc datasets lib=sd1 nodetails nolist;
 delete want;
run;quit;

%utl_rbeginx;
parmcards4;
library(haven)
library(sqldf)
source("c:/oto/fn_tosas9x.R");
a<-read_sas("d:/sd1/a.sas7bdat")
b<-read_sas("d:/sd1/b.sas7bdat")
want <- sqldf('
 select
     l.key
    ,r.lumins
 from
    (select
        key
       ,substr(key,1,5) as part
     from
        a
     union
        all
     select
        key
       ,substr(key,7,5) as part
     from
        a ) as l left join b as r
 on
   trim(l.part) = trim(substr(r.key,1,5))
 where
   lumins not null
');
want;
fn_tosas9x(
      inp    = want
     ,outlib ="d:/sd1/"
     ,outdsn ="want"
     );
;;;;
%utl_rendx;

libname sd1 "d:/sd1";
proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* R                                                                                                                      */
/* =                                                                                                                      */
/*                                                                                                                        */
/* > want;                                                                                                                */
/*           key LUMINS                                                                                                   */
/*                                                                                                                        */
/* 1 gene1-ID001     22                                                                                                   */
/* 2 gene3-ID003     11                                                                                                   */
/* 3 gene4-ID004     15                                                                                                   */
/* 4 gene2-ID002     13                                                                                                   */
/*                                                                                                                        */
/*                                                                                                                        */
/* SAS                                                                                                                    */
/* ===                                                                                                                    */
/*                                                                                                                        */
/* ROWNAMES        KEY        LUMINS                                                                                      */
/*                                                                                                                        */
/*     1       gene1-ID001      22                                                                                        */
/*     2       gene3-ID003      11                                                                                        */
/*     3       gene4-ID004      15                                                                                        */
/*     4       gene2-ID002      13                                                                                        */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____               _   _                             _     _  __
|___ /   _ __  _   _| |_| |__   ___  _ __    ___  __ _| | __| |/ _|
  |_ \  | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |/ _` | |_
 ___) | | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | | (_| |  _|
|____/  | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|\__,_|_|
        |_|    |___/                                |_|
*/

proc datasets lib=sd1 nodetails nolist;
 delete want;
run;quit;

%utl_pybeginx;
parmcards4;
import pyperclip
import os
from os import path
import sys
import subprocess
import time
import pandas as pd
import pyreadstat as ps
import numpy as np
import pandas as pd
from pandasql import sqldf
mysql = lambda q: sqldf(q, globals())
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
mysql = lambda q: sqldf(q, globals())
a, meta = ps.read_sas7bdat("d:/sd1/a.sas7bdat")
b, meta = ps.read_sas7bdat("d:/sd1/b.sas7bdat")
exec(open('c:/temp/fn_tosas9.py').read())
want = pdsql("""
 select
     l.key
    ,r.lumins
 from
    (select
        key
       ,substr(key,1,5) as part
     from
        a
     union
        all
     select
        key
       ,substr(key,7,5) as part
     from
        a ) as l left join b as r
 on
   trim(l.part) = trim(substr(r.key,1,5))
 where
   lumins not null
""")
print(want)
fn_tosas9(
   want
   ,dfstr="want"
   ,timeest=3
   )
;;;;
%utl_pyendx;

libname tmp "c:/temp";
proc print data=tmp.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* PYTHON                                                                                                                 */
/* ======                                                                                                                 */
/*             key  LUMINS                                                                                                */
/*  0  gene1-ID001    22.0                                                                                                */
/*  1  gene3-ID003    11.0                                                                                                */
/*  2  gene4-ID004    15.0                                                                                                */
/*  3  gene2-ID002    13.0                                                                                                */
/*                                                                                                                        */
/*                                                                                                                        */
/* SAS                                                                                                                    */
/* ===                                                                                                                    */
/*                                                                                                                        */
/* Obs        KEY        LUMINS                                                                                           */
/*                                                                                                                        */
/*  1     gene1-ID001      22                                                                                             */
/*  2     gene3-ID003      11                                                                                             */
/*  3     gene4-ID004      15                                                                                             */
/*  4     gene2-ID002      13                                                                                             */
/*                                                                                                                        */
/**************************************************************************************************************************/
/*  _                 _                               _                            _
| || |    _ __  _   _| |_ ___  _ __   _ __   ___  ___| |_ __ _ _ __ ___  ___  __ _| |
| || |_  | `_ \| | | | __/ _ \| `_ \ | `_ \ / _ \/ __| __/ _` | `__/ _ \/ __|/ _` | |
|__   _| | |_) | |_| | || (_) | | | || |_) | (_) \__ \ || (_| | | |  __/\__ \ (_| | |
   |_|   | .__/ \__, |\__\___/|_| |_|| .__/ \___/|___/\__\__, |_|  \___||___/\__, |_|
         |_|    |___/                |_|                 |___/                  |_|
*/

proc datasets lib=sd1 nodetails nolist;
 delete want;
run;quit;

%utl_pybeginx;
parmcards4;
import pyperclip
import os
from os import path
import sys
import subprocess
import time
import pandas as pd
import pyreadstat as ps
import numpy as np
import pandas as pd
from pandasql import sqldf
mysql = lambda q: sqldf(q, globals())
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
mysql = lambda q: sqldf(q, globals())
from sqlalchemy import create_engine
import psycopg2
exec(open('c:/temp/fn_tosas9.py').read())
a, meta = ps.read_sas7bdat("d:/sd1/a.sas7bdat")
b, meta = ps.read_sas7bdat("d:/sd1/b.sas7bdat")

# PostgreSQL connection parameters

# Define connection parameters
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    user="postgres",
    password="Sas2@rlx",
    database="devel",
    options="-c search_path=demographics"
)

query =                                    \
 "select                                   \
     l.key                                 \
    ,r.lumins                              \
 from                                      \
    (select                                \
        key                                \
       ,substr(key,1,5) as part            \
     from                                  \
        a                                  \
     union                                 \
        all                                \
     select                                \
        key                                \
       ,substr(key,7,5) as part            \
     from                                  \
        a ) as l left join b as r          \
 on                                        \
   trim(l.part) = trim(substr(r.key,1,5))  \
 where                                     \
   lumins is not null"
# Execute the query and load results into a DataFrame
want = pd.read_sql_query(query, conn)
print(want)
fn_tosas9(
    want
   ,dfstr="want"
   ,timeest=3
   )
conn.close()
;;;;
%utl_pyendx;

libname tmp "c:/temp";
proc print data=tmp.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* PYTHON                                                                                                                 */
/* ======                                                                                                                 */
/*                                                                                                                        */
/*             key  lumins                                                                                                */
/*  0  gene1-ID001    22.0                                                                                                */
/*  1  gene3-ID003    11.0                                                                                                */
/*  2  gene4-ID004    15.0                                                                                                */
/*  3  gene2-ID002    13.0                                                                                                */
/*                                                                                                                        */
/* SAS                                                                                                                    */
/* ===                                                                                                                    */
/*                                                                                                                        */
/*  Obs        key        lumins                                                                                          */
/*                                                                                                                        */
/*   1     gene1-ID001      22                                                                                            */
/*   2     gene3-ID003      11                                                                                            */
/*   3     gene4-ID004      15                                                                                            */
/*   4     gene2-ID002      13                                                                                            */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                           _                            _
| ___|   _ __   _ __   ___  ___| |_ __ _ _ __ ___  ___  __ _| |
|___ \  | `__| | `_ \ / _ \/ __| __/ _` | `__/ _ \/ __|/ _` | |
 ___) | | |    | |_) | (_) \__ \ || (_| | | |  __/\__ \ (_| | |
|____/  |_|    | .__/ \___/|___/\__\__, |_|  \___||___/\__, |_|
               |_|                 |___/                  |_|
*/

proc datasets lib=sd1 nodetails nolist;
 delete want;
run;quit;

%utl_rbeginx;
parmcards4;
library(RPostgres)
library(DBI)
library(haven)
source("c:/oto/fn_tosas9x.R");
a<-read_sas("d:/sd1/a.sas7bdat")
b<-read_sas("d:/sd1/b.sas7bdat")
con <- dbConnect(RPostgres::Postgres(),
            dbname = "devel",  # Use the default 'postgres' database
            host = "localhost",   # Replace with your PostgreSQL server address if not local
            port = 5432,          # Default PostgreSQL port
            user = "postgres",
            password = "12345678")
dbExecute(con, "SET search_path TO demographics")
dbListObjects(con, Id(schema = 'demographics'))
dbWriteTable(conn=con,name="a",value=a,row.names = FALSE, overwrite = TRUE)
dbWriteTable(conn=con,name="b",value=b,row.names = FALSE, overwrite = TRUE)
query <- "
  select
     l.key
    ,r.lumins
 from
    (select
        key
       ,substr(key,1,5) as part
     from
        a
     union
        all
     select
        key
       ,substr(key,7,5) as part
     from
        a ) as l left join b as r
 on
   trim(l.part) = trim(substr(r.key,1,5))
 where                                     \
   lumins is not null"
df <- dbGetQuery(con, query)
dbListObjects(con, Id(schema = 'demographics'))
dbDisconnect(con)
df;
fn_tosas9x(
      inp    = df
     ,outlib ="d:/sd1/"
     ,outdsn ="want"
     );
');
;;;;
%utl_rendx;

proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* R                                                                                                                      */
/* ======                                                                                                                 */
/*                                                                                                                        */
/*             key  lumins                                                                                                */
/*  1  gene1-ID001    22.0                                                                                                */
/*  2  gene3-ID003    11.0                                                                                                */
/*  3  gene4-ID004    15.0                                                                                                */
/*  4  gene2-ID002    13.0                                                                                                */
/*                                                                                                                        */
/* SAS                                                                                                                    */
/* ===                                                                                                                    */
/*                                                                                                                        */
/*  Obs        key        lumins                                                                                          */
/*                                                                                                                        */
/*   1     gene1-ID001      22                                                                                            */
/*   2     gene3-ID003      11                                                                                            */
/*   3     gene4-ID004      15                                                                                            */
/*   4     gene2-ID002      13                                                                                            */
/*                                                                                                                        */
/**************************************************************************************************************************/


/*__              _       _           _
 / /_    _ __ ___| | __ _| |_ ___  __| |  _ __ ___ _ __   ___  ___
| `_ \  | `__/ _ \ |/ _` | __/ _ \/ _` | | `__/ _ \ `_ \ / _ \/ __|
| (_) | | | |  __/ | (_| | ||  __/ (_| | | | |  __/ |_) | (_) \__ \
 \___/  |_|  \___|_|\__,_|\__\___|\__,_| |_|  \___| .__/ \___/|___/
                                                  |_|
*/


https://github.com/rogerjdeangelis/mySQL-uml-modeling-to-create-entity-diagrams-for-sas-datasets
https://github.com/rogerjdeangelis/utl-PASSTHRU-to-mysql-and-select-rows-based-on-a-SAS-dataset-without-loading-the-SAS-daatset-into-my
https://github.com/rogerjdeangelis/utl-comparison-between-python-and-sql-programming
https://github.com/rogerjdeangelis/utl-converting-sas-proc-sql-code-to-r-and-python
https://github.com/rogerjdeangelis/utl-formatting-your-proc-sql-code
https://github.com/rogerjdeangelis/utl-missing-basic-math-and-stat-functions-in-python-sqlite3-sql
https://github.com/rogerjdeangelis/utl-passing-arguments-to-sqldf-using-wps-python-f-text-function
https://github.com/rogerjdeangelis/utl-passing-arguments-to-sqldf-wps-r-sql-functional-sql
https://github.com/rogerjdeangelis/utl-sas-proc-transpose-in-sas-r-wps-python-native-and-sql-code
https://github.com/rogerjdeangelis/utl-sas-proc-transpose-wide-to-long-in-sas-wps-r-python-native-and-sql
https://github.com/rogerjdeangelis/utl-select-the-first-two-observations-from-each-group-wps-r-python-sql
https://github.com/rogerjdeangelis/utl-select-the-n-largest-number-using-sas-sql
https://github.com/rogerjdeangelis/utl-simple-classic-transpose-pivot-wider-in-native-and-sql-wps-r-python
https://github.com/rogerjdeangelis/utl-simple-example-of-sql-array
https://github.com/rogerjdeangelis/utl-simple-example-of-sql-array
https://github.com/rogerjdeangelis/utl-sqlite-processing-in-python-with-added-math-and-stat-functions
https://github.com/rogerjdeangelis/utl-transpose-pivot-wide-using-sql-partitioning-in-wps-r-python
https://github.com/rogerjdeangelis/utl-transposing-and-summarizing-by-group-using-sql-arrays
https://github.com/rogerjdeangelis/utl-transposing-multiple-variables-using-transpose-macro-sql-arrays-proc-report
https://github.com/rogerjdeangelis/utl-transposing-pivoting-minimums-using-sql-arrays
https://github.com/rogerjdeangelis/utl-transposing-rows-to-columns-using-proc-sql-partitioning
https://github.com/rogerjdeangelis/utl-using-sql-in-wps-r-python-select-the-four-youngest-male-and-female-students-partitioning
https://github.com/rogerjdeangelis/utl_explicit_pass_through_to_mysql_to_subset_a_table_using_macro_variable_dates
https://github.com/rogerjdeangelis/utl_sql_operational_order_of_sql_statements
https://github.com/rogerjdeangelis/utl_transpose_with_proc_sql
https://github.com/rogerjdeangelis/utl_sql_update_master_using_a_transaction_table_mysql_database
https://github.com/rogerjdeangelis/utl_sql_version_of_proc_transpose_with_major_advantage_of_summarization
https://github.com/rogerjdeangelis/utl_using-sql-instead-of-the-family-of-R-apply-functions
https://github.com/rogerjdeangelis/utl_with_a_press_of_a_function_key_convert_the_highlighted_dataset_to_a_mysql_database_table

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/



