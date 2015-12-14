#
#
#	Sample Python script to dump data from an Oracle database
#
#	To Use:
#	   Create a named pipe (or flat file) on the system you will be running
#			gpfdist from.
#		Make sure you start writing to the named pipe before trying to 
#			select from a GPDB external table.
# 	   Writing to a named pipe starts to break down when the fetched data set
#   		gets above 500 million rows or so.  YMMV.
#
#	Bart Kersteter - bkersteter@gopivotal.com

import cx_Oracle
import csv
import time

# connect via SQL*Net string or by each segment in a separate argument
#connection = cx_Oracle.connect("user/password@TNS")
start=time.time()
#quotes are needed for line below. 
connection = cx_Oracle.connect("<userid>", "<password>", "<ORACLE_SID>")

# We want to dump the data as pipe-delimited data.
csv.register_dialect('pipe_delimited', delimiter='|', escapechar='\\', quoting=csv.QUOTE_NONE)

cursor = connection.cursor()
cursor.arraysize=20000
#
#	Note we're dumping to a pre-existing fifo.
#
f = open("/home/gpadmin/vrmspoc/data/my_fifo.fifo", "w")
writer = csv.writer(f, dialect='pipe_delimited', lineterminator="\n")
#
# In this case at least, the three sets of double-quotes were necessary.  No semicolon needed.
#
r = cursor.execute("""<select query from source>""")
#for row in cursor:
#	writer.writerow(row)
done = False
rowcount = 0
while not done:
        rows = cursor.fetchmany()
        if rows == []:
            done = True
	writer.writerows(rows)
        rowcount+=20000
        print "Rows Processed: ", rowcount

f.close()
end=time.time()
elapsed=end-start
print "Total execution time: ", elapsed, " seconds."

