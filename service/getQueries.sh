#In full the program, loads output from file (argument 1), removes lines that don't contain queries
# (lines that don't start with '>'), then removes calls that come from H2O and not the application.
# It then sends the result to a file (argument 2).

#-Ev removes line containing a match for the given expression. In this case it 
# removes the commits that H2O creates, rather than the application.

cat $1 | grep '^>' | grep -Ev "commit TRANSACTION|PREPARE COMMIT|DELETE FROM SESSIONS|COMMIT TRANSACTION" > $2
