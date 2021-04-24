# SQL-Redis-Zato
A Zato service for stocking the output of a SQL request in Redis.

The two files RequestSqlServiceFinal.py and SqlToRedisServiceFinal.py are both a Zato service. 

RequestSqlServiceFinal.py executes a SQL request and store the output as a list of dictionnary, where the keys of the dictionnary are the name of the columns. It requires three inputs: 
 - 'query': the SQL request itself, 
 - 'out_name': the name of the "Outgoing SQL connections",
 - and 'operation': "select", "insert", "update" or "delete" (need to commit the change for the last 3 operations)

SqlToRedisServiceFinal.py invokes the service RequestSqlServiceFinal.py synchronously in oder to get the SQL output. Then it stores this output in Redis as a dictionnary where the key of each line is the value of a given column. The given column is advised to be the column to which a jointure is done. This service requires four inputs:
 - 'query': the SQL request itself, 
 - 'out_name': the name of the "Outgoing SQL connections",
 - 'output_dict_name': Redis key, 
 - 'output_dict_key': this is the name of the column that its value will used as the key in the dictionnary output. 


