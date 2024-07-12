pip install ldap3

 

from ldap3 import Server, Connection, ALL

# Define the server and connection details
server = Server('ldap://10.100.12.210', get_info=ALL)
conn = Connection(server, user='rdiusr', password='2UC9ze2UC9ze2UC9ze#', auto_bind=True)

# Check if the connection is bound successfully
if conn.bind():
   print('Successfully connected to the LDAP server')
else:
   print('Failed to connect to the LDAP server')

# Perform a search operation
conn.search('dc=example,dc=com', '(objectclass=person)', attributes=['cn', 'sn', 'mail'])

# Print the results
for entry in conn.entries:
   print(entry)

# Unbind the connection
conn.unbind()