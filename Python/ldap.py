pip install ldap3

 

from ldap3 import Server, Connection, ALL

# Define the server and connection details
server = Server('ldap://your_ldap_server', get_info=ALL)
conn = Connection(server, user='your_username', password='your_password', auto_bind=True)

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