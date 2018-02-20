import ldap
import sys
sys.path.append('/home/nate/source')
from caeser import config

ldap_params = config.ldap
uri = "{host}:{port}".format(**ldap_params)
l = ldap.initialize(uri)
user = ldap_params['user']
password = ldap_params['password']
user_dn = "cn={},ou=UoMPeople,dc=uom,dc=memphis,dc=edu".format(user)
l.simple_bind_s(user_dn, password)
base_dn = "dc=uom,dc=memphis,dc=edu"
search = "cn=CPGIS_users"
org_dn = "ou=CPGIS," + base_dn
members = l.search_s(org_dn, ldap.SCOPE_SUBTREE, search)

for member in members[0][1]['member']:
    member_dn = member.split(',')[0]
    uid = member_dn.replace("CN=", "")
    member_detail = l.search_s(base_dn, ldap.SCOPE_SUBTREE, member_dn)
    if 'description' not in member_detail[0][1].keys():
        print '\nmissing', uid, '\n'
        continue
    name = member_detail[0][1]['description'][0]
    print uid, '\t', name


