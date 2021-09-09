import os


myConfig = {"username":"","userid":"","instanceurl":"","loginurl":"","password":"","domain":"","orgId":""}
for v in myConfig.keys():
    if v=='username':
        print (v,':',os.environ.get(v,'unknown'))
    myConfig[v] = os.environ.get(v,'unknown')