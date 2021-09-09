from __future__ import print_function
import grpc
import requests
import threading
import io
import pubsub_api_pb2 as pb2
import pubsub_api_pb2_grpc as pb2_grpc
import avro.schema
import avro.io
import datetime
import certifi
##############################################
import pprint as pp 
##############################################
import json
with open('config.json') as json_file:
    myConfig=json.load(json_file)
myUserName = myConfig['username']
myPassword = myConfig['password']
myOrgId = myConfig['orgId']
myDomain = myConfig['domain']
myInstanceUrl = myConfig['instanceurl']
myLoginUrl = myConfig['loginurl']
##############################################
from xml.etree import cElementTree as ET
from collections import defaultdict

def etree_to_dict(t):
    d = {t.tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.items():
                dd[k].append(v)
        d = {t.tag: {k:v[0] if len(v) == 1 else v for k, v in dd.items()}}
    if t.attrib:
        d[t.tag].update(('@' + k, v) for k, v in t.attrib.items())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
              d[t.tag]['#text'] = text
        else:
            d[t.tag] = text
    return d
##############################################
def fetchReqStream(topic):
    while True:
        semaphore.acquire()
        yield pb2.FetchRequest(
            topic_name = topic,
            replay_preset = pb2.ReplayPreset.LATEST,
            num_requested = 1)
def decode(schema, payload):
    schema = avro.schema.Parse(schema)
    buf = io.BytesIO(payload)
    decoder = avro.io.BinaryDecoder(buf)
    reader = avro.io.DatumReader(writer_schema=schema)
    ret = reader.read(decoder)
    return ret
##############################################
print('hello')
"""
Set a semaphore at the beginning of the program. Because of the way Python gRPC is
designed, the program shuts down immediately if no response comes back in the
milliseconds between calling an RPC and the end of the program. By setting a
semaphore, you cause the client to keep running indefinitely.
"""
semaphore = threading.Semaphore(1)
"""
Create a global variable to store the replay ID.
"""
latest_replay_id = None
with open(certifi.where(), 'rb') as f:
    creds = grpc.ssl_channel_credentials(f.read())
    with grpc.secure_channel('eventbusapi-core1.sfdc-ypmv18.svc.sfdcfc.net:7443', creds) as channel:
        #All of the code in the rest of the tutorial will go inside
        # this block
    
        url =myLoginUrl 
    
    
        payload = "<soapenv:Envelope\nxmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/'\n"
        payload = payload+"xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'\n"
        payload = payload+"xmlns:urn='urn:partner.soap.sforce.com'><soapenv:Body>\n"
        payload = payload+"<urn:login> "
        payload = payload+"<urn:username><![CDATA["+myUserName+"]]></urn:username>\n"
        payload = payload+"<urn:password><![CDATA["+myPassword+"]]></urn:password>\n"
        payload = payload+"</urn:login></soapenv:Body></soapenv:Envelope>"

        headers = {
        "cookie": "BrowserId=-r07QQ_nEeyiUm-DzrfKRA",
        "Content-Type": "text/xml",
        "SOAPAction": "login",
        "Authorization": "Basic Og=="
        }
        res = requests.post(url, data=payload, headers=headers, verify=False)
        #Optionally, print the content field returned
        #print(res.content)
        tree = ET.XML(res.content)
        #print('*****************')
        xmldict = etree_to_dict(tree)
        pp.pprint(xmldict)
        envdict = xmldict['{http://schemas.xmlsoap.org/soap/envelope/}Envelope']
        bodydict = envdict['{http://schemas.xmlsoap.org/soap/envelope/}Body']
        logindict= bodydict['{urn:partner.soap.sforce.com}loginResponse']['{urn:partner.soap.sforce.com}result']
        #
        sessionId = logindict['{urn:partner.soap.sforce.com}sessionId']
        instanceUrl = myInstanceUrl
        #instanceUrl = logindict['{urn:partner.soap.sforce.com}serverUrl']
        #instanceUrl = instanceUrl[:instanceUrl.find('/services')]
        myDomain = myDomain 
        #myDomain = instanceUrl[8:instanceUrl.find('.my')] # 'se' 
        orgId = myOrgId
        orgId = logindict['{urn:partner.soap.sforce.com}userInfo']['{urn:partner.soap.sforce.com}organizationId']
        tenantid = "core/"+ myDomain +"/"+orgId
        #
        authmetadata = (('x-sfdc-api-session-token', sessionId),
                    ('x-sfdc-instance-url', instanceUrl),
                    ('x-sfdc-tenant-id', tenantid))
        print(authmetadata)
        stub = pb2_grpc.PubSubStub(channel)
        print(stub)
        mysubtopic = "/event/Order_Event__e"
        substream = stub.Subscribe(fetchReqStream(mysubtopic),metadata=authmetadata)
        for event in substream:
            semaphore.release()
            if event.events:
                payloadbytes = event.events[0].event.payload
                schemaid = event.events[0].event.schema_id
                schema = stub.GetSchema(
                    pb2.SchemaRequest(schema_id=schemaid),
                    metadata=authmetadata).schema_json
                decoded = decode(schema, payloadbytes)
                print("Got an event!")
                pp.pprint(decoded)
            else:
                print("[",datetime.datetime.now(),"] The subscription is active.")
            latest_replay_id = event.latest_replay_id