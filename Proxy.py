import paho.mqtt.client as mqtt	
import threadpool,time
from azure.servicebus import ServiceBusService

eventPool=threadpool.ThreadPool(1)

def init_sbs():
	# api_key=dict(namespace='AirForceUAV-ns',policy_name='RootManageSharedAccessKey',policy_secret='3bP2rrfIKLbWkQvSwBEJB1iawxhwUdoBC/lDYbRReSI=',host_base='.servicebus.chinacloudapi.cn')
	api_key=dict(namespace='airforceuav',policy_name='RootManageSharedAccessKey',policy_secret='mdq0pk8QTd/VXelOfL7VgQtJQ4Xto2HtVs0rfF2JuOE=',host_base='.servicebus.windows.net')
	sbs = ServiceBusService(api_key["namespace"], shared_access_key_name=api_key["policy_name"], shared_access_key_value=api_key["policy_secret"],host_base=api_key['host_base'])
	return sbs
def init_mqtt():
	client = mqtt.Client(client_id='Ran',clean_session=True,userdata=None)
	client.reinitialise(client_id='Ran',clean_session=True, userdata=None)
	client.on_connect = on_connect
	client.on_message = on_message
	client.connect("139.217.26.207", 1883)
	client.loop_start()	
	return client

def on_message(client, userdata, msg):
	global eventPool
	requests = threadpool.makeRequests(push_wrapper,(str(msg.payload),))
	[eventPool.putRequest(req) for req in requests]

def on_connect(client, userdata, rc):
	print("Connected mqtt with result code "+str(rc))
	client.subscribe("FlightLog",qos=1)

def push_wrapper(msg):
	push(msg)

def push(msg):
	global sbs
	# print(msg)
	sbs.send_event('airforceuav',msg)

if __name__=="__main__":
	sbs=init_sbs()
	mqtt=init_mqtt()
	while True:
		time.sleep(60)
