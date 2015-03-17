import json

class Task:
    def init(self, task_config):
        print task_config

    def run(self, header, body):
        #for key, val in msg.properties.items():
        #    print ('%s: %s' % (key, str(val)))
        #for key, val in msg.delivery_info.items():
        #    print ('> %s: %s' % (key, str(val)))

        #print ('')
        #print (msg.body)
        #print ('-------')
        #print msg
        #data = json.loads(msg)
        #print data
        #print data["SecurityId"],data["SendingTime"],data["LastPx"]
        print body
        return True
        

    def stop(self):
        pass

    
