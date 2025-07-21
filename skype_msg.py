from skpy import Skype

# Define your Skype username and password
username = 'product_eng@tallykhata.com'
password = ''

# Create a Skype object and log in with your Skype credentials
# sk = Skype(username, password)
# ch = sk.contacts['live:.cid.e24b0e5ba588df56'].chat
# ch.sendMsg("Hello, this is an automated SkPy message!")

techops_id = '19:e21174b60c114ababa6d0f95d7eb7e08@thread.skype'
alert_id = '19:e21174b60c114ababa6d0f95d7eb7e08@thread.skype'
#19:1869036a197849f6952171c025c1de7a@thread.skype
#19:445a36a730cb4de2b752fa2d1ad45c72@thread.skype

def send_skype_msg(msg):
    try:
        sk.chats.chat(alert_id).sendMsg(msg)
    except Exception as e:
        sk.chats.chat(techops_id).sendMsg(f"Try block code failed. This is from except block. Error encountered {e}")

# send_skype_msg('Hello! iBot testing')