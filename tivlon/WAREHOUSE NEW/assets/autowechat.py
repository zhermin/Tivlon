import pyautogui, pyperclip, time, os, random, csv
pyautogui.FAILSAFE = True
testing = False

wechat = os.path.join("C:", os.sep, "Program Files (x86)", "Tencent", "WeChat", "WeChat.exe")
contacts_list = {row[0]:row[1] for row in csv.reader(open(os.path.join('assets' if not testing else '', 'wechat_contacts.csv')))}

all_options = "\n".join([f"{index+1}) {key} ({contacts_list[key]} contacts)" for index,key in enumerate(contacts_list)])
invalid = False
invalid_msg = "INVALID INPUT, PLEASE RETRY\n\n"

while True: # choose contact name
    try:
        contact = pyautogui.confirm(f"{invalid_msg if invalid else ''}Please select the options\n\n{all_options}", buttons=[*contacts_list]+["custom"])

        if contact == None: raise SystemExit(0)
        elif contact == "custom": 
            contact = pyautogui.prompt(f"Please type in the custom contact to search for").upper()
            if contact == None:
                invalid = False
                continue
            elif not contact: raise ValueError("Invalid Input")

            num_contacts = int(pyautogui.prompt(f"Please type in the number of contacts for '{contact}'"))
            if num_contacts == None: raise ValueError("Quit custom number of contacts field")
            elif num_contacts <= 0: raise ValueError("Invalid Input")

            contacts_list[contact] = num_contacts

        invalid = False
        break
    except Exception as e:
        # print(e)
        invalid = True
        continue

while True: # number of contacts
    try:
        num_contacts = pyautogui.confirm(f"{invalid_msg if invalid else ''}Searching for {contact} with {contacts_list[contact]} contacts", buttons=[f"{contacts_list[contact]} contacts", "custom"])

        if num_contacts == None: raise SystemExit(0)
        elif num_contacts == "custom":
            num_contacts = int(pyautogui.prompt(f"Please type in the correct number of contacts"))
            if num_contacts == None: raise ValueError("Quit custom number of contacts field")
            contacts_list[contact] = num_contacts

        invalid = False
        break
    except Exception as e:
        # print(e)
        invalid = True
        continue

while True: # message to be sent
    try:
        msg = pyautogui.prompt(f"Please paste in your message")
        if msg == None: raise SystemExit(0)
        elif not msg:
            raise ValueError("Invalid Input")
        pyperclip.copy(msg)
        break
    except Exception as e: continue

while True: # delay between messages
    try:
        msg_interval = int(pyautogui.prompt(f"{invalid_msg if invalid else ''}Input time intervals between messages (in seconds)"))

        if msg_interval == None: raise SystemExit(0)
        elif msg_interval < 0: raise ValueError("Invalid Input")

        invalid = False
        break
    except Exception as e:
        # print(e)
        invalid = True
        continue

while True: # number of contacts in each batch
    try:
        batch_number = int(pyautogui.prompt(f"{invalid_msg if invalid else ''}Input number of contacts per batch"))

        if batch_number == None: raise SystemExit(0)
        elif batch_number <= 0: raise ValueError("Invalid Input")

        invalid = False
        break
    except Exception as e:
        # print(e)
        invalid = True
        continue

while True: # delay between each batch
    try:
        batch_delays = int(pyautogui.prompt(f"{invalid_msg if invalid else ''}Input delay after every {batch_number} contacts (in seconds)"))

        if batch_delays == None: raise SystemExit(0)
        elif batch_delays < 0: raise ValueError("Invalid Input")

        invalid = False
        break
    except Exception as e:
        # print(e)
        invalid = True
        continue

# confirmation and show all user inputs
final_confirm = pyautogui.alert(f"Interval between messages: {time.strftime('%Hh %Mm %Ss', time.gmtime(msg_interval))}\nDelay after every {batch_number} contacts: {time.strftime('%Hh %Mm %Ss', time.gmtime(batch_delays))}\n\nMessage to {contact} ({contacts_list[contact]} contacts):\n\n{msg}")
if final_confirm == None: raise SystemExit(0)

print(f"contact: {contact} ({contacts_list[contact]} contacts)\nmessage:\n{msg}")

os.startfile(wechat) # open wechat app
time.sleep(2)

current_batch = 1
for n in range(contacts_list[contact]):
    msg_interval_cd, batch_delays_cd = msg_interval + random.uniform(-1,1), batch_delays + random.uniform(-1,1)

    print(f"\n\nchat #{n+1} complete (delay: {msg_interval_cd:.1f}s)")
    while msg_interval_cd > 0: # msg_interval between each message
        print(f"next message in: {time.strftime('%Hh %Mm %S.', time.gmtime(msg_interval_cd))}{str(msg_interval_cd).split('.')[1][:1]}s", end="\r")
        msg_interval_cd -= 0.1
        time.sleep(0.1)

    print("\nsending...")
    pyautogui.PAUSE = 1.5
    pyautogui.hotkey("ctrl", "f") # search shortcut
    pyautogui.typewrite(f"{contact}") # paste contact into search bar

    for i in range(1, n+1): # find the correct chat by spamming down button
        pyautogui.PAUSE = 0.1
        pyautogui.hotkey("down") # traverse through each contact
        if i == 5: pyautogui.hotkey("enter") # show all

    pyautogui.PAUSE = 1
    pyautogui.hotkey("enter") # open chat

    pyautogui.PAUSE = 0.5
    pyautogui.hotkey("ctrl", "v") # paste
    pyautogui.hotkey("enter") # send message

    print("sent!")
    if (n+1) % batch_number == 0 and n < contacts_list[contact] - 1: # every n contacts
        print(f"\n\n*batch #{current_batch} complete (delay: {batch_delays_cd:.1f}s)")
        current_batch += 1
        while batch_delays_cd > 0:
            print(f"next batch in: {time.strftime('%Hh %Mm %S.', time.gmtime(batch_delays_cd))}{str(batch_delays_cd).split('.')[1][:1]}s", end="\r")
            batch_delays_cd -= 0.1
            time.sleep(0.1)

print("\n\n\n[DONE]")
input("press enter to exit...")