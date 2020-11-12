import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((socket.gethostname(), 1234))

block = []
full_message = ''

while True:
    msg = s.recv(8).decode('utf-8')
    if len(msg) <= 0:
        break
    block.append(msg)
    full_message += msg

print(block)
print(full_message)
