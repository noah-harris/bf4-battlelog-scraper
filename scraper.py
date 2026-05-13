import pipleline
import time

def run():
    pipleline.download_ea_text_content()
    CYCLE_LENGTH_MIN = 60
    cycle = 0
    while True:
        if cycle % CYCLE_LENGTH_MIN == 0:
            current_servers = pipleline.download_servers()
            for server in current_servers:
                pipleline.download_server_snapshot(server['server_guid'])
        cycle += 1
        time.sleep(60)
