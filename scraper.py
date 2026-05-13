import pipleline

pipleline.download_ea_text_content()

current_servers = pipleline.download_servers()

for server in current_servers:
    pipleline.download_server_snapshot(server['server_guid'])

