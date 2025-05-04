import json
import random
import socket
import sys
import threading

import pandas as pd
from sympy import nextprime

name, l, manager_ip, manager_port = None, None, None, None
table = {}
m_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
p_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# Function for building/rebuilding the DHT
def leader(data, rebuild):
    global l, manager_ip, manager_port

    # How many entries is stored in each node
    leader_output = {}

    # 'Peers' is the list of peers in the dht
    peers = data['peers']

    # Get the download url for the entered year and download the spreadsheet
    year = data['year']
    url = f'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/StormEvents_details-ftp_v1.0_d{year}_c20250401.csv.gz'
    storm = pd.read_csv(url, compression="gzip", low_memory=False)
    storm.columns = storm.columns.str.lower()

    # Remove all columns other than the ones below from the dataframe
    columns = [
        "event_id",
        "state",
        "year",
        "month_name",
        "event_type",
        "cz_type",
        "cz_name",
        "injuries_direct",
        "injuries_indirect",
        "deaths_direct",
        "deaths_indirect",
        "damage_property",
        "damage_crops",
        "tor_f_scale"
    ]
    storm = storm[columns]

    # Set l and s as specified in the project doc
    l = len(storm['event_id'])
    s = nextprime(l * 2)

    # Loop through each entry in the dataframe
    for i, row in storm.iterrows():

        # Set pos and id as specified in the project doc
        pos = int(row['event_id']) % s
        id_of_peer_to_store = pos % len(peers)

        # Assemble output: count how many entries are stored in each peer
        try:
            leader_output[peers[str(id_of_peer_to_store)]['name']] += 1
        except Exception:
            try:
                leader_output[peers[str(id_of_peer_to_store)]['name']] = 1
            except Exception:
                try:
                    leader_output['peer'] += 1
                except Exception:
                    leader_output['peer'] = 1

        # Assemble data to send to peer
        entry = row.to_dict()
        data = {
            "command": "store",
            "entry": entry,
            "peers": peers,
            "peer": 0,
            "pos": pos,
            "id": id_of_peer_to_store,
            "l": l
        }

        # Send data to leader (this peer)
        p_socket.sendto(json.dumps(data).encode(), (peers['0']['ip'], int(peers['0']['port'])))

    # Send either dht-rebuilt or dht-complete depending on whether dht is new or rebuilt
    if rebuild:
        # dht-rebuilt <name> <new_leader_name> (to be sent to manager)
        data = f'dht-rebuilt {name} {name}'.encode()
        m_socket.sendto(data, (manager_ip, manager_port))
        print('dht-rebuilt sent to manager')
    else:
        # dht-complete <name> (to be sent to manager)
        data = f'dht-complete {name}'.encode()
        m_socket.sendto(data, (manager_ip, manager_port))
        print('dht-complete sent to manager')

    # Receive SUCCESS or FAILURE from manager
    # If success, print output
    response, _ = m_socket.recvfrom(1024)
    if response.decode() == 'SUCCESS':
        for key, value in sorted(leader_output.items()):
            print(f'{key}: {value} entries stored')
        print("DHT setup successful")
    else:
        print("Error: DHT setup failed")
        return

# Function for listening for commands being sent to the peer socket
def listen():
    global name, table, p_socket, l

    # Listen for data
    while True:

        # Receive data from manager or peer
        data, _ = p_socket.recvfrom(65535)
        data = json.loads(data.decode())
        command = data['command']

        # On command set-id, start a leader thread to build the dht
        if command == 'set-id':
            threading.Thread(target=leader, args=(data, False), daemon=True).start()

        # On command store, check if data belongs to this peer
        # If yes, store the data, if no, send it to the next peer
        elif command == 'store':
            peers = data['peers']
            id_of_peer_to_store = data['id']
            l = data['l']

            # Check if the peer the data belongs to is this peer
            # If so, store the data in the table at pos
            if peers[str(id_of_peer_to_store)]['name'] == name:
                entry = data['entry']
                pos = data['pos']
                table[int(pos)] = entry

            # Otherwise, send the data to the next peer in the ring
            else:
                data['peer'] = int(data['peer']) + 1
                right_peer = data['peer']
                data = json.dumps(data).encode()
                p_socket.sendto(
                    data, (peers[str(right_peer)]['ip'], int(peers[str(right_peer)]['port'])))

        # Calculate pos and id for entry, then send find-event
        elif command == 'query-dht':
            event_id = data['event_id']
            peer_id = str(data['peer'])
            peers = data['peers']

            # Calculate s, pos, and id for the entry
            s = nextprime(int(l) * 2)
            pos = int(event_id) % s
            id_of_peer_with_entry = pos % len(peers)

            # Send the command to the entered peer
            data = {
                "command": "find-event",
                "event_id": event_id,
                "peers": peers,
                "peer": peer_id,
                "id_seq": '',
                "pos": pos,
                "id": id_of_peer_with_entry,
                "original_peer_ip": data['original_peer_ip'],
                "original_peer_port": data['original_peer_port']
            }
            data = json.dumps(data).encode()
            p_socket.sendto(data, (peers[peer_id]['ip'], int(peers[peer_id]['port'])))

        # Check if this peer has the correct entry
        # If yes, return the entry, if no, send to next peer
        # If all peers are checked and no entry is found, return failure
        elif command == 'find-event':
            peers = data['peers']
            event_id = data['event_id']
            peer_id = str(data['peer'])
            id_seq = data['id_seq']
            pos = data['pos']
            id_of_peer_to_store = data['id']

            # IP and port to return the output to the peer where the command was entered
            original_peer_ip = data['original_peer_ip']
            original_peer_port = int(data['original_peer_port'])

            # Check if the entry belongs to this peer
            # If yes, return the entry
            if peers[str(id_of_peer_to_store)]['name'] == name:

                try:
                    id_seq += peer_id
                    entry = table[pos]
                    data = {
                        'command': 'return-event',
                        'status': 'SUCCESS',
                        'id_seq': id_seq,
                        'entry': entry,
                        'event_id': event_id
                    }

                    # Send the entry to the peer where the command was entered
                    data = json.dumps(data).encode()
                    p_socket.sendto(data, (original_peer_ip, original_peer_port))

                # If this peer does not have the entry, return failure
                except Exception:
                    data = {
                        'command': 'return-event',
                        'status': 'FAILURE',
                        'event_id': event_id
                    }

                    data = json.dumps(data).encode()
                    p_socket.sendto(data, (original_peer_ip, original_peer_port))

            # Otherwise, send it to a random unvisited peer
            else:
                id_seq += peer_id
                visited = list(id_seq)
                visited = [int(i) for i in visited]
                unvisited = list(set(range(len(peers))) - set(visited))

                # If all peers have been visited, return failure
                if not unvisited:
                    data = {
                        'command': 'return-event',
                        'status': 'FAILURE',
                        'event_id': event_id
                    }

                    data = json.dumps(data).encode()
                    p_socket.sendto(data, (original_peer_ip, original_peer_port))

                # Send the entry to a random unvisited peer
                else:
                    peer_id = str(random.choice(unvisited))
                    data['peer'] = peer_id
                    data['id_seq'] = id_seq
                    data = json.dumps(data).encode()
                    p_socket.sendto(
                        data, (peers[peer_id]['ip'], int(peers[peer_id]['port'])))

        # Command to receive and print a retrieved entry
        elif command == 'return-event':
            event_id = data['event_id']

            # If the entry was not found, print an error
            if data['status'] == 'FAILURE':
                print(f'Storm event {event_id} not found in the DHT.')

            # Otherwise, print the entry and sequence of nodes
            else:
                entry = data['entry']

                # Check if the returned entry has the same id as was entered
                if str(entry['event_id']) != str(event_id):
                    # If not, output en error
                    print(f'Storm event {event_id} not found in the DHT.')

                else:
                    id_seq = list(data['id_seq'])
                    print(f'Entry: {entry}')
                    print(f'Sequence of nodes: {id_seq}')

        # Command for a peer to leave the dht, which requires the dht to be destroyed and rebuilt
        elif command == 'leave-dht':

            # Clear the table of this peer
            table = {}
            peers = data['peers']
            year = data['year']

            # Get the peer to the right and left of this peer (u is name of left peer)
            right_peer = (int(data['peer_id']) + 1) % len(peers)
            u = int(data['peer_id']) - 1
            if u == -1:
                u = len(peers) - 1
            u = peers[str(u)]['name']

            # Send teardown-dht <leader_name> to manager
            leader_name = peers['0']['name']
            m_socket.sendto(
                f'teardown-dht {leader_name}'.encode(), (manager_ip, manager_port))
            print('teardown-dht sent to manager')
            response = m_socket.recvfrom(1024)
            if response == 'FAILURE':
                print('Failed tearing down DHT')
                continue

            # Send list of peers, right peer, and left peer to right peer to teardown the dht
            teardown_data = {
                'command': 'teardown-dht',
                'peers': peers,
                'u': u,
                'peer': right_peer
            }
            teardown_data = json.dumps(teardown_data).encode()
            p_socket.sendto(
                teardown_data, (peers[str(right_peer)]['ip'], int(peers[str(right_peer)]['port'])))

            sorted_items = sorted(peers.items(), key=lambda item: item[0])

            # Remove this peer from peers and reset ids of all peers
            new_peers = []
            for key, value in sorted_items:
                if key != str(data['peer_id']):
                    new_peers.append(value)
            peers = {}
            for i, new_peer in enumerate(new_peers):
                peers[str(i)] = new_peer

            # Rebuild the dht with the new list of peers
            data = {
                'command': 'rebuild-dht',
                'peers': peers,
                'peer': right_peer,
                'year': year
            }
            data = json.dumps(data).encode()
            p_socket.sendto(
                data, (peers['0']['ip'], int(peers['0']['port'])))

        # Command for a peer to join the dht, which requires the dht to be destroyed and rebuilt
        elif command == 'join-dht':
            peers = data['peers']
            year = data['year']

            # Get the peer to the right and left of the leader (u is name of left peer)
            right_peer = '1'
            u = str(len(peers) - 1)
            u = peers[u]['name']

            # Send teardown-dht <leader_name> to manager
            leader_name = peers['0']['name']
            m_socket.sendto(
                f'teardown-dht {leader_name}'.encode(), (manager_ip, manager_port))
            print('teardown-dht sent to manager')
            response = m_socket.recvfrom(1024)
            if response == 'FAILURE':
                print('Failed tearing down DHT')
                continue

            # Send list of peers, right peer, and left peer to right peer to teardown the dht
            teardown_data = {
                'command': 'teardown-dht',
                'peers': peers,
                'u': u,
                'peer': right_peer
            }
            teardown_data = json.dumps(teardown_data).encode()
            p_socket.sendto(
                teardown_data, (peers[str(right_peer)]['ip'], int(peers[str(right_peer)]['port'])))

            # Add this peer to the list of peers
            peers[str(len(peers))] = data['new_peer']

            # Rebuild the dht with the new list of peers
            data = {
                'command': 'rebuild-dht',
                'peers': peers,
                'peer': 0,
                'year': year
            }
            data = json.dumps(data).encode()
            p_socket.sendto(
                data, (peers['0']['ip'], int(peers['0']['port'])))

        # Command to destroy the dht
        elif command == 'teardown-dht':
            u = data['u']

            # If the command has gone around the whole ring, delete table and send a teardown-complete to the manager
            if u == name:
                table = {}
                m_socket.sendto(f'teardown-complete {name}'.encode(), (manager_ip, manager_port))
                print('teardown-complete sent to manager')

            # Otherwise, delete the table and send it to the next peer
            else:
                table = {}
                peers = data['peers']
                right_peer = str((int(data['peer']) + 1) % len(peers))
                data = {
                    'command': 'teardown-dht',
                    'peers': peers,
                    'u': u,
                    'peer': right_peer
                }
                data = json.dumps(data).encode()
                p_socket.sendto(
                    data, (peers[right_peer]['ip'], int(peers[right_peer]['port'])))

        # On command rebuild-dht, start a leader thread to rebuild the dht
        elif command == 'rebuild-dht':
            threading.Thread(target=leader, args=(data, True), daemon=True).start()

# Function to take and process commands from stdin for the peer
def peer(input_manager_ip, input_manager_port):
    global name, l, table, m_socket, p_socket
    registered = False

    ip, p_port = None, None

    # Loop to read commands from stdin
    while True:
        command = input()
        parts = command.split()

        # register <name> <ip> <m_port> <p_port>
        if parts[0] == 'register':

            # Check if this instance of the program is already registered
            if registered:
                print('Error: Cannot register twice')
                continue

            name = parts[1]
            ip = parts[2]
            m_port = int(parts[3])
            p_port = int(parts[4])

            # Attempt to bind to entered ports. If the ports are unavailable, return an error
            try:
                m_socket.bind((ip, m_port))
                p_socket.bind((ip, p_port))
            except Exception:
                print("Error: Cannot bind to ports")
                continue

            # Send register command to manager
            data = command.encode()
            m_socket.sendto(data, (input_manager_ip, input_manager_port))
            print('register sent to manager')

            # Receive SUCCESS or FAILURE from manager
            response, _ = m_socket.recvfrom(1024)
            response = response.decode()
            if response == "FAILURE":
                m_socket.close()
                p_socket.close()
                print("Error: Registration failed")
                continue

            # If response from manager is SUCCESS, start listening for data on p_port
            # However, use threading so that commands can still be accepted from stdin
            elif response == "SUCCESS":
                threading.Thread(target=listen, daemon=True).start()
                registered = True
                print("Registration successful")

        # setup-dht <name> <n> <year>
        elif parts[0] == 'setup-dht':

            # Send data to manager, receive json response
            data = command.encode()
            m_socket.sendto(data, (input_manager_ip, input_manager_port))
            print('setup-dht sent to manager')
            response, _ = m_socket.recvfrom(65535)

            # Receive response and send a set-id to the leader if success
            response = response.decode()
            if response == "FAILURE":
                # Otherwise output an error
                print("Error: DHT setup failed")
                continue
            else:
                data = json.loads(response)
                data['command'] = 'set-id'
                data['year'] = parts[3]
                p_socket.sendto(json.dumps(data).encode(
                ), (data['peers']['0']['ip'], int(data['peers']['0']['port'])))

        # query-dht <peer-name>
        elif parts[0] == 'query-dht':

            # Send data to manager, receive json response
            data = command.encode()
            m_socket.sendto(data, (input_manager_ip, input_manager_port))
            print('query-dht sent to manager')
            response, _ = m_socket.recvfrom(65535)

            # Receive response and send a find-event to the leader if success
            response = response.decode()
            if response == "FAILURE":
                # Otherwise output an error
                print("Error: DHT setup failed")
                continue
            else:
                response = json.loads(response)
                peer_id = response['peer_id']
                peers = response['peers']

                # Get event ID from user
                event_id = input("Enter ID of event to find: ")

                # Store this peer's ip and port so the entry can be returned here
                original_peer_ip = ip
                original_peer_port = p_port

                # Assemble data to send to peer and send it
                data = {
                    'command': 'query-dht',
                    'event_id': event_id,
                    'peers': peers,
                    'peer': peer_id,
                    'original_peer_ip': original_peer_ip,
                    'original_peer_port': original_peer_port
                }
                p_socket.sendto(json.dumps(data).encode(),(peers[str(peer_id)]['ip'], int(peers[str(peer_id)]['port'])))

        # leave-dht <peer-name>
        elif parts[0] == 'leave-dht':
            data = command.encode()

            # Send the command to the manager
            m_socket.sendto(data, (input_manager_ip, input_manager_port))
            response, _ = m_socket.recvfrom(65535)
            print('leave-dht sent to manager')

            # Receive response and send leave-dht to entered peer if success
            response = response.decode()
            if response == "FAILURE":
                # Otherwise output an error
                print("Error: DHT setup failed")
                continue
            else:
                response = json.loads(response)
                peers = response['peers']
                peer_ip = response['peer_ip']
                peer_port = int(response['peer_port'])
                year = response['year']

                # Send the command to the entered peer
                data = {
                    'command': 'leave-dht',
                    'peers': peers,
                    'peer_id': response['peer_id'],
                    'year': year
                }
                p_socket.sendto(json.dumps(data).encode(), (peer_ip, peer_port))

        # join-dht <peer-name>
        elif parts[0] == 'join-dht':
            data = command.encode()

            # Send the command to the manager
            m_socket.sendto(data, (input_manager_ip, input_manager_port))
            response, _ = m_socket.recvfrom(65535)
            print("join-dht sent to manager")

            # Receive response and send join-dht to entered peer if success
            response = response.decode()
            if response == "FAILURE":
                # Otherwise output an error
                print("Error: DHT setup failed")
                continue
            else:
                response = json.loads(response)
                peers = response['peers']
                peer_ip = response['peer_ip']
                peer_port = int(response['peer_port'])
                year = response['year']

                # Assemble the new peer to be added to the DHT
                new_peer = {
                    "name": parts[1],
                    "ip": peer_ip,
                    "port": peer_port
                }

                # Send the command to the entered peer
                data = {
                    'command': 'join-dht',
                    'peers': peers,
                    'new_peer': new_peer,
                    'year': year
                }
                p_socket.sendto(json.dumps(data).encode(), (peer_ip, peer_port))

        # teardown-dht <peer-name>
        elif parts[0] == 'teardown-dht':
            if parts[1] != name:
                print('Error: entered name must be this peer')
                continue

            data = command.encode()

            # Send the command to the manager
            m_socket.sendto(data, (input_manager_ip, input_manager_port))
            response, _ = m_socket.recvfrom(65535)
            print("teardown-dht sent to manager")

            # Receive response and send teardown-dht to entered peer if success
            response = response.decode()
            if response == "FAILURE":
                # Otherwise output an error
                print("Error: Unable to teardown DHT")
                continue
            else:
                response = json.loads(response)
                peers = response['peers']

                # Clear this peer's table
                table = {}

                # Get the peer to the right and left of this peer (u is name of left peer)
                right_peer = '1'
                u = len(peers) - 1
                u = peers[str(u)]['name']

                # Send list of peers, right peer, and left peer to right peer to teardown the dht
                data = {
                    'command': 'teardown-dht',
                    'peers': peers,
                    'u': u,
                    'peer': right_peer
                }
                data = json.dumps(data).encode()
                p_socket.sendto(data, (peers[str(right_peer)]['ip'], int(peers[str(right_peer)]['port'])))

        # deregister <peer-name>
        elif parts[0] == 'deregister':
            if parts[1] != name:
                print('Error: entered name must be this peer')
                continue

            # Send the command to the manager
            data = command.encode()
            m_socket.sendto(data, (input_manager_ip, input_manager_port))
            print('deregister sent to manager')
            response, _ = m_socket.recvfrom(1024)

            # Exit the program if the response from the manager is SUCCESS
            response = response.decode()
            if response == 'FAILURE':
                # Otherwise output an error
                print('Error: peer is not free')
                continue
            else:
                print('Successfully deregistered peer')
                break

if __name__ == "__main__":
    # Enter the manager IPv4 and port via command line
    if len(sys.argv) != 3:
        print("Usage: python peer.py <IPv4> <port>\n")
        sys.exit(-1)

    # Send manager IP and port to peer function
    manager_ip = sys.argv[1]
    manager_port = int(sys.argv[2])
    print('Please enter a command:')
    peer(manager_ip, manager_port)
