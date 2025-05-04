import json
import random
import socket

# global variables
peers = {}
DHT_peers = []
leaving_peer = None
joining_peer = None
manager_port = 7500  # port range 7500-7999
table = None  # holds basic DHT
leader_name = ''  # leader of DHT
dht_in_progress = False
dht_completed = False
dht_rebuilt = True
teardown_completed = True
joining_leaving = False
year = None


def send_response(manager_socket, address, data):  # sends responses
    manager_socket.sendto(json.dumps(data).encode(),
                          address)  # send json data


def register(parts, address, manager_socket):  # registers a user
    global peers, dht_in_progress, dht_rebuilt, teardown_completed
    if dht_in_progress or not dht_rebuilt or not teardown_completed:
        manager_socket.sendto('FAILURE'.encode(), address)
        print("error, DHT busy or incomplete\n")
        return
    # if not 5 parts (4 arguments in function) or if manager is waiting for DHT to be set up
    if len(parts) != 5:
        manager_socket.sendto('FAILURE'.encode(), address)
        return

    # assign arguments
    name, ip, m_port, p_port = parts[1], parts[2], parts[3], parts[4]

    # if name already exists, name is over 15 characters, name is not alphabetic, or any port is already taken
    if name in peers or len(name) > 15 or not name.isalpha() or any(
            p['m_port'] == m_port or p['p_port'] == p_port for p in peers.values()):
        manager_socket.sendto('FAILURE'.encode(), address)
        return

    peers[name] = {'name': name, 'ip': ip, 'm_port': m_port, 'p_port': p_port,
                   'state': 'Free'}  # add peer, set status to 'Free'
    manager_socket.sendto('SUCCESS'.encode(), address)
    print(f"Successfully registered {name} with ip {ip}, m_port {m_port}, and p_port {p_port}")


def setup_DHT(parts, address, manager_socket):  # sets up DHT
    global peers, table, leader_name, dht_in_progress, DHT_peers, teardown_completed, dht_rebuilt, year
    # checks for if manager should be ignoring incoming commands
    if dht_in_progress or dht_completed or not teardown_completed or not dht_rebuilt:
        manager_socket.sendto('FAILURE'.encode(), address)
        print("error, DHT busy or incomplete\n")
        return
    # if not 4 parts (3 arguments in function) or if manager is waiting for DHT to be set up
    if len(parts) != 4:
        manager_socket.sendto('FAILURE'.encode(), address)
        print("error with command\n")
        return

    leader, n, year = parts[1], int(parts[2]), int(
        parts[3])  # assign arguments

    # if leader not registered, not enough peers, year not between 1950 and 2024, or DHT already exists
    if leader not in peers or n < 3 or len(peers) < n or not 1950 <= year <= 2024 or table is not None:
        manager_socket.sendto('FAILURE'.encode(), address)
        print("error with DHT\n")
        return

    # Gets the names of all peers which have state Free
    free_users = [peers[peer]['name']
                  for peer in peers if peers[peer]['state'] == 'Free']
    if leader not in free_users:  # if leader isn't available...
        manager_socket.sendto('FAILURE'.encode(), address)
        print("Leader unavaliable\n")
        return

    leader_name = leader
    peers[leader]['state'] = 'Leader'  # update avaliability

    free_users.remove(leader)
    selected_users = random.sample(
        free_users, n - 1)  # get n-1 random free users
    for name in selected_users:
        peers[name]['state'] = 'InDHT'  # update avaliability

    DHT_peers = [leader] + selected_users  # full DHT users
    # records basic DHT info, indicates DHT is created
    table = {'size': n, 'leader': leader, 'year': year, 'peers': DHT_peers}

    # Forming base of json data to send
    data = {
        "code": "SUCCESS",
        "peers": {}
    }

    # Saving the name, ip, and p_port of all peers selected for DHT
    for (i, name) in enumerate(DHT_peers):
        data['peers'][i] = {}
        data['peers'][i]['name'] = name
        data['peers'][i]['ip'] = peers[name]['ip']
        data['peers'][i]['port'] = peers[name]['p_port']

    # Sending the json data
    send_response(manager_socket, address, data)
    print("setup response given\n")

    # update waiting for setup to be completed
    dht_in_progress = True


# updates that DHT leader completed setup
def DHT_complete(parts, address, manager_socket):
    global leader_name, dht_completed, dht_in_progress, DHT_peers
    # if not 2 parts (1 argument in function) or name is not leader's name or if manager is NOT waiting for DHT to be set up
    if len(parts) != 2 or parts[1] != leader_name or not dht_in_progress:
        print("error with DHT complete\n")
        manager_socket.sendto('FAILURE'.encode(), address)
        return

    manager_socket.sendto('SUCCESS'.encode(), address)
    dht_completed = True
    dht_in_progress = False
    print(f"DHT leader {leader_name} completed setup")
    print(f'Peers in DHT: {sorted(DHT_peers)}')


def query_DHT(parts, address, manager_socket):
    global peers, DHT_peers, dht_in_progress, dht_completed, dht_rebuilt, teardown_completed
    # checks for if manager should be ignoring incoming commands
    if dht_in_progress or not dht_completed or not dht_rebuilt or not teardown_completed:
        manager_socket.sendto('FAILURE'.encode(), address)
        print("error, DHT busy or incomplete\n")
        return
 # Gets the names of all peers which have state Free
    free_users = [peers[peer]['name']
                  for peer in peers if peers[peer]['state'] == 'Free']
    if parts[1] not in free_users: # if requestor is not free
        manager_socket.sendto('FAILURE'.encode(), address)
        print("error, query user not free\n")
        return
    selected_user = random.choice(DHT_peers) # random choice of DHT peer to query

    # Forming base of json data to send
    data = {
        "code": "SUCCESS",
        "peer_id": DHT_peers.index(selected_user),
        "peers": {}
    }

    # Saving the name, ip, and p_port of all peers selected for DHT
    for (i, name) in enumerate(DHT_peers):
        data['peers'][i] = {}
        data['peers'][i]['name'] = name
        data['peers'][i]['ip'] = peers[name]['ip']
        data['peers'][i]['port'] = peers[name]['p_port']

    send_response(manager_socket, address, data)


def leave_DHT(parts, address, manager_socket):
    global joining_leaving, DHT_peers, dht_completed, dht_rebuilt, leaving_peer, year, teardown_completed, dht_in_progress
    # checks for if manager should be ignoring incoming commands
    if dht_in_progress or not dht_completed or not dht_rebuilt or not teardown_completed:
        manager_socket.sendto('FAILURE'.encode(), address)
        print("error, DHT busy or incomplete\n")
        return
    if parts[1] not in DHT_peers: #must be part of dht to leave it
        manager_socket.sendto('FAILURE'.encode(), address)
        print("error leaving DHT\n")
        return
    leaving_peer = parts[1] # set variable for leaving peer

    # Forming base of json data to send
    data = {
        "code": "SUCCESS",
        "year": year,
        "peers": {},
        "peer_ip": peers[leaving_peer]['ip'],
        "peer_port": peers[leaving_peer]['p_port'],
        "peer_id": DHT_peers.index(leaving_peer)
    }

    # Saving the name, ip, and p_port of all peers selected for DHT
    for (i, name) in enumerate(DHT_peers):
        data['peers'][i] = {}
        data['peers'][i]['name'] = name
        data['peers'][i]['ip'] = peers[name]['ip']
        data['peers'][i]['port'] = peers[name]['p_port']

    DHT_peers.remove(parts[1]) # remove peer from list of DHT peers

    peers[parts[1]]['state'] = 'Free'  # update avaliability
    dht_rebuilt = False # dht needs to be rebuilt
    joining_leaving = True # flag to indicate for teardown if a peer is leaving
    send_response(manager_socket, address, data)
    print(f"Peer {parts[1]} left DHT")


def join_DHT(parts, address, manager_socket):
    global joining_leaving, dht_rebuilt, joining_peer, peers, dht_in_progress, year, dht_completed, teardown_completed
    # checks for if manager should be ignoring incoming commands
    if dht_in_progress or not dht_completed or not dht_rebuilt or not teardown_completed:
        manager_socket.sendto('FAILURE'.encode(), address)
        print("error, DHT busy or incomplete\n")
        return
    free_users = [peers[peer]['name']
                  for peer in peers if peers[peer]['state'] == 'Free']
    if parts[1] not in free_users: # joining peer must initially be free
        print("error with joining DHT\n")
        manager_socket.sendto('FAILURE'.encode(), address)
    joining_peer = parts[1]  # set variable for joining peer

    # Forming base of json data to send
    data = {
        "code": "SUCCESS",
        "year": year,
        "peers": {},
        "peer_ip": peers[joining_peer]['ip'],
        "peer_port": peers[joining_peer]['p_port']
    }

    # Saving the name, ip, and p_port of all peers selected for DHT
    for (i, name) in enumerate(DHT_peers):
        data['peers'][i] = {}
        data['peers'][i]['name'] = name
        data['peers'][i]['ip'] = peers[name]['ip']
        data['peers'][i]['port'] = peers[name]['p_port']

    DHT_peers.append(parts[1])

    peers[parts[1]]['state'] = 'InDHT'  # update avaliability
    dht_rebuilt = False # dht needs to be rebuilt
    joining_leaving = True # flag to indicate for teardown if a peer is joining
    send_response(manager_socket, address, data)
    print(f"Peer {parts[1]} joined DHT")


def DHT_rebuilt(parts, address, manager_socket):
    global leaving_peer, joining_peer, peers, leader_name, dht_in_progress, dht_rebuilt, DHT_peers
    # checks for if manager should be ignoring incoming commands
    if dht_in_progress:
        manager_socket.sendto('FAILURE'.encode(), address)
        print("error, DHT busy or incomplete\n")
        return
    # checks if leader is leaving. If it is not leaving, clears state to prepare for new leader
    if leader_name != leaving_peer:
        peers[leader_name]['state'] = 'InDHT'  # update avaliability
    peers[parts[2]]['state'] = 'Leader'  # update avaliability
    leader_name = parts[2] # set new leader
    dht_rebuilt = True # finish rebuild
    leaving_peer = None # reset variable
    joining_peer = None # reset variable
    manager_socket.sendto('SUCCESS'.encode(), address)
    print(f'Leader of DHT is {leader_name}')
    print(f'Peers in DHT: {sorted(DHT_peers)}')


def deregister(parts, address, manager_socket):
    global peers, dht_in_progress, dht_rebuilt
    # checks for if manager should be ignoring incoming commands
    if dht_in_progress or not dht_rebuilt or not teardown_completed:
        manager_socket.sendto('FAILURE'.encode(), address)
        print("error, DHT busy or incomplete\n")
        return
    if peers[parts[1]]['state'] == 'InDHT': # cannot deregister while in DHT
        manager_socket.sendto('FAILURE'.encode(), address)
        print("error deregistering\n")
        return
    peers.pop(parts[1]) # remove peer from avaliable peers
    manager_socket.sendto('SUCCESS'.encode(), address)
    print(f'Peer {parts[1]} deregistered')


def teardown_DHT(parts, address, manager_socket):
    global teardown_completed, peers, leader_name, dht_in_progress, dht_completed
    # checks for if manager should be ignoring incoming commands
    if dht_in_progress or not dht_completed or not teardown_completed:
        manager_socket.sendto('FAILURE'.encode(), address)
        print("error, DHT busy or incomplete\n")
        return
    if parts[1] != leader_name: #only leader can teardown DHT
        manager_socket.sendto('FAILURE'.encode(), address)
        print("only leader can teardown dht\n")
        return
    teardown_completed = False # flag for indicating teardown status

    data = {
        "code": "SUCCESS",
        "peers": {}
    }

    # Saving the name, ip, and p_port of all peers selected for DHT
    for (i, name) in enumerate(DHT_peers):
        data['peers'][i] = {}
        data['peers'][i]['name'] = name
        data['peers'][i]['ip'] = peers[name]['ip']
        data['peers'][i]['port'] = peers[name]['p_port']

    send_response(manager_socket, address, data)


def teardown_complete(parts, address, manager_socket):
    global joining_leaving, teardown_completed, peers, table, leader_name, dht_in_progress, dht_rebuilt, dht_completed, DHT_peers
    # checks for if manager should be ignoring incoming commands
    if dht_in_progress or not dht_completed:
        manager_socket.sendto('FAILURE'.encode(), address)
        print("error, DHT busy or incomplete\n")
        return
    teardown_completed = True # flag for indicating teardown status
    if not joining_leaving: # check if teardown part of join/leave operation
        # reset all variables
        leader_name = ''
        for name in DHT_peers:
            peers[name]['state'] = 'Free'
        dht_completed = False
        DHT_peers = []
        table = None
    else:
        joining_leaving = False # reset variable
    manager_socket.sendto('SUCCESS'.encode(), address)


if __name__ == "__main__":  # main function - setup socket and listen for commands
    manager_port = int(
        input("Please enter manager port number (7500 - 7999): "))
    manager_socket = socket.socket(
        socket.AF_INET, socket.SOCK_DGRAM)  # ipv4, UDP
    manager_socket.bind(('', manager_port))  # initialize socket
    print(f"Manager listening on port {manager_port}")
    while True:  # infinite while loop
        data, address = manager_socket.recvfrom(1024)  # listen for command
        # converts data from binary to readable data
        parts = data.decode().split()
        if not parts:  # if no data, ignore command
            continue

        # get command and call corresponding function
        command = parts[0]
        print(f'{command} received from peer')
        if command == 'register':
            register(parts, address, manager_socket)
        elif command == 'setup-dht':
            setup_DHT(parts, address, manager_socket)
        elif command == 'dht-complete':
            DHT_complete(parts, address, manager_socket)
        elif command == 'query-dht':
            query_DHT(parts, address, manager_socket)
        elif command == 'leave-dht':
            leave_DHT(parts, address, manager_socket)
        elif command == 'join-dht':
            join_DHT(parts, address, manager_socket)
        elif command == 'dht-rebuilt':
            DHT_rebuilt(parts, address, manager_socket)
        elif command == 'deregister':
            deregister(parts, address, manager_socket)
        elif command == 'teardown-dht':
            teardown_DHT(parts, address, manager_socket)
        elif command == 'teardown-complete':
            teardown_complete(parts, address, manager_socket)
        else:
            manager_socket.sendto('FAILURE'.encode(), address)
