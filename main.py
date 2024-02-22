import socket
import pickle
import threading
import time
import random
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor


class Main:
    def __init__(self, node_id):
        # maps each socket connection to its output stream for message sending
        self.output_streams = {}
        # executor service for managing asynchronous tasks
        self.executor = ThreadPoolExecutor()
        # total number of peers in the system
        self.total_peers = 4
        # common port number for all peer connections
        self.PORT = 6001
        # latch to ensure all connections are established before broadcasting begins
        self.setup_latch = None
        # atomic counter for the number of messages sent by this process
        self.messages_sent = 0
        # stores connections to peers for message broadcasting
        self.peer_connections = []
        # server socket to accept incoming connections from peers
        self.server_socket = None
        # unique identifier for each process
        self.node_id = node_id
        # buffer for messages that cannot be immediately delivered due to causal order
        self.message_buffer = []
        # tracks the count of messages delivered from each peer
        self.message_counts = defaultdict(int)
        # vector clock for managing causal relationships among messages
        self.clock = VectorClock(self.total_peers)

    def initiate_server(self):
        # starts accepting connections from peers
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(("localhost", self.PORT))
        self.server_socket.listen()

        self.setup_latch = threading.Event()

        # asynchronous execution of connection acceptance
        self.executor.submit(self.accept_connections)

    def accept_connections(self):
        try:
            while True:
                client, _ = self.server_socket.accept()
                self.output_streams[client] = client.makefile("wb")

                # asynchronous execution of handling client
                self.executor.submit(self.handle_client, client)

                if len(self.output_streams) == self.total_peers - 1:
                    self.setup_latch.set()
                    break
        except Exception as e:
            self.handle_server_exception(e)

    def establish_peer_connections(self, addresses):
        # establishes connections to all peers
        for i, address in enumerate(addresses):
            if i != self.node_id:
                self.try_peer_connection(address)

    def try_peer_connection(self, address):
        # attempts to connect to a single peer, retrying if necessary
        while True:
            try:
                peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_socket.connect((address, self.PORT))
                self.peer_connections.append(peer_socket)
                self.output_streams[peer_socket] = peer_socket.makefile("wb")
                self.setup_latch.wait(timeout=1)
                break
            except Exception as e:
                self.handle_connection_failure(e, address)
                time.sleep(1)

    def start_broadcasting(self):
        # begins broadcasting messages to all peers
        self.setup_latch.wait()

        print(f"Process {self.node_id}: All peers connected. Starting to broadcast messages...")

        while self.messages_sent < 100:
            time.sleep(random.randint(1, 10))  # random delay to emulate processing time
            self.broadcast_message()
            self.messages_sent += 1

    def broadcast_message(self):
        # broadcasts a message to all connected peers
        self.clock.increment(self.node_id)
        msg = Message(self.clock.get_clock(), f"Message from {self.node_id}", self.node_id)

        print(f"Process {self.node_id}: Broadcasting message.")

        for peer_socket in self.peer_connections:
            try:
                pickle.dump(msg, self.output_streams[peer_socket])
                self.output_streams[peer_socket].flush()
            except Exception as e:
                self.handle_broadcast_error(e)

    def handle_client(self, client):
        # handles incoming messages from a single client
        try:
            while True:
                msg = pickle.load(client.makefile("rb"))
                print(f"Process {self.node_id}: Received message from {msg.sender_id}")
                self.process_message(msg)
        except Exception as e:
            self.handle_client_exception(e)

    def process_message(self, msg):
        # processes a received message for delivery
        if self.clock.can_deliver(msg):
            self.deliver_message(msg)
            self.attempt_message_delivery()
        else:
            print(f"Process {self.node_id}: Buffering message from {msg.sender_id}")
            self.message_buffer.append(msg)

    def deliver_message(self, msg):
        # delivers a message and updates counters
        print(f"Process {self.node_id}: Delivering message from {msg.sender_id}")
        self.clock.update_after_delivery(msg)
        self.message_counts[msg.sender_id] += 1
        self.check_completion()

    def attempt_message_delivery(self):
        # attempts to deliver any buffered messages that are now eligible for delivery
        while True:
            progress = False
            for msg in self.message_buffer:
                if self.clock.can_deliver(msg):
                    self.deliver_message(msg)
                    self.message_buffer.remove(msg)
                    progress = True
                    break
            if not progress:
                break

    def check_completion(self):
        # checks if the process has completed its task
        if self.messages_sent == 100 and all(count == 100 for count in self.message_counts.values()):
            print(f"Process {self.node_id}: Finished.")
            self.shutdown()

    def shutdown(self):
        # shuts down the executor and closes all connections
        try:
            self.executor.shutdown(wait=False)
            self.server_socket.close()
            for sock in self.output_streams:
                sock.close()
        except Exception as e:
            self.handle_error(e)

    # Exception handling methods

    def handle_server_exception(self, e):
        print(f"Process {self.node_id}: Server exception - {e}")

    def handle_connection_failure(self, e, address):
        print(f"Process {self.node_id}: Failed to connect to {address}, retrying...")

    def handle_broadcast_error(self, e):
        pass

    def handle_client_exception(self, e):
        print(f"Process {self.node_id}: Exception handling client - {e}")

    def handle_error(self, e):
        print(f"Process {self.node_id}: Error - {e}")


class VectorClock:
    def __init__(self, size):
        # array representing the logical timestamp of each process
        self.times = [0] * size

    def increment(self, node_id):
        # increments the logical clock for the given process id
        self.times[node_id] += 1

    def get_clock(self):
        # returns a copy of the current state of the vector clock
        return self.times.copy()

    def can_deliver(self, msg):
        # checks if a message can be delivered based on the vector clock comparison
        msg_clock = msg.vector_clock
        for i in range(len(self.times)):
            if i != msg.sender_id and self.times[i] < msg_clock[i]:
                return False
        return self.times[msg.sender_id] + 1 == msg_clock[msg.sender_id]

    def update_after_delivery(self, msg):
        # updates the vector clock based on the received message
        msg_clock = msg.vector_clock
        for i in range(len(self.times)):
            self.times[i] = max(self.times[i], msg_clock[i])
        self.increment(msg.sender_id)


class Message:
    def __init__(self, vector_clock, content, sender_id):
        # vector clock snapshot attached to the message
        self.vector_clock = vector_clock.copy()
        # content of the message
        self.content = content
        # identifier of the sender
        self.sender_id = sender_id


def main():
    # determines the process id based on the hostname, ensuring each process has a unique id
    hostname = socket.gethostname()
    if hostname == "dc01.utdallas.edu":
        node_id = 0
    elif hostname == "dc02.utdallas.edu":
        node_id = 1
    elif hostname == "dc03.utdallas.edu":
        node_id = 2
    elif hostname == "dc04.utdallas.edu":
        node_id = 3
    else:
        print("Unknown host:", hostname)
        return

    app = Main(node_id)
    app.initiate_server()

    addresses = ["localhost", "localhost", "localhost", "localhost"]
    app.establish_peer_connections(addresses)

    try:
        app.start_broadcasting()
    except KeyboardInterrupt:
        print("Keyboard interrupt received. Exiting...")


if __name__ == "__main__":
    main()
