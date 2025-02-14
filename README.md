# python-distributed-system-casual-ordering-project
The project aims to implement a distributed messaging system using Python. It enables communication between multiple processes running on different machines, facilitating message broadcasting and causal ordering. The system ensures that messages are delivered in the order they were sent, maintaining causality among processes.

Key Features:
Distributed Messaging: Allows communication between processes running on different machines over a network.
Causal Ordering: Ensures that messages are delivered in the causal order, preserving causality among processes.
Buffering: Implements a message buffer to hold messages that cannot be immediately delivered due to causal dependencies.
Asynchronous Communication: Utilizes asynchronous tasks for handling incoming connections and message delivery.
Fault Tolerance: Incorporates error handling mechanisms to handle connection failures and other exceptions gracefully.

Use Cases
Distributed Systems: Suitable for building distributed applications where communication between multiple processes is required.
Event Sourcing: Enables event sourcing architectures where events need to be ordered causally.
Message Queues: Can be used as the foundation for building message queue systems with causal ordering support.

Used:
Python 3.10 
Socket Programming: Utilized for establishing network connections between processes.
Concurrency: Utilizes threading or asynchronous programming for handling multiple connections concurrently.
Serialization: Implements serialization for transmitting message objects over the network.
