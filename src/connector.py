

#connector.py

class Connector:
    """
    Moving packets from producer
    to downstream consumers --observer--.
    """

    def __init__(self, observer):
        self.observer = observer

    def ingest(self, packet: dict):
        """
        Accept a packet from the simulator and forward it.
        """
        # In a real system, this could enqueue, serialize, buffer, or transmit over the network.

        self.observer.observe(packet)
