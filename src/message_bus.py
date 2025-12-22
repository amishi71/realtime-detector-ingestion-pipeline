class MessageBus:
    """
    Minimal in-process message bus.
    Responsible only for delivering messages to subscribers.
    """

    def __init__(self):
        self.subscribers = []

    def subscribe(self, handler):
        self.subscribers.append(handler)

    def publish(self, message):
        for handler in self.subscribers:
            handler(message)
