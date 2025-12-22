class Consumer:
    """
    Consumes messages from the message bus and
    routes them to downstream services.
    """

    def __init__(self, observer, preprocessor):
        self.observer = observer
        self.preprocessor = preprocessor

    def handle(self, packet):
        # Observe raw packet
        self.observer.observe(packet)

        # Preprocess packet
        clean_frames = self.preprocessor.process(packet)

        for frame in clean_frames:
            print("CLEAN:", frame)

