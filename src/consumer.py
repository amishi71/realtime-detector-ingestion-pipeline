class Consumer:
    """
    Consumes messages from the message bus and
    routes them to downstream services.
    """
    def __init__(self, validator, observer, preprocessor):
        self.validator = validator
        self.observer = observer
        self.preprocessor = preprocessor

    def handle(self, packet):# Observe raw packet
        packet = self.validator.validate(packet)
        self.observer.observe(packet)
        # Preprocess packet
        frames = self.preprocessor.process(packet)
        for frame in frames:
            print("CLEAN:", frame)

