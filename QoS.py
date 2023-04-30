"""
Note:
This is a class that defines Quality of Service (QoS) parameters for message delivery. 
It has an __init__ method that initializes the QoS parameters, 
an update_qos method to update the parameters,
and a __str__ method to represent the QoS parameters as a string. 
The history_size, reliability, durability, and deadline parameters can be set and updated 
through the update_qos method. 
The __str__ method returns a string representation of the QoS object with its current parameters.
"""

class QoS:
    def __init__(self, history_size=0, reliability=None, durability=None, deadline=None):
        self.history_size = history_size
        self.reliability = reliability
        self.durability = durability
        self.deadline = deadline

    def update_qos(self, history_size=None, reliability=None, durability=None, deadline=None):
        if history_size is not None:
            self.history_size = history_size
        if reliability is not None:
            self.reliability = reliability
        if durability is not None:
            self.durability = durability
        if deadline is not None:
            self.deadline = deadline

    def __str__(self):
        return f"QoS(history_size={self.history_size}, reliability={self.reliability}, durability={self.durability}, deadline={self.deadline})"
