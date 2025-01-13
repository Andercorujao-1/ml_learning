#changeNotifier.py
class ChangeNotifier:
    def __init__(self, value):
        self.__value = value
        self.observers = []
    
    def addObserver(self, observer):
        self.observers.append(observer)
    
    def notifyObservers(self):
        for observer in self.observers:
            observer()
    
    def changeValue(self, newValue):
        self.__value = newValue
        self.notifyObservers()

    def getValue(self):
        return self.__value