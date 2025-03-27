# ChangeNotifier.py


class ChangeNotifier:
    def __init__(self, value):
        self.value = value
        self.observers = []

    def fAddObserver(self, observer: callable) -> None:
        try:
            if observer not in self.observers:
                self.observers.append(observer)
                logger.debug("Observer added.")
            else:
                logger.debug("Observer already exists, not adding again.")
        except Exception as e:
            logger.error(f"Error in fAddObserver: {e}")
            raise

    def fRemoveObserver(self, observer: callable) -> None:
        try:
            if observer in self.observers:
                self.observers.remove(observer)
                logger.debug("Observer removed.")
            else:
                logger.debug("Observer not found; cannot remove.")
        except Exception as e:
            logger.error(f"Error in fRemoveObserver: {e}")
            raise

    def fNotifyObservers(self) -> None:
        for observer in self.observers:
            try:
                observer()
                logger.debug("Observer notified.")
            except Exception as e:
                logger.error(f"Error notifying observer: {e}")

    def fChangeValue(self, newValue) -> None:
        try:
            self.value = newValue
            logger.debug("Value changed; notifying observers.")
            self.fNotifyObservers()
        except Exception as e:
            logger.error(f"Error in fChangeValue: {e}")
            raise

    def fGetValue(self):
        return self.value

# --- Basic test ---
if __name__ == "__main__":
    def testObserver():
        print("Observer triggered.")

    cn = ChangeNotifier("initial")
    cn.fAddObserver(testObserver)
    cn.fChangeValue("new value")
    cn.fRemoveObserver(testObserver)
    cn.fChangeValue("another value")
